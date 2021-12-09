use crate::workflows::definitions::{WorkflowDefinition, WorkflowStepDefinition};
use crate::workflows::steps::factory::WorkflowStepFactory;
use crate::workflows::steps::{
    StepFutureResult, StepInputs, StepOutputs, StepStatus, WorkflowStep,
};
use crate::workflows::{MediaNotification, MediaNotificationContent};
use crate::StreamId;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Sender;
use tracing::{error, info, instrument, span, warn, Level};

/// A request to the workflow to perform an action
pub struct WorkflowRequest {
    /// An identifier that can be used to correlate this request with its
    pub request_id: String,
    pub operation: WorkflowRequestOperation,
}

/// Operations that can be made to an actively running workflow
pub enum WorkflowRequestOperation {
    /// Requests the workflow update with a new definition. The workflow will take shape to look
    /// exactly as the specified definition has.  Any existing steps that aren't specified will
    /// be removed, any new steps will be created, and any steps that stay will reflect the order
    /// specified.
    UpdateDefinition { new_definition: WorkflowDefinition },

    /// Requests the workflow to return a snapshot of its current state
    GetState {
        response_channel: Sender<Option<WorkflowState>>,
    },

    /// Requests the workflow stop operating
    StopWorkflow,

    /// Sends a media notification to this stream
    MediaNotification { media: MediaNotification },
}

pub struct WorkflowState {
    pub active_steps: Vec<WorkflowStepState>,
    pub pending_steps: Vec<WorkflowStepState>,
}

pub struct WorkflowStepState {
    pub definition: WorkflowStepDefinition,
    pub status: StepStatus,
}

/// Starts the execution of a workflow with the specified definition
pub fn start_workflow(
    definition: WorkflowDefinition,
    step_factory: Arc<WorkflowStepFactory>,
) -> UnboundedSender<WorkflowRequest> {
    let (sender, receiver) = unbounded_channel();
    let actor = Actor::new(&definition, step_factory, receiver);
    tokio::spawn(actor.run(definition));

    sender
}

enum FutureResult {
    AllConsumersGone,
    WorkflowRequestReceived(WorkflowRequest, UnboundedReceiver<WorkflowRequest>),

    StepFutureResolved {
        step_id: u64,
        result: Box<dyn StepFutureResult>,
    },
}

struct StreamDetails {
    /// The step that first sent a new stream media notification.  We know that if this step is
    /// removed, the stream no longer has a source of video and should be considered disconnected
    originating_step_id: u64,
}

struct Actor<'a> {
    name: String,
    steps_by_definition_id: HashMap<u64, Box<dyn WorkflowStep>>,
    active_steps: Vec<u64>,
    pending_steps: Vec<u64>,
    futures: FuturesUnordered<BoxFuture<'a, FutureResult>>,
    step_inputs: StepInputs,
    step_outputs: StepOutputs<'a>,
    cached_step_media: HashMap<u64, HashMap<StreamId, Vec<MediaNotification>>>,
    cached_inbound_media: HashMap<StreamId, Vec<MediaNotification>>,
    active_streams: HashMap<StreamId, StreamDetails>,
    step_factory: Arc<WorkflowStepFactory>,
    step_definitions: HashMap<u64, WorkflowStepDefinition>,
}

impl<'a> Actor<'a> {
    #[instrument(skip(definition, step_factory, receiver), fields(workflow_name = %definition.name))]
    fn new(
        definition: &WorkflowDefinition,
        step_factory: Arc<WorkflowStepFactory>,
        receiver: UnboundedReceiver<WorkflowRequest>,
    ) -> Self {
        let futures = FuturesUnordered::new();
        info!("Creating workflow");

        futures.push(wait_for_workflow_request(receiver).boxed());

        Actor {
            name: definition.name.clone(),
            futures,
            steps_by_definition_id: HashMap::new(),
            active_steps: Vec::new(),
            pending_steps: Vec::new(),
            step_inputs: StepInputs::new(),
            step_outputs: StepOutputs::new(),
            cached_step_media: HashMap::new(),
            cached_inbound_media: HashMap::new(),
            active_streams: HashMap::new(),
            step_factory,
            step_definitions: HashMap::new(),
        }
    }

    #[instrument(name = "Workflow Execution", skip(self, initial_definition), fields(workflow_name = %self.name))]
    async fn run(mut self, initial_definition: WorkflowDefinition) {
        info!("Starting workflow");

        self.apply_new_definition(initial_definition);

        while let Some(future) = self.futures.next().await {
            match future {
                FutureResult::AllConsumersGone => {
                    warn!("All channel owners gone");
                    break;
                }

                FutureResult::WorkflowRequestReceived(request, receiver) => {
                    self.futures
                        .push(wait_for_workflow_request(receiver).boxed());

                    let mut stop_workflow = false;
                    self.handle_workflow_request(request, &mut stop_workflow);

                    if stop_workflow {
                        break;
                    }
                }

                FutureResult::StepFutureResolved { step_id, result } => {
                    self.execute_steps(step_id, Some(result), false, true);
                }
            }
        }

        info!("Workflow closing");
    }

    #[instrument(skip(self, request, stop_workflow), fields(request_id = %request.request_id))]
    fn handle_workflow_request(&mut self, request: WorkflowRequest, stop_workflow: &mut bool) {
        match request.operation {
            WorkflowRequestOperation::UpdateDefinition { new_definition } => {
                info!("Workflow requested to have its definition updated");
                self.apply_new_definition(new_definition);
            }

            WorkflowRequestOperation::GetState { response_channel } => {
                info!("Workflow state requested by external caller");
                let mut state = WorkflowState {
                    pending_steps: Vec::new(),
                    active_steps: Vec::new(),
                };

                for id in &self.pending_steps {
                    if let Some(definition) = self.step_definitions.get(&id) {
                        if let Some(step) = self.steps_by_definition_id.get(&id) {
                            state.pending_steps.push(WorkflowStepState {
                                definition: definition.clone(),
                                status: step.get_status().clone(),
                            });
                        } else {
                            error!(step_id = %id, "No step was found with an id of {}", id);
                        }
                    } else {
                        error!(step_id = %id, "No definition was found for step id {}", id);
                    }
                }

                for id in &self.active_steps {
                    if let Some(definition) = self.step_definitions.get(&id) {
                        if let Some(step) = self.steps_by_definition_id.get(&id) {
                            state.active_steps.push(WorkflowStepState {
                                definition: definition.clone(),
                                status: step.get_status().clone(),
                            });
                        } else {
                            error!(step_id = %id, "No step was found with an id of {}", id);
                        }
                    } else {
                        error!(step_id = %id, "No definition was found for step id {}", id);
                    }
                }

                let _ = response_channel.send(Some(state));
            }

            WorkflowRequestOperation::StopWorkflow => {
                info!("Closing workflow as requested");
                *stop_workflow = true;

                for id in &self.active_steps {
                    if let Some(step) = self.steps_by_definition_id.get_mut(id) {
                        step.shutdown();
                    }
                }

                for id in &self.pending_steps {
                    if let Some(step) = self.steps_by_definition_id.get_mut(id) {
                        step.shutdown();
                    }
                }
            }

            WorkflowRequestOperation::MediaNotification { media } => {
                self.update_inbound_media_cache(&media);
                self.step_inputs.clear();
                self.step_inputs.media.push(media);
                if let Some(id) = self.active_steps.get(0) {
                    let id = *id;
                    self.execute_steps(id, None, true, true);
                }
            }
        }
    }

    fn apply_new_definition(&mut self, definition: WorkflowDefinition) {
        info!(
            "Applying a new workflow definition with {} steps",
            definition.steps.len()
        );

        self.pending_steps.clear();
        for step_definition in definition.steps {
            let id = step_definition.get_id();
            let step_type = step_definition.step_type.clone();
            self.step_definitions
                .insert(step_definition.get_id(), step_definition.clone());

            self.pending_steps.push(id);

            if !self.steps_by_definition_id.contains_key(&id) {
                let span = span!(Level::INFO, "Step Creation", step_id = id);
                let _ = span.enter();

                let step_result = match self.step_factory.create_step(step_definition) {
                    Ok(step_result) => step_result,
                    Err(error) => {
                        error!("Step factory failed to generate step instance: {:?}", error);

                        // TODO: put workflow in error state
                        return;
                    }
                };

                let (step, futures) = match step_result {
                    Ok((step, futures)) => (step, futures),
                    Err(error) => {
                        error!("Step could not be generated: {}", error);

                        // TODO: put workflow in error state
                        return;
                    }
                };

                for future in futures {
                    self.futures.push(wait_for_step_future(id, future).boxed());
                }

                self.steps_by_definition_id.insert(id, step);
                info!("Step type '{}' created", step_type);
            }
        }

        self.check_if_all_pending_steps_are_active(true);
    }

    fn execute_steps(
        &mut self,
        initial_step_id: u64,
        future_result: Option<Box<dyn StepFutureResult>>,
        preserve_current_step_inputs: bool,
        perform_pending_check: bool,
    ) {
        if !preserve_current_step_inputs {
            self.step_inputs.clear();
        }

        self.step_outputs.clear();

        if let Some(future_result) = future_result {
            self.step_inputs.notifications.push(future_result);
        }

        let mut start_index = None;
        for x in 0..self.active_steps.len() {
            if self.active_steps[x] == initial_step_id {
                start_index = Some(x);
                break;
            }
        }

        // If we have a start_index, that means the step we want to execute is an active step.  So
        // execute that step and all active steps after it. If it's not an active step, then we
        // only want to execute that one step and none others.
        if let Some(start_index) = start_index {
            for x in start_index..self.active_steps.len() {
                self.execute_step(self.active_steps[x]);
            }
        } else {
            self.execute_step(initial_step_id);
        }

        if perform_pending_check {
            self.check_if_all_pending_steps_are_active(false);
        }
    }

    fn execute_step(&mut self, step_id: u64) {
        let span = span!(Level::INFO, "Step Execution", step_id = step_id);
        let _enter = span.enter();

        let step = match self.steps_by_definition_id.get_mut(&step_id) {
            Some(x) => x,
            None => {
                let is_active = self.active_steps.contains(&step_id);
                error!(
                    "Attempted to execute step id {} but we it has no definition (is active: {})",
                    step_id, is_active
                );

                return;
            }
        };

        let status = step.get_status();
        if status != &StepStatus::Shutdown && status != &StepStatus::Error {
            step.execute(&mut self.step_inputs, &mut self.step_outputs);
        }

        // Even if we didn't call execute, run through the logic below to make sure we are in a
        // clean state if we do fall through to the next step.
        for future in self.step_outputs.futures.drain(..) {
            self.futures
                .push(wait_for_step_future(step.get_definition().get_id(), future).boxed());
        }

        self.update_stream_details(step_id);
        self.update_media_cache_from_outputs(step_id);
        self.step_inputs.clear();
        self.step_inputs
            .media
            .extend(self.step_outputs.media.drain(..));

        self.step_outputs.clear();
    }

    fn check_if_all_pending_steps_are_active(&mut self, swap_if_pending_is_empty: bool) {
        let mut all_are_active = true;
        for id in &self.pending_steps {
            let step = match self.steps_by_definition_id.get(id) {
                Some(x) => Some(x),
                None => {
                    error!(
                        step_id = id,
                        "Workflow had step id {} pending but this step was not defined", id
                    );

                    return; // TODO: set workflow in error state
                }
            };

            if let Some(step) = step {
                match step.get_status() {
                    StepStatus::Created => all_are_active = false,
                    StepStatus::Active => (),

                    // TODO: Set workflow in error state for these two
                    StepStatus::Error => return,
                    StepStatus::Shutdown => return,
                }
            } else {
                // the step is still waiting to be instantiated by the factory
            }
        }

        if (self.pending_steps.len() > 0 && all_are_active)
            || (self.pending_steps.len() == 0 && swap_if_pending_is_empty)
        {
            // Since we have pending steps and all are now ready to become active, we need to
            // swap all active steps for pending steps to make them active.

            // In the case of `swap_if_pending_is_empty`, this is usually the case if the user
            // updates this workflow with a definition that contains no workflow steps, then that
            // means the user specifically wants this workflow empty.  So we need to tear down all
            // active steps.

            // Note: there's a possibility that a pending swap can trigger a new set
            // of sequence headers to fall through.  An example of this happening is if
            // a transcoding step is placed in between an existing playback step.  This
            // will probably cause playback issues unless the client supports changing
            // decoding parameters mid-stream, which isn't certain.  We either need to
            // leave this up to mmids operators to realize, or need to come up with a
            // solution to remove the footgun (such as disconnecting playback clients
            // upon a new sequence header being seen).  Unsure if that's the best
            // approach though.
            for index in (0..self.active_steps.len()).rev() {
                let step_id = self.active_steps[index];
                if !self.pending_steps.contains(&step_id) {
                    // Since this step is currently active but not pending, the swap will make this
                    // step go away for good.  Therefore, we need to clean up its definition and
                    // raise disconnection notices for any streams originating from this step, so
                    // that latter steps that will survive will know not to expect more media
                    // from these streams.
                    info!(step_id = step_id, "Removing now unused step id {}", step_id);
                    self.step_definitions.remove(&step_id);
                    if let Some(mut step) = self.steps_by_definition_id.remove(&step_id) {
                        let span = span!(Level::INFO, "Step Shutdown", step_id = %step_id);
                        let _enter = span.enter();
                        step.shutdown();
                    }

                    if let Some(cache) = self.cached_step_media.remove(&step_id) {
                        for key in cache.keys() {
                            if let Some(stream) = self.active_streams.get(key) {
                                if stream.originating_step_id == step_id {
                                    for x in (index + 1)..self.active_steps.len() {
                                        self.step_outputs.clear();
                                        self.step_inputs.clear();
                                        self.step_inputs.media.push(MediaNotification {
                                            stream_id: key.clone(),
                                            content: MediaNotificationContent::StreamDisconnected,
                                        });

                                        self.execute_step(self.active_steps[x]);
                                    }

                                    self.active_streams.remove(key);
                                }
                            }
                        }
                    }
                }
            }

            // Since some pending steps may not have been around previously, they would not have
            // gotten stream started notifications and missing sequence headers.  So we need to
            // find its parent step's cache and replay any required media notifications
            for index in 0..self.pending_steps.len() {
                let current_step_id = self.pending_steps[index];
                if !self.active_steps.contains(&current_step_id) {
                    // This is a new step
                    let notifications = if index == 0 {
                        // The first step uses the inbound cache, not step based cache
                        self.cached_inbound_media
                            .values()
                            .flatten()
                            .map(|x| x.clone())
                            .collect::<Vec<_>>()
                    } else {
                        let previous_step_id = self.pending_steps[index - 1];
                        if let Some(cache) = self.cached_step_media.get(&previous_step_id) {
                            cache
                                .values()
                                .flatten()
                                .map(|x| x.clone())
                                .collect::<Vec<_>>()
                        } else {
                            Vec::new()
                        }
                    };

                    self.step_inputs.clear();
                    self.step_inputs.media.extend(notifications);
                    self.execute_steps(current_step_id, None, true, false);

                    // TODO: This is probably going to cause duplicate stream started notifications.
                    // Not sure a way around that and we probably need to remove those warnings.

                    // TODO: The current code only handles notifications raised by parents of
                    // new steps.  There's the possibility that a change of order of existing
                    // steps could cause steps to be tracking streams that come in after the step,
                    // or not know about steps that were created in steps that used to be after but
                    // is now before.  It also means it may have outdated sequence headers if
                    // a transcoding step was removed.
                }
            }

            std::mem::swap(&mut self.pending_steps, &mut self.active_steps);
            self.pending_steps.clear();

            info!("All pending steps moved to active");
        }
    }

    fn update_stream_details(&mut self, current_step_id: u64) {
        for media in &self.step_outputs.media {
            match &media.content {
                MediaNotificationContent::Video { .. } => (),
                MediaNotificationContent::Audio { .. } => (),
                MediaNotificationContent::Metadata { .. } => (),
                MediaNotificationContent::NewIncomingStream { .. } => {
                    if !self.active_streams.contains_key(&media.stream_id) {
                        // Since this is the first time we've gotten a new incoming stream
                        // notification for this stream, assume this this stream originates from
                        // the current step
                        self.active_streams.insert(
                            media.stream_id.clone(),
                            StreamDetails {
                                originating_step_id: current_step_id,
                            },
                        );
                    }
                }

                MediaNotificationContent::StreamDisconnected => {
                    if let Some(details) = self.active_streams.get(&media.stream_id) {
                        if details.originating_step_id == current_step_id {
                            self.active_streams.remove(&media.stream_id);
                        }
                    }
                }
            }
        }
    }

    fn update_inbound_media_cache(&mut self, media: &MediaNotification) {
        match media.content {
            MediaNotificationContent::NewIncomingStream { .. } => {
                let collection = vec![media.clone()];
                self.cached_inbound_media
                    .insert(media.stream_id.clone(), collection);
            }

            MediaNotificationContent::StreamDisconnected => {
                self.cached_inbound_media.remove(&media.stream_id);
            }

            MediaNotificationContent::Audio {
                is_sequence_header: true,
                ..
            } => {
                if let Some(collection) = self.cached_inbound_media.get_mut(&media.stream_id) {
                    collection.push(media.clone());
                }
            }

            MediaNotificationContent::Video {
                is_sequence_header: true,
                ..
            } => {
                if let Some(collectoin) = self.cached_inbound_media.get_mut(&media.stream_id) {
                    collectoin.push(media.clone());
                }
            }

            _ => (),
        }
    }

    fn update_media_cache_from_outputs(&mut self, step_id: u64) {
        let step_cache = self
            .cached_step_media
            .entry(step_id)
            .or_insert(HashMap::new());

        for media in &self.step_outputs.media {
            enum Operation {
                Add,
                Remove,
                Ignore,
            }
            let operation = match &media.content {
                MediaNotificationContent::StreamDisconnected => {
                    // Stream has ended so no reason to keep the cache around
                    Operation::Remove
                }

                MediaNotificationContent::NewIncomingStream { .. } => Operation::Add,

                MediaNotificationContent::Metadata { .. } => {
                    // I *think* we can ignore these, since the sequence headers are really
                    // what's important to replay
                    Operation::Ignore
                }

                MediaNotificationContent::Video {
                    is_sequence_header, ..
                } => {
                    // We must cache sequence headers.  We *may* need to cache the latest key frame
                    if *is_sequence_header {
                        Operation::Add
                    } else {
                        Operation::Ignore
                    }
                }

                MediaNotificationContent::Audio {
                    is_sequence_header, ..
                } => {
                    if *is_sequence_header {
                        Operation::Add
                    } else {
                        Operation::Ignore
                    }
                }
            };

            match operation {
                Operation::Ignore => (),
                Operation::Remove => {
                    step_cache.remove(&media.stream_id);
                }

                Operation::Add => {
                    let collection = step_cache
                        .entry(media.stream_id.clone())
                        .or_insert(Vec::new());

                    collection.push(media.clone());
                }
            }
        }
    }
}

unsafe impl Send for Actor<'_> {}

async fn wait_for_workflow_request<'a>(
    mut receiver: UnboundedReceiver<WorkflowRequest>,
) -> FutureResult {
    match receiver.recv().await {
        Some(x) => FutureResult::WorkflowRequestReceived(x, receiver),
        None => FutureResult::AllConsumersGone,
    }
}

async fn wait_for_step_future<'a>(
    step_id: u64,
    future: BoxFuture<'a, Box<dyn StepFutureResult>>,
) -> FutureResult {
    let result = future.await;
    FutureResult::StepFutureResolved { step_id, result }
}
