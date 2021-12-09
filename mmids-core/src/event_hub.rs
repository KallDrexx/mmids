//! The event hub is a central actor that receives events from all type of mmids subsystems and
//! allows them to be published to interested subscribers.

use crate::workflows::WorkflowRequest;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::num::Wrapping;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{info, instrument, warn};

/// A request to publish a notification to the event hub
pub enum PublishEventRequest {
    WorkflowStartedOrStopped(WorkflowStartedOrStoppedEvent),
}

/// A request to subscribe to a category of events
pub enum SubscriptionRequest {
    WorkflowStartedOrStopped {
        channel: UnboundedSender<WorkflowStartedOrStoppedEvent>,
    },
}

/// Events relating to workflows being started or stopped
#[derive(Clone)]
pub enum WorkflowStartedOrStoppedEvent {
    WorkflowStarted {
        name: String,
        channel: UnboundedSender<WorkflowRequest>,
    },

    WorkflowEnded {
        name: String,
    },
}

pub fn start_event_hub() -> (
    UnboundedSender<PublishEventRequest>,
    UnboundedSender<SubscriptionRequest>,
) {
    let (publish_sender, publish_receiver) = unbounded_channel();
    let (sub_sender, sub_receiver) = unbounded_channel();
    let actor = Actor::new(publish_receiver, sub_receiver);
    tokio::spawn(actor.run());

    (publish_sender, sub_sender)
}

struct WorkflowStartStopSubscriber {
    id: usize,
    channel: UnboundedSender<WorkflowStartedOrStoppedEvent>,
}

enum FutureResult {
    AllPublishConsumersGone,
    AllSubscriptionRequestConsumersGone,
    NewPublishRequest(PublishEventRequest, UnboundedReceiver<PublishEventRequest>),
    NewSubscriptionRequest(SubscriptionRequest, UnboundedReceiver<SubscriptionRequest>),
    WorkflowStartStopSubscriberGone(usize),
}

struct Actor {
    futures: FuturesUnordered<BoxFuture<'static, FutureResult>>,
    next_subscriber_id: Wrapping<usize>,
    active_subscriber_ids: HashSet<usize>,
    workflow_start_stop_subscribers: Vec<WorkflowStartStopSubscriber>,
    new_subscribers_can_join: bool,
    active_workflows: HashMap<String, UnboundedSender<WorkflowRequest>>,
}

impl Actor {
    fn new(
        publish_receiver: UnboundedReceiver<PublishEventRequest>,
        subscribe_receiver: UnboundedReceiver<SubscriptionRequest>,
    ) -> Self {
        let futures = FuturesUnordered::new();
        futures.push(wait_for_publish_request(publish_receiver).boxed());
        futures.push(wait_for_subscription_request(subscribe_receiver).boxed());

        Actor {
            futures,
            next_subscriber_id: Wrapping(0),
            active_subscriber_ids: HashSet::new(),
            workflow_start_stop_subscribers: Vec::new(),
            new_subscribers_can_join: true,
            active_workflows: HashMap::new(),
        }
    }

    #[instrument(name = "Event Hub Execution", skip(self))]
    async fn run(mut self) {
        info!("Starting event hub");

        while let Some(result) = self.futures.next().await {
            match result {
                FutureResult::AllPublishConsumersGone => {
                    info!("All publish request consumers are gone.  No new events can come in");
                    break;
                }

                FutureResult::AllSubscriptionRequestConsumersGone => {
                    warn!("All subscription request consumers gone.  No new subscribers can join");

                    // Theoretically this should only happen when everything is shutting down.  I
                    // guess technically we might still have valid subscribers to send new events to
                    // still so we don't have to shut this down until all subscribers are gone
                    self.new_subscribers_can_join = false;
                }

                FutureResult::WorkflowStartStopSubscriberGone(id) => {
                    self.active_subscriber_ids.remove(&id);
                    for index in 0..self.workflow_start_stop_subscribers.len() {
                        if self.workflow_start_stop_subscribers[index].id == id {
                            self.workflow_start_stop_subscribers.remove(index);
                            break;
                        }
                    }
                }

                FutureResult::NewPublishRequest(request, receiver) => {
                    self.futures
                        .push(wait_for_publish_request(receiver).boxed());
                    self.handle_publish_request(request);
                }

                FutureResult::NewSubscriptionRequest(request, receiver) => {
                    self.futures
                        .push(wait_for_subscription_request(receiver).boxed());
                    self.handle_subscription_request(request);
                }
            }

            if !self.new_subscribers_can_join && self.total_subscriber_count() == 0 {
                info!("All subscribers are gone and no new subscribers can join.  Closing");
                break;
            }
        }

        info!("Closing event hub");
    }

    fn handle_publish_request(&mut self, request: PublishEventRequest) {
        match request {
            PublishEventRequest::WorkflowStartedOrStopped(event) => {
                for subscriber in &self.workflow_start_stop_subscribers {
                    let _ = subscriber.channel.send(event.clone());
                }

                // We want to maintain a list of active workflows, so if a subscriber joins after
                // we receive the notification of a workflow starting they don't miss that event.
                match event {
                    WorkflowStartedOrStoppedEvent::WorkflowStarted { name, channel } => {
                        self.active_workflows.insert(name, channel);
                    }

                    WorkflowStartedOrStoppedEvent::WorkflowEnded { name } => {
                        self.active_workflows.remove(&name);
                    }
                }
            }
        }
    }

    fn handle_subscription_request(&mut self, request: SubscriptionRequest) {
        let id = self.next_subscriber_id;
        self.active_subscriber_ids.insert(id.0);

        loop {
            self.next_subscriber_id += Wrapping(1);
            if !self
                .active_subscriber_ids
                .contains(&self.next_subscriber_id.0)
            {
                break;
            }
        }

        match request {
            SubscriptionRequest::WorkflowStartedOrStopped { channel } => {
                for (name, workflow_channel) in &self.active_workflows {
                    let _ = channel.send(WorkflowStartedOrStoppedEvent::WorkflowStarted {
                        name: name.to_string(),
                        channel: workflow_channel.clone(),
                    });
                }

                self.workflow_start_stop_subscribers
                    .push(WorkflowStartStopSubscriber {
                        id: id.0,
                        channel: channel.clone(),
                    });

                self.futures
                    .push(notify_workflow_start_stop_subscriber_gone(id.0, channel).boxed());
            }
        }
    }

    fn total_subscriber_count(&self) -> usize {
        self.workflow_start_stop_subscribers.len()
    }
}

async fn wait_for_publish_request(
    mut receiver: UnboundedReceiver<PublishEventRequest>,
) -> FutureResult {
    match receiver.recv().await {
        Some(request) => FutureResult::NewPublishRequest(request, receiver),
        None => FutureResult::AllPublishConsumersGone,
    }
}

async fn wait_for_subscription_request(
    mut receiver: UnboundedReceiver<SubscriptionRequest>,
) -> FutureResult {
    match receiver.recv().await {
        Some(request) => FutureResult::NewSubscriptionRequest(request, receiver),
        None => FutureResult::AllSubscriptionRequestConsumersGone,
    }
}

async fn notify_workflow_start_stop_subscriber_gone(
    id: usize,
    sender: UnboundedSender<WorkflowStartedOrStoppedEvent>,
) -> FutureResult {
    sender.closed().await;
    FutureResult::WorkflowStartStopSubscriberGone(id)
}
