use crate::workflows::definitions::{
    WorkflowDefinition, WorkflowStepDefinition, WorkflowStepId, WorkflowStepType,
};
use crate::workflows::runner::test_steps::{TestInputStepGenerator, TestOutputStepGenerator};
use crate::workflows::steps::factory::WorkflowStepFactory;
use crate::workflows::steps::StepStatus;
use crate::workflows::{
    start_workflow, MediaNotification, MediaNotificationContent, WorkflowRequest,
};
use crate::StreamId;
use std::collections::HashMap;
use std::sync::atomic::AtomicU16;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::watch::{channel, Sender};

pub struct TestContext {
    pub workflow: UnboundedSender<WorkflowRequest>,
    pub input_media_sender: Sender<MediaNotification>,
    pub output_step_media_receiver: UnboundedReceiver<MediaNotification>,
    pub input_status: Sender<StepStatus>,
    pub output_status: Sender<StepStatus>,
    pub input_step_id: WorkflowStepId,
    pub output_step_id: WorkflowStepId,
    pub input_future_media_sender: Sender<MediaNotification>,
    pub input_step_media_received_count: Arc<AtomicU16>,
}

impl TestContext {
    pub fn new() -> Self {
        let (input_media_sender, input_media_receiver) = channel(MediaNotification {
            stream_id: StreamId(Arc::new("invalid".to_string())),
            content: MediaNotificationContent::StreamDisconnected,
        });

        let (output_media_sender, output_media_receiver) = unbounded_channel();
        let (input_status_sender, input_status_receiver) = channel(StepStatus::Created);
        let (output_status_sender, output_status_receiver) = channel(StepStatus::Created);
        // let (input_media_sender, input_media_receiver) = unbounded_channel();
        let (future_media_sender, future_media_receiver) = channel(MediaNotification {
            stream_id: StreamId(Arc::new("bad".to_string())),
            content: MediaNotificationContent::StreamDisconnected,
        });

        let input_received_counter = Arc::new(AtomicU16::new(0));

        let input_step = TestInputStepGenerator {
            media_receiver: input_media_receiver,
            status_change: input_status_receiver,
            future_result_media_receiver: future_media_receiver,
            media_received_count: input_received_counter.clone(),
        };

        let output_step = TestOutputStepGenerator {
            media_sender: output_media_sender,
            status_change: output_status_receiver,
        };

        let mut factory = WorkflowStepFactory::new();
        factory
            .register(WorkflowStepType("input".to_string()), Box::new(input_step))
            .expect("Failed to register input step");

        factory
            .register(
                WorkflowStepType("output".to_string()),
                Box::new(output_step),
            )
            .expect("Failed to register output step");

        let definition = WorkflowDefinition {
            name: Arc::new("abc".to_string()),
            routed_by_reactor: false,
            steps: vec![
                WorkflowStepDefinition {
                    step_type: WorkflowStepType("input".to_string()),
                    parameters: HashMap::new(),
                },
                WorkflowStepDefinition {
                    step_type: WorkflowStepType("output".to_string()),
                    parameters: HashMap::new(),
                },
            ],
        };

        let input_step_id = definition.steps[0].get_id();
        let output_step_id = definition.steps[1].get_id();

        let workflow = start_workflow(definition, Arc::new(factory));

        TestContext {
            workflow,
            input_media_sender,
            output_step_media_receiver: output_media_receiver,
            input_status: input_status_sender,
            output_status: output_status_sender,
            input_step_id,
            output_step_id,
            input_future_media_sender: future_media_sender,
            input_step_media_received_count: input_received_counter,
        }
    }
}
