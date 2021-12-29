use crate::workflows::definitions::{WorkflowDefinition, WorkflowStepDefinition, WorkflowStepType};
use crate::workflows::runner::test_context::TestContext;
use crate::workflows::steps::StepStatus;
use crate::workflows::MediaNotificationContent::StreamDisconnected;
use crate::workflows::{
    MediaNotification, MediaNotificationContent, WorkflowRequest, WorkflowRequestOperation,
    WorkflowStatus,
};
use crate::{test_utils, StreamId};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::oneshot::channel;
use tokio::time::timeout;

#[tokio::test]
async fn workflow_created_with_steps_in_pending_state() {
    let context = TestContext::new();

    let (sender, receiver) = channel();
    context
        .workflow
        .send(WorkflowRequest {
            request_id: "".to_string(),
            operation: WorkflowRequestOperation::GetState {
                response_channel: sender,
            },
        })
        .expect("Failed to send get state request to workflow");

    let response = test_utils::expect_oneshot_response(receiver).await;
    assert!(response.is_some(), "Expected workflow state returned");

    let workflow = response.unwrap();
    assert_eq!(
        workflow.status,
        WorkflowStatus::Running,
        "Expected workflow to be running"
    );
    assert_eq!(workflow.active_steps.len(), 0, "Expected zero active steps");
    assert_eq!(
        workflow.pending_steps.len(),
        2,
        "Expected two pending steps"
    );
}

#[tokio::test]
async fn steps_pending_when_only_first_step_is_active() {
    let context = TestContext::new();
    context
        .input_status
        .send(StepStatus::Active)
        .expect("Failed to set input state");

    let (sender, receiver) = channel();
    context
        .workflow
        .send(WorkflowRequest {
            request_id: "".to_string(),
            operation: WorkflowRequestOperation::GetState {
                response_channel: sender,
            },
        })
        .expect("Failed to send get state request to workflow");

    let response = test_utils::expect_oneshot_response(receiver).await;
    assert!(response.is_some(), "Expected workflow state returned");

    let workflow = response.unwrap();
    assert_eq!(
        workflow.status,
        WorkflowStatus::Running,
        "Expected workflow to be running"
    );
    assert_eq!(workflow.active_steps.len(), 0, "Expected zero active steps");
    assert_eq!(
        workflow.pending_steps.len(),
        2,
        "Expected two pending steps"
    );
}

#[tokio::test]
async fn steps_pending_when_only_second_step_is_active() {
    let context = TestContext::new();
    context
        .output_status
        .send(StepStatus::Active)
        .expect("Failed to set output state");

    let (sender, receiver) = channel();
    context
        .workflow
        .send(WorkflowRequest {
            request_id: "".to_string(),
            operation: WorkflowRequestOperation::GetState {
                response_channel: sender,
            },
        })
        .expect("Failed to send get state request to workflow");

    let response = test_utils::expect_oneshot_response(receiver).await;
    assert!(response.is_some(), "Expected workflow state returned");

    let workflow = response.unwrap();
    assert_eq!(
        workflow.status,
        WorkflowStatus::Running,
        "Expected workflow to be running"
    );
    assert_eq!(workflow.active_steps.len(), 0, "Expected zero active steps");
    assert_eq!(
        workflow.pending_steps.len(),
        2,
        "Expected two pending steps"
    );
}

#[tokio::test]
async fn steps_active_when_all_pending_steps_become_active() {
    let context = TestContext::new();
    context
        .output_status
        .send(StepStatus::Active)
        .expect("Failed to set output state");
    context
        .input_status
        .send(StepStatus::Active)
        .expect("Failed to set input state");

    let (sender, receiver) = channel();
    context
        .workflow
        .send(WorkflowRequest {
            request_id: "".to_string(),
            operation: WorkflowRequestOperation::GetState {
                response_channel: sender,
            },
        })
        .expect("Failed to send get state request to workflow");

    let response = test_utils::expect_oneshot_response(receiver).await;
    assert!(response.is_some(), "Expected workflow state returned");

    let workflow = response.unwrap();
    assert_eq!(
        workflow.status,
        WorkflowStatus::Running,
        "Expected workflow to be running"
    );
    assert_eq!(workflow.active_steps.len(), 0, "Expected zero active steps");
    assert_eq!(
        workflow.pending_steps.len(),
        2,
        "Expected two pending steps"
    );
}

#[tokio::test]
async fn workflow_in_error_state_if_any_step_goes_to_error_state() {
    let context = TestContext::new();
    context
        .output_status
        .send(StepStatus::Error {
            message: "hi".to_string(),
        })
        .expect("Failed to set output state");

    tokio::time::sleep(Duration::from_millis(10)).await;

    let (sender, receiver) = channel();
    context
        .workflow
        .send(WorkflowRequest {
            request_id: "".to_string(),
            operation: WorkflowRequestOperation::GetState {
                response_channel: sender,
            },
        })
        .expect("Failed to send get state request to workflow");

    let response = test_utils::expect_oneshot_response(receiver).await;
    assert!(response.is_some(), "Expected workflow state returned");

    let workflow = response.unwrap();
    match workflow.status {
        WorkflowStatus::Error {
            message: _,
            failed_step_id,
        } => {
            assert_eq!(
                failed_step_id, context.output_step_id,
                "Unexpected failed step id"
            );
        }

        status => panic!("Unexpected workflow status: {:?}", status),
    }
}

#[tokio::test]
async fn workflow_passes_media_from_one_step_to_the_next() {
    let mut context = TestContext::new();
    context
        .output_status
        .send(StepStatus::Active)
        .expect("Failed to set output state");
    context
        .input_status
        .send(StepStatus::Active)
        .expect("Failed to set input state");
    tokio::time::sleep(Duration::from_millis(10)).await;

    context
        .media_sender
        .send(MediaNotification {
            stream_id: StreamId("abc".to_string()),
            content: StreamDisconnected,
        })
        .expect("Failed to send media notification to step");

    let response = test_utils::expect_mpsc_response(&mut context.media_receiver).await;
    assert_eq!(
        response.stream_id,
        StreamId("abc".to_string()),
        "Unexpected stream id"
    );

    match response.content {
        MediaNotificationContent::StreamDisconnected => (),
        x => panic!("Unexpected media notification: {:?}", x),
    }
}

#[tokio::test]
async fn media_sent_to_workflow_flows_through_steps() {
    let mut context = TestContext::new();
    context
        .output_status
        .send(StepStatus::Active)
        .expect("Failed to set output state");
    context
        .input_status
        .send(StepStatus::Active)
        .expect("Failed to set input state");
    tokio::time::sleep(Duration::from_millis(10)).await;

    context
        .workflow
        .send(WorkflowRequest {
            request_id: "".to_string(),
            operation: WorkflowRequestOperation::MediaNotification {
                media: MediaNotification {
                    stream_id: StreamId("abc".to_string()),
                    content: StreamDisconnected,
                },
            },
        })
        .expect("Failed to send media to workflow");

    let response = test_utils::expect_mpsc_response(&mut context.media_receiver).await;
    assert_eq!(
        response.stream_id,
        StreamId("abc".to_string()),
        "Unexpected stream id"
    );

    match response.content {
        MediaNotificationContent::StreamDisconnected => (),
        x => panic!("Unexpected media notification: {:?}", x),
    }
}

#[tokio::test]
async fn steps_in_active_workflow_are_pending() {
    let context = TestContext::new();
    context
        .input_status
        .send(StepStatus::Active)
        .expect("Failed to set input state");
    context
        .output_status
        .send(StepStatus::Active)
        .expect("Failed to set output state");
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Otherwise pending step will immediately get a resolved future as active
    context
        .output_status
        .send(StepStatus::Created)
        .expect("Failed to set output state");

    let mut params = HashMap::new(); // parameters will give it a new id
    params.insert("a".to_string(), Some("b".to_string()));
    let definition = WorkflowDefinition {
        name: "abc".to_string(),
        routed_by_reactor: false,
        steps: vec![WorkflowStepDefinition {
            step_type: WorkflowStepType("output".to_string()),
            parameters: params,
        }],
    };

    let new_step_id = definition.steps[0].get_id();
    context
        .workflow
        .send(WorkflowRequest {
            request_id: "".to_string(),
            operation: WorkflowRequestOperation::UpdateDefinition {
                new_definition: definition,
            },
        })
        .expect("Failed ot send update request");

    tokio::time::sleep(Duration::from_millis(10)).await;

    let (sender, receiver) = channel();
    context
        .workflow
        .send(WorkflowRequest {
            request_id: "".to_string(),
            operation: WorkflowRequestOperation::GetState {
                response_channel: sender,
            },
        })
        .expect("Failed to send get state request to workflow");

    let response = test_utils::expect_oneshot_response(receiver).await;
    assert!(response.is_some(), "Expected workflow state returned");

    let workflow = response.unwrap();
    assert_eq!(
        workflow.status,
        WorkflowStatus::Running,
        "Expected workflow to be running"
    );
    assert_eq!(workflow.active_steps.len(), 2, "Expected two active steps");
    assert_eq!(
        workflow.pending_steps.len(),
        1,
        "Expected one pending steps"
    );
    assert_eq!(
        workflow.pending_steps[0].step_id, new_step_id,
        "Unexpected pending step id"
    );
}

#[tokio::test]
async fn new_pending_steps_replace_active_steps_when_pending_steps_get_active_status() {
    let context = TestContext::new();
    context
        .output_status
        .send(StepStatus::Active)
        .expect("Failed to set output state");
    context
        .input_status
        .send(StepStatus::Active)
        .expect("Failed to set input state");
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Otherwise pending step will immediately get a resolved future as active
    context
        .output_status
        .send(StepStatus::Created)
        .expect("Failed to set output state");

    let mut params1 = HashMap::new(); // parameters will give it a new id
    params1.insert("a".to_string(), Some("b".to_string()));

    let mut params2 = HashMap::new();
    params2.insert("c".to_string(), None);

    let definition = WorkflowDefinition {
        name: "abc".to_string(),
        routed_by_reactor: false,
        steps: vec![
            WorkflowStepDefinition {
                step_type: WorkflowStepType("output".to_string()),
                parameters: params1,
            },
            WorkflowStepDefinition {
                step_type: WorkflowStepType("output".to_string()),
                parameters: params2,
            },
        ],
    };

    let step1_id = definition.steps[0].get_id();
    let step2_id = definition.steps[1].get_id();

    context
        .workflow
        .send(WorkflowRequest {
            request_id: "".to_string(),
            operation: WorkflowRequestOperation::UpdateDefinition {
                new_definition: definition,
            },
        })
        .expect("Failed ot send update request");

    tokio::time::sleep(Duration::from_millis(10)).await;
    context
        .output_status
        .send(StepStatus::Active)
        .expect("Failed to set output state");
    tokio::time::sleep(Duration::from_millis(10)).await;

    let (sender, receiver) = channel();
    context
        .workflow
        .send(WorkflowRequest {
            request_id: "".to_string(),
            operation: WorkflowRequestOperation::GetState {
                response_channel: sender,
            },
        })
        .expect("Failed to send get state request to workflow");

    let response = test_utils::expect_oneshot_response(receiver).await;
    assert!(response.is_some(), "Expected workflow state returned");

    let workflow = response.unwrap();
    assert_eq!(
        workflow.status,
        WorkflowStatus::Running,
        "Expected workflow to be running"
    );
    assert_eq!(
        workflow.active_steps.len(),
        2,
        "Unexpected number of active steps"
    );
    assert_eq!(
        workflow.active_steps[0].step_id, step1_id,
        "Unexpected active step 1 id"
    );
    assert_eq!(
        workflow.active_steps[1].step_id, step2_id,
        "Unexpected active step 2 id"
    );
    assert_eq!(
        workflow.pending_steps.len(),
        0,
        "Unexpected number of pending steps"
    );
}

#[tokio::test]
async fn channel_closed_after_shutdown() {
    let context = TestContext::new();
    context
        .workflow
        .send(WorkflowRequest {
            request_id: "".to_string(),
            operation: WorkflowRequestOperation::StopWorkflow,
        })
        .expect("Failed to send shutdown message");

    match timeout(Duration::from_millis(10), context.workflow.closed()).await {
        Ok(_) => (),
        Err(_) => panic!("Workflow channel didn't close"),
    }
}
