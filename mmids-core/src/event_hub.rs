//! The event hub is a central actor that receives events from all type of mmids subsystems and
//! allows them to be published to interested subscribers.

use crate::workflows::manager::WorkflowManagerRequest;
use crate::workflows::WorkflowRequest;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::num::Wrapping;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{info, instrument, warn};

/// A request to publish a notification to the event hub
#[derive(Debug)]
pub enum PublishEventRequest {
    WorkflowStartedOrStopped(WorkflowStartedOrStoppedEvent),
    WorkflowManagerEvent(WorkflowManagerEvent),
}

/// A request to subscribe to a category of events
#[derive(Debug)]
pub enum SubscriptionRequest {
    WorkflowStartedOrStopped {
        channel: UnboundedSender<WorkflowStartedOrStoppedEvent>,
    },

    WorkflowManagerEvents {
        channel: UnboundedSender<WorkflowManagerEvent>,
    },
}

/// Events relating to workflows being started or stopped
#[derive(Clone, Debug)]
pub enum WorkflowStartedOrStoppedEvent {
    WorkflowStarted {
        name: String,
        channel: UnboundedSender<WorkflowRequest>,
    },

    WorkflowEnded {
        name: String,
    },
}

// Events relating to workflow managers
#[derive(Clone, Debug)]
pub enum WorkflowManagerEvent {
    WorkflowManagerRegistered {
        channel: UnboundedSender<WorkflowManagerRequest>,
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

enum FutureResult {
    AllPublishConsumersGone,
    AllSubscriptionRequestConsumersGone,
    NewPublishRequest(PublishEventRequest, UnboundedReceiver<PublishEventRequest>),
    NewSubscriptionRequest(SubscriptionRequest, UnboundedReceiver<SubscriptionRequest>),
    WorkflowStartStopSubscriberGone(usize),
    WorkflowManagerSubscriberGone(usize),
}

struct Actor {
    futures: FuturesUnordered<BoxFuture<'static, FutureResult>>,
    next_subscriber_id: Wrapping<usize>,
    active_subscriber_ids: HashSet<usize>,
    workflow_start_stop_subscribers: HashMap<usize, UnboundedSender<WorkflowStartedOrStoppedEvent>>,
    workflow_manager_subscribers: HashMap<usize, UnboundedSender<WorkflowManagerEvent>>,
    new_subscribers_can_join: bool,
    active_workflows: HashMap<String, UnboundedSender<WorkflowRequest>>,
    active_workflow_manager: Option<UnboundedSender<WorkflowManagerRequest>>,
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
            workflow_start_stop_subscribers: HashMap::new(),
            workflow_manager_subscribers: HashMap::new(),
            new_subscribers_can_join: true,
            active_workflows: HashMap::new(),
            active_workflow_manager: None,
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
                    self.workflow_start_stop_subscribers.remove(&id);
                }

                FutureResult::WorkflowManagerSubscriberGone(id) => {
                    self.active_subscriber_ids.remove(&id);
                    self.workflow_manager_subscribers.remove(&id);
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
                for subscriber in self.workflow_start_stop_subscribers.values() {
                    let _ = subscriber.send(event.clone());
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

            PublishEventRequest::WorkflowManagerEvent(event) => {
                for subscriber in self.workflow_manager_subscribers.values() {
                    let _ = subscriber.send(event.clone());
                }

                match event {
                    WorkflowManagerEvent::WorkflowManagerRegistered { channel } => {
                        self.active_workflow_manager = Some(channel);
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
                    .insert(id.0, channel.clone());
                self.futures
                    .push(notify_workflow_start_stop_subscriber_gone(id.0, channel).boxed());
            }

            SubscriptionRequest::WorkflowManagerEvents { channel } => {
                if let Some(sender) = &self.active_workflow_manager {
                    let _ = channel.send(WorkflowManagerEvent::WorkflowManagerRegistered {
                        channel: sender.clone(),
                    });
                }

                self.workflow_manager_subscribers
                    .insert(id.0, channel.clone());
                self.futures
                    .push(notify_workflow_manager_subscriber_gone(id.0, channel).boxed());
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

async fn notify_workflow_manager_subscriber_gone(
    id: usize,
    sender: UnboundedSender<WorkflowManagerEvent>,
) -> FutureResult {
    sender.closed().await;
    FutureResult::WorkflowManagerSubscriberGone(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils;
    use std::time::Duration;

    #[tokio::test]
    async fn can_receive_workflow_started_notifications() {
        let (publish_channel, subscribe_channel) = start_event_hub();
        let (subscriber_sender, mut subscriber_receiver) = unbounded_channel();
        let (workflow_sender, _workflow_receiver) = unbounded_channel();

        subscribe_channel
            .send(SubscriptionRequest::WorkflowStartedOrStopped {
                channel: subscriber_sender,
            })
            .expect("Failed to subscribe to workflow start/stop events");

        tokio::time::sleep(Duration::from_millis(10)).await;

        publish_channel
            .send(PublishEventRequest::WorkflowStartedOrStopped(
                WorkflowStartedOrStoppedEvent::WorkflowStarted {
                    name: "test".to_string(),
                    channel: workflow_sender,
                },
            ))
            .expect("Failed to publish workflow started event");

        let response = test_utils::expect_mpsc_response(&mut subscriber_receiver).await;
        match response {
            WorkflowStartedOrStoppedEvent::WorkflowStarted { name, channel: _ } => {
                assert_eq!(&name, "test", "Unexpected workflow name");
            }

            event => panic!("Unexpected event received: {:?}", event),
        }
    }

    #[tokio::test]
    async fn can_receive_workflow_started_notification_when_subscribed_after_published() {
        let (publish_channel, subscribe_channel) = start_event_hub();
        let (subscriber_sender, mut subscriber_receiver) = unbounded_channel();
        let (workflow_sender, _workflow_receiver) = unbounded_channel();

        publish_channel
            .send(PublishEventRequest::WorkflowStartedOrStopped(
                WorkflowStartedOrStoppedEvent::WorkflowStarted {
                    name: "test".to_string(),
                    channel: workflow_sender,
                },
            ))
            .expect("Failed to publish workflow started event");

        tokio::time::sleep(Duration::from_millis(10)).await;

        subscribe_channel
            .send(SubscriptionRequest::WorkflowStartedOrStopped {
                channel: subscriber_sender,
            })
            .expect("Failed to subscribe to workflow start/stop events");

        let response = test_utils::expect_mpsc_response(&mut subscriber_receiver).await;
        match response {
            WorkflowStartedOrStoppedEvent::WorkflowStarted { name, channel: _ } => {
                assert_eq!(&name, "test", "Unexpected workflow name");
            }

            event => panic!("Unexpected event received: {:?}", event),
        }
    }

    #[tokio::test]
    async fn can_receive_workflow_stopped_notifications() {
        let (publish_channel, subscribe_channel) = start_event_hub();
        let (subscriber_sender, mut subscriber_receiver) = unbounded_channel();

        subscribe_channel
            .send(SubscriptionRequest::WorkflowStartedOrStopped {
                channel: subscriber_sender,
            })
            .expect("Failed to subscribe to workflow start/stop events");

        tokio::time::sleep(Duration::from_millis(10)).await;

        publish_channel
            .send(PublishEventRequest::WorkflowStartedOrStopped(
                WorkflowStartedOrStoppedEvent::WorkflowEnded {
                    name: "test".to_string(),
                },
            ))
            .expect("Failed to publish workflow ended event");

        let response = test_utils::expect_mpsc_response(&mut subscriber_receiver).await;
        match response {
            WorkflowStartedOrStoppedEvent::WorkflowEnded { name } => {
                assert_eq!(&name, "test", "Unexpected workflow name");
            }

            event => panic!("Unexpected event received: {:?}", event),
        }
    }

    #[tokio::test]
    async fn no_events_when_workflow_started_and_stopped_prior_to_subscription() {
        let (publish_channel, subscribe_channel) = start_event_hub();
        let (subscriber_sender, mut subscriber_receiver) = unbounded_channel();
        let (workflow_sender, _workflow_receiver) = unbounded_channel();

        publish_channel
            .send(PublishEventRequest::WorkflowStartedOrStopped(
                WorkflowStartedOrStoppedEvent::WorkflowStarted {
                    name: "test".to_string(),
                    channel: workflow_sender,
                },
            ))
            .expect("Failed to publish workflow started event");

        publish_channel
            .send(PublishEventRequest::WorkflowStartedOrStopped(
                WorkflowStartedOrStoppedEvent::WorkflowEnded {
                    name: "test".to_string(),
                },
            ))
            .expect("Failed to publish workflow ended event");

        tokio::time::sleep(Duration::from_millis(10)).await;

        subscribe_channel
            .send(SubscriptionRequest::WorkflowStartedOrStopped {
                channel: subscriber_sender,
            })
            .expect("Failed to subscribe to workflow start/stop events");

        test_utils::expect_mpsc_timeout(&mut subscriber_receiver).await;
    }

    #[tokio::test]
    async fn can_receive_workflow_manager_registered_event() {
        let (publish_channel, subscribe_channel) = start_event_hub();
        let (subscriber_sender, mut subscriber_receiver) = unbounded_channel();
        let (manager_sender, _manager_receiver) = unbounded_channel();

        subscribe_channel
            .send(SubscriptionRequest::WorkflowManagerEvents {
                channel: subscriber_sender,
            })
            .expect("Failed to send subscription request");

        tokio::time::sleep(Duration::from_millis(10)).await;

        publish_channel
            .send(PublishEventRequest::WorkflowManagerEvent(
                WorkflowManagerEvent::WorkflowManagerRegistered {
                    channel: manager_sender,
                },
            ))
            .expect("Failed to send publish request");

        let response = test_utils::expect_mpsc_response(&mut subscriber_receiver).await;
        match response {
            WorkflowManagerEvent::WorkflowManagerRegistered { channel: _ } => (),
        }
    }
}
