use crate::rtmp_server::actor::tests::rtmp_client::RtmpTestClient;
use crate::rtmp_server::actor::tests::test_context::TestContextBuilder;
use crate::rtmp_server::{
    start_rtmp_server_endpoint, IpRestriction, RtmpEndpointMediaData, RtmpEndpointMediaMessage,
    RtmpEndpointPublisherMessage, RtmpEndpointRequest, RtmpEndpointWatcherNotification,
    StreamKeyRegistration, ValidationResponse,
};
use bytes::Bytes;
use mmids_core::codecs::VideoCodec;
use mmids_core::codecs::VideoCodec::{Unknown, H264};
use mmids_core::test_utils;
use rml_rtmp::sessions::{ClientSessionEvent, StreamMetadata};
use rml_rtmp::time::RtmpTimestamp;
use std::sync::Arc;
use tokio::sync::mpsc::unbounded_channel;

mod rtmp_client;
mod test_context;

#[tokio::test]
async fn can_register_for_specific_port_for_publishers() {
    let (mut client, sender) = RtmpTestClient::new();
    let endpoint = start_rtmp_server_endpoint(sender);

    let (sender, mut receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForPublishers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            stream_id: None,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Any,
            message_channel: sender,
        })
        .expect("Endpoint request failed to send");

    client.accept_port_request(9999, false).await;

    let response = test_utils::expect_mpsc_response(&mut receiver).await;
    match response {
        RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }
}

#[tokio::test]
async fn can_register_with_tls_enabled() {
    let (mut client, sender) = RtmpTestClient::new();
    let endpoint = start_rtmp_server_endpoint(sender);

    let (sender, mut receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForPublishers {
            port: 9999,
            use_tls: true,
            requires_registrant_approval: false,
            stream_id: None,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Any,
            message_channel: sender,
        })
        .expect("Endpoint request failed to send");

    client.accept_port_request(9999, true).await;

    let response = test_utils::expect_mpsc_response(&mut receiver).await;
    match response {
        RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }
}

#[tokio::test]
async fn endpoint_publisher_receives_failed_when_port_rejected() {
    let (mut client, sender) = RtmpTestClient::new();
    let endpoint = start_rtmp_server_endpoint(sender);

    let (sender, mut receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForPublishers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            stream_id: None,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Any,
            message_channel: sender,
        })
        .expect("Endpoint request failed to send");

    client.deny_port_request(9999, false).await;

    let response = test_utils::expect_mpsc_response(&mut receiver).await;
    match response {
        RtmpEndpointPublisherMessage::PublisherRegistrationFailed => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }
}

#[tokio::test]
async fn multiple_requests_for_same_port_only_sends_one_request_to_socket_manager() {
    let (mut client, sender) = RtmpTestClient::new();
    let endpoint = start_rtmp_server_endpoint(sender);

    let (sender, mut receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForPublishers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            stream_id: None,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Any,
            message_channel: sender,
        })
        .expect("Endpoint request failed to send");

    client.accept_port_request(9999, false).await;

    let response = test_utils::expect_mpsc_response(&mut receiver).await;
    match response {
        RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }

    let (sender2, mut receiver2) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForPublishers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            stream_id: None,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app2".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Any,
            message_channel: sender2,
        })
        .expect("2nd endpoint request failed to send");

    client.expect_empty_request_channel().await;

    let response = test_utils::expect_mpsc_response(&mut receiver2).await;
    match response {
        RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }
}

#[tokio::test]
async fn second_publisher_rejected_on_same_app_when_both_any_stream_key() {
    let (mut client, sender) = RtmpTestClient::new();
    let endpoint = start_rtmp_server_endpoint(sender);

    let (sender, mut receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForPublishers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            stream_id: None,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Any,
            message_channel: sender,
        })
        .expect("Endpoint request failed to send");

    client.accept_port_request(9999, false).await;

    let response = test_utils::expect_mpsc_response(&mut receiver).await;
    match response {
        RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }

    let (sender2, mut receiver2) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForPublishers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            stream_id: None,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Any,
            message_channel: sender2,
        })
        .expect("2nd endpoint request failed to send");

    let response = test_utils::expect_mpsc_response(&mut receiver2).await;
    match response {
        RtmpEndpointPublisherMessage::PublisherRegistrationFailed => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }
}

#[tokio::test]
async fn second_publisher_rejected_on_same_app_and_same_exact_key() {
    let (mut client, sender) = RtmpTestClient::new();
    let endpoint = start_rtmp_server_endpoint(sender);

    let (sender, mut receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForPublishers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            stream_id: None,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Exact(Arc::new("abc".to_string())),
            message_channel: sender,
        })
        .expect("Endpoint request failed to send");

    client.accept_port_request(9999, false).await;

    let response = test_utils::expect_mpsc_response(&mut receiver).await;
    match response {
        RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }

    let (sender2, mut receiver2) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForPublishers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            stream_id: None,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Exact(Arc::new("abc".to_string())),
            message_channel: sender2,
        })
        .expect("2nd endpoint request failed to send");

    let response = test_utils::expect_mpsc_response(&mut receiver2).await;
    match response {
        RtmpEndpointPublisherMessage::PublisherRegistrationFailed => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }
}

#[tokio::test]
async fn second_publisher_rejected_on_same_app_when_first_request_is_for_any_key() {
    let (mut client, sender) = RtmpTestClient::new();
    let endpoint = start_rtmp_server_endpoint(sender);

    let (sender, mut receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForPublishers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            stream_id: None,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Any,
            message_channel: sender,
        })
        .expect("Endpoint request failed to send");

    client.accept_port_request(9999, false).await;

    let response = test_utils::expect_mpsc_response(&mut receiver).await;
    match response {
        RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }

    let (sender2, mut receiver2) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForPublishers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            stream_id: None,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Exact(Arc::new("abc".to_string())),
            message_channel: sender2,
        })
        .expect("2nd endpoint request failed to send");

    let response = test_utils::expect_mpsc_response(&mut receiver2).await;
    match response {
        RtmpEndpointPublisherMessage::PublisherRegistrationFailed => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }
}

#[tokio::test]
async fn second_publisher_rejected_on_same_app_when_first_request_is_for_specific_key() {
    let (mut client, sender) = RtmpTestClient::new();
    let endpoint = start_rtmp_server_endpoint(sender);

    let (sender, mut receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForPublishers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            stream_id: None,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Exact(Arc::new("abc".to_string())),
            message_channel: sender,
        })
        .expect("Endpoint request failed to send");

    client.accept_port_request(9999, false).await;

    let response = test_utils::expect_mpsc_response(&mut receiver).await;
    match response {
        RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }

    let (sender2, mut receiver2) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForPublishers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            stream_id: None,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Any,
            message_channel: sender2,
        })
        .expect("2nd endpoint request failed to send");

    let response = test_utils::expect_mpsc_response(&mut receiver2).await;
    match response {
        RtmpEndpointPublisherMessage::PublisherRegistrationFailed => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }
}

#[tokio::test]
async fn second_publisher_accepted_on_same_app_on_different_exact_keys() {
    let (mut client, sender) = RtmpTestClient::new();
    let endpoint = start_rtmp_server_endpoint(sender);

    let (sender, mut receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForPublishers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            stream_id: None,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Exact(Arc::new("abc".to_string())),
            message_channel: sender,
        })
        .expect("Endpoint request failed to send");

    client.accept_port_request(9999, false).await;

    let response = test_utils::expect_mpsc_response(&mut receiver).await;
    match response {
        RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }

    let (sender2, mut receiver2) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForPublishers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            stream_id: None,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Exact(Arc::new("def".to_string())),
            message_channel: sender2,
        })
        .expect("2nd endpoint request failed to send");

    let response = test_utils::expect_mpsc_response(&mut receiver2).await;
    match response {
        RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }
}

#[tokio::test]
async fn can_register_for_specific_port_for_watcher() {
    let (mut client, sender) = RtmpTestClient::new();
    let endpoint = start_rtmp_server_endpoint(sender);

    let (sender, mut receiver) = unbounded_channel();
    let (_media_sender, media_receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForWatchers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Any,
            media_channel: media_receiver,
            notification_channel: sender,
        })
        .expect("Endpoint request failed to send");

    client.accept_port_request(9999, false).await;

    let response = test_utils::expect_mpsc_response(&mut receiver).await;
    match response {
        RtmpEndpointWatcherNotification::WatcherRegistrationSuccessful => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }
}

#[tokio::test]
async fn endpoint_watcher_receives_failed_when_port_rejected() {
    let (mut client, sender) = RtmpTestClient::new();
    let endpoint = start_rtmp_server_endpoint(sender);

    let (sender, mut receiver) = unbounded_channel();
    let (_media_sender, media_receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForWatchers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Any,
            media_channel: media_receiver,
            notification_channel: sender,
        })
        .expect("Endpoint request failed to send");

    client.deny_port_request(9999, false).await;

    let response = test_utils::expect_mpsc_response(&mut receiver).await;
    match response {
        RtmpEndpointWatcherNotification::WatcherRegistrationFailed => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }
}

#[tokio::test]
async fn second_watcher_rejected_on_same_app_when_both_any_stream_key() {
    let (mut client, sender) = RtmpTestClient::new();
    let endpoint = start_rtmp_server_endpoint(sender);

    let (sender, mut receiver) = unbounded_channel();
    let (_media_sender, media_receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForWatchers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Any,
            media_channel: media_receiver,
            notification_channel: sender,
        })
        .expect("Endpoint request failed to send");

    client.accept_port_request(9999, false).await;

    let response = test_utils::expect_mpsc_response(&mut receiver).await;
    match response {
        RtmpEndpointWatcherNotification::WatcherRegistrationSuccessful => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }

    let (sender, mut receiver2) = unbounded_channel();
    let (_media_sender, media_receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForWatchers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Any,
            media_channel: media_receiver,
            notification_channel: sender,
        })
        .expect("Endpoint request failed to send");

    let response = test_utils::expect_mpsc_response(&mut receiver2).await;
    match response {
        RtmpEndpointWatcherNotification::WatcherRegistrationFailed => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }
}

#[tokio::test]
async fn second_watcher_rejected_on_same_app_and_same_exact_key() {
    let (mut client, sender) = RtmpTestClient::new();
    let endpoint = start_rtmp_server_endpoint(sender);

    let (sender, mut receiver) = unbounded_channel();
    let (_media_sender, media_receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForWatchers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Exact(Arc::new("abc".to_string())),
            media_channel: media_receiver,
            notification_channel: sender,
        })
        .expect("Endpoint request failed to send");

    client.accept_port_request(9999, false).await;

    let response = test_utils::expect_mpsc_response(&mut receiver).await;
    match response {
        RtmpEndpointWatcherNotification::WatcherRegistrationSuccessful => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }

    let (sender, mut receiver2) = unbounded_channel();
    let (_media_sender, media_receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForWatchers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Exact(Arc::new("abc".to_string())),
            media_channel: media_receiver,
            notification_channel: sender,
        })
        .expect("Endpoint request failed to send");

    let response = test_utils::expect_mpsc_response(&mut receiver2).await;

    match response {
        RtmpEndpointWatcherNotification::WatcherRegistrationFailed => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }
}

#[tokio::test]
async fn second_watcher_rejected_on_same_app_when_first_request_is_for_any_key() {
    let (mut client, sender) = RtmpTestClient::new();
    let endpoint = start_rtmp_server_endpoint(sender);

    let (sender, mut receiver) = unbounded_channel();
    let (_media_sender, media_receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForWatchers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Any,
            media_channel: media_receiver,
            notification_channel: sender,
        })
        .expect("Endpoint request failed to send");

    client.accept_port_request(9999, false).await;

    let response = test_utils::expect_mpsc_response(&mut receiver).await;
    match response {
        RtmpEndpointWatcherNotification::WatcherRegistrationSuccessful => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }

    let (sender, mut receiver2) = unbounded_channel();
    let (_media_sender, media_receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForWatchers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Exact(Arc::new("abc".to_string())),
            media_channel: media_receiver,
            notification_channel: sender,
        })
        .expect("Endpoint request failed to send");

    let response = test_utils::expect_mpsc_response(&mut receiver2).await;
    match response {
        RtmpEndpointWatcherNotification::WatcherRegistrationFailed => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }
}

#[tokio::test]
async fn second_watcher_rejected_on_same_app_when_first_request_is_for_specific_key() {
    let (mut client, sender) = RtmpTestClient::new();
    let endpoint = start_rtmp_server_endpoint(sender);

    let (sender, mut receiver) = unbounded_channel();
    let (_media_sender, media_receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForWatchers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Exact(Arc::new("abc".to_string())),
            media_channel: media_receiver,
            notification_channel: sender,
        })
        .expect("Endpoint request failed to send");

    client.accept_port_request(9999, false).await;

    let response = test_utils::expect_mpsc_response(&mut receiver).await;

    match response {
        RtmpEndpointWatcherNotification::WatcherRegistrationSuccessful => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }

    let (sender, mut receiver2) = unbounded_channel();
    let (_media_sender, media_receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForWatchers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Any,
            media_channel: media_receiver,
            notification_channel: sender,
        })
        .expect("Endpoint request failed to send");

    let response = test_utils::expect_mpsc_response(&mut receiver2).await;

    match response {
        RtmpEndpointWatcherNotification::WatcherRegistrationFailed => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }
}

#[tokio::test]
async fn second_watcher_accepted_on_same_app_with_different_exact_keys() {
    let (mut client, sender) = RtmpTestClient::new();
    let endpoint = start_rtmp_server_endpoint(sender);

    let (sender, mut receiver) = unbounded_channel();
    let (_media_sender, media_receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForWatchers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Exact(Arc::new("abc".to_string())),
            media_channel: media_receiver,
            notification_channel: sender,
        })
        .expect("Endpoint request failed to send");

    client.accept_port_request(9999, false).await;

    let response = test_utils::expect_mpsc_response(&mut receiver).await;

    match response {
        RtmpEndpointWatcherNotification::WatcherRegistrationSuccessful => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }

    let (sender, mut receiver2) = unbounded_channel();
    let (_media_sender, media_receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForWatchers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Exact(Arc::new("def".to_string())),
            media_channel: media_receiver,
            notification_channel: sender,
        })
        .expect("Endpoint request failed to send");

    let response = test_utils::expect_mpsc_response(&mut receiver2).await;

    match response {
        RtmpEndpointWatcherNotification::WatcherRegistrationSuccessful => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }
}

#[tokio::test]
async fn second_request_fails_if_tls_option_differs() {
    let (mut client, sender) = RtmpTestClient::new();
    let endpoint = start_rtmp_server_endpoint(sender);

    let (sender, mut receiver) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForPublishers {
            port: 9999,
            use_tls: false,
            requires_registrant_approval: false,
            stream_id: None,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Any,
            message_channel: sender,
        })
        .expect("Endpoint request failed to send");

    client.accept_port_request(9999, false).await;

    let response = test_utils::expect_mpsc_response(&mut receiver).await;
    match response {
        RtmpEndpointPublisherMessage::PublisherRegistrationSuccessful => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }

    let (sender2, mut receiver2) = unbounded_channel();
    endpoint
        .send(RtmpEndpointRequest::ListenForPublishers {
            port: 9999,
            use_tls: true,
            requires_registrant_approval: false,
            stream_id: None,
            ip_restrictions: IpRestriction::None,
            rtmp_app: Arc::new("app2".to_string()),
            rtmp_stream_key: StreamKeyRegistration::Any,
            message_channel: sender2,
        })
        .expect("2nd endpoint request failed to send");

    let response = test_utils::expect_mpsc_response(&mut receiver2).await;
    match response {
        RtmpEndpointPublisherMessage::PublisherRegistrationFailed => (),
        x => panic!("Unexpected endpoint response: {:?}", x),
    }
}

#[tokio::test]
async fn publisher_disconnected_if_connecting_to_wrong_app() {
    let mut context = TestContextBuilder::new().into_publisher().await;
    context.client.perform_handshake().await;
    context
        .client
        .connect_to_app("adsfasdfadfs".to_string(), false)
        .await;

    context.client.assert_connection_sender_closed().await;
}

#[tokio::test]
async fn publisher_disconnected_if_connecting_to_wrong_stream_key() {
    let mut context = TestContextBuilder::new()
        .set_stream_key(StreamKeyRegistration::Exact(Arc::new("key".to_string())))
        .into_publisher()
        .await;

    context.client.perform_handshake().await;
    context
        .client
        .connect_to_app(context.rtmp_app.clone(), true)
        .await;

    context
        .client
        .publish_to_stream_key("abc".to_string(), false)
        .await;

    context.client.assert_connection_sender_closed().await;
}

#[tokio::test]
async fn publisher_can_connect_on_registered_app_and_stream_key() {
    let mut context = TestContextBuilder::new()
        .set_stream_key(StreamKeyRegistration::Exact(Arc::new("key".to_string())))
        .into_publisher()
        .await;

    context.client.perform_handshake().await;
    context
        .client
        .connect_to_app(context.rtmp_app.clone(), true)
        .await;

    context
        .client
        .publish_to_stream_key("key".to_string(), true)
        .await;

    let receiver = context.publish_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointPublisherMessage::NewPublisherConnected {
            stream_key,
            connection_id,
            stream_id: _,
            reactor_update_channel: _,
        } => {
            assert_eq!(
                stream_key.as_str(),
                "key",
                "Unexpected stream key in publisher connected message"
            );

            assert_eq!(
                connection_id.0.as_str(),
                rtmp_client::CONNECTION_ID,
                "Unexpected connection id"
            );
        }

        message => panic!("Unexpected publisher message: {:?}", message),
    };
}

#[tokio::test]
async fn publish_stopped_notification_raised_on_disconnection() {
    let mut context = TestContextBuilder::new().into_publisher().await;
    context.set_as_active_publisher().await;

    context.client.disconnect();

    let receiver = context.publish_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointPublisherMessage::PublishingStopped { connection_id } => {
            assert_eq!(
                connection_id.0.as_str(),
                rtmp_client::CONNECTION_ID,
                "Unexpected connection id"
            );
        }

        message => panic!("Unexpected publisher message: {:?}", message),
    };
}

#[tokio::test]
async fn publish_stopped_when_rtmp_client_stops_publishing() {
    let mut context = TestContextBuilder::new().into_publisher().await;
    context.set_as_active_publisher().await;

    context.client.stop_publishing().await;

    let receiver = context.publish_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointPublisherMessage::PublishingStopped { connection_id } => {
            assert_eq!(
                connection_id.0.as_str(),
                rtmp_client::CONNECTION_ID,
                "Unexpected connection id"
            );
        }

        message => panic!("Unexpected publisher message: {:?}", message),
    };
}

#[tokio::test]
async fn notification_raised_when_video_published() {
    let mut context = TestContextBuilder::new().into_publisher().await;
    context.set_as_active_publisher().await;

    let data = Bytes::from(vec![1, 2, 3, 4, 5, 6, 7]);
    let timestamp = RtmpTimestamp::new(5);
    context.client.publish_video(data.clone(), timestamp);

    let receiver = context.publish_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointPublisherMessage::NewVideoData {
            publisher,
            timestamp: event_timestamp,
            data: event_data,
            is_sequence_header: _,
            codec: _,
            is_keyframe: _,
            composition_time_offset: _,
        } => {
            assert_eq!(
                publisher.0.as_str(),
                rtmp_client::CONNECTION_ID,
                "Unexpected connection id"
            );

            assert_eq!(event_timestamp, timestamp, "Unexpected timestamp");

            // Should contain flv tag and avc video packet header stripped out
            assert_eq!(event_data, data[5..], "Unexpected video data");
        }

        message => panic!("Unexpected publisher message: {:?}", message),
    };
}

#[tokio::test]
async fn published_video_detects_h264_codec_when_first_byte_masks_to_0x07() {
    let mut context = TestContextBuilder::new().into_publisher().await;
    context.set_as_active_publisher().await;

    let data = Bytes::from(vec![0x07, 1, 0, 0, 0, 2, 3, 4]);
    let timestamp = RtmpTimestamp::new(5);
    context.client.publish_video(data.clone(), timestamp);

    let receiver = context.publish_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointPublisherMessage::NewVideoData {
            publisher: _,
            timestamp: _,
            data: _,
            is_sequence_header: _,
            codec,
            is_keyframe: _,
            composition_time_offset: _,
        } => {
            assert_eq!(codec, VideoCodec::H264, "Unexpected video codec");
        }

        message => panic!("Unexpected publisher message: {:?}", message),
    };
}

#[tokio::test]
async fn published_video_detects_unknown_codec_when_first_byte_does_not_mask_to_0x07() {
    let mut context = TestContextBuilder::new().into_publisher().await;
    context.set_as_active_publisher().await;

    let data = Bytes::from(vec![0x08, 1, 0, 0, 0, 2, 3, 4]);
    let timestamp = RtmpTimestamp::new(5);
    context.client.publish_video(data.clone(), timestamp);

    let receiver = context.publish_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointPublisherMessage::NewVideoData {
            publisher: _,
            timestamp: _,
            data: _,
            is_sequence_header: _,
            codec,
            is_keyframe: _,
            composition_time_offset: _,
        } => {
            assert_eq!(codec, VideoCodec::Unknown, "Unexpected video codec");
        }

        message => panic!("Unexpected publisher message: {:?}", message),
    };
}

#[tokio::test]
async fn published_video_sequence_header_when_h264_and_second_byte_is_zero() {
    let mut context = TestContextBuilder::new().into_publisher().await;
    context.set_as_active_publisher().await;

    let data = Bytes::from(vec![0x07, 0, 0, 0, 0, 3, 4]);
    let timestamp = RtmpTimestamp::new(5);
    context.client.publish_video(data.clone(), timestamp);

    let receiver = context.publish_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointPublisherMessage::NewVideoData {
            publisher: _,
            timestamp: _,
            data: _,
            is_sequence_header,
            codec: _,
            is_keyframe: _,
            composition_time_offset: _,
        } => {
            assert!(is_sequence_header, "Unexpected sequence header value");
        }

        message => panic!("Unexpected publisher message: {:?}", message),
    };
}

#[tokio::test]
async fn published_video_not_sequence_header_when_h264_and_second_byte_is_not_zero() {
    let mut context = TestContextBuilder::new().into_publisher().await;
    context.set_as_active_publisher().await;

    let data = Bytes::from(vec![0x07, 1, 0, 0, 0, 1, 3, 4]);
    let timestamp = RtmpTimestamp::new(5);
    context.client.publish_video(data.clone(), timestamp);

    let receiver = context.publish_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointPublisherMessage::NewVideoData {
            publisher: _,
            timestamp: _,
            data: _,
            is_sequence_header,
            codec: _,
            is_keyframe: _,
            composition_time_offset: _,
        } => {
            assert!(!is_sequence_header, "Unexpected sequence header value");
        }

        message => panic!("Unexpected publisher message: {:?}", message),
    };
}

#[tokio::test]
async fn published_video_not_key_frame_when_first_4_half_octet_is_not_one() {
    let mut context = TestContextBuilder::new().into_publisher().await;
    context.set_as_active_publisher().await;

    let data = Bytes::from(vec![0x27, 1, 0, 0, 0, 2, 3, 4]);
    let timestamp = RtmpTimestamp::new(5);
    context.client.publish_video(data.clone(), timestamp);

    let receiver = context.publish_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointPublisherMessage::NewVideoData {
            publisher: _,
            timestamp: _,
            data: _,
            is_sequence_header: _,
            codec: _,
            is_keyframe,
            composition_time_offset: _,
        } => {
            assert!(!is_keyframe, "Unexpected sequence header value");
        }

        message => panic!("Unexpected publisher message: {:?}", message),
    };
}

#[tokio::test]
async fn published_video_key_frame_when_first_4_half_octet_is_one() {
    let mut context = TestContextBuilder::new().into_publisher().await;
    context.set_as_active_publisher().await;

    let data = Bytes::from(vec![0x17, 1, 0, 0, 0, 2, 3, 4]);
    let timestamp = RtmpTimestamp::new(5);
    context.client.publish_video(data.clone(), timestamp);

    let receiver = context.publish_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointPublisherMessage::NewVideoData {
            publisher: _,
            timestamp: _,
            data: _,
            is_sequence_header: _,
            codec: _,
            is_keyframe,
            composition_time_offset: _,
        } => {
            assert!(is_keyframe, "Unexpected sequence header value");
        }

        message => panic!("Unexpected publisher message: {:?}", message),
    };
}

#[tokio::test]
async fn notification_raised_when_metadata_published() {
    let mut context = TestContextBuilder::new().into_publisher().await;
    context.set_as_active_publisher().await;

    let mut metadata = StreamMetadata::new();
    metadata.video_width = Some(1920);

    context.client.publish_metadata(metadata.clone());

    let receiver = context.publish_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointPublisherMessage::StreamMetadataChanged {
            publisher,
            metadata: event_metadata,
        } => {
            assert_eq!(
                publisher.0.as_str(),
                rtmp_client::CONNECTION_ID,
                "Unexpected connection id"
            );

            assert_eq!(
                event_metadata, metadata,
                "Unexpected metadata value returned"
            );
        }

        message => panic!("Unexpected publisher message: {:?}", message),
    }
}

#[tokio::test]
async fn notification_raised_when_audio_published() {
    let mut context = TestContextBuilder::new().into_publisher().await;
    context.set_as_active_publisher().await;

    let data = Bytes::from(vec![1, 2, 3, 4]);
    let timestamp = RtmpTimestamp::new(5);
    context.client.publish_audio(data.clone(), timestamp);

    let receiver = context.publish_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointPublisherMessage::NewAudioData {
            publisher,
            timestamp: event_timestamp,
            data: event_data,
            is_sequence_header: _,
        } => {
            assert_eq!(
                publisher.0.as_str(),
                rtmp_client::CONNECTION_ID,
                "Unexpected connection id"
            );

            assert_eq!(event_timestamp, timestamp, "Unexpected timestamp");

            // First two bytes are the flv-tag and packet type, and are stripped out of the notification
            assert_eq!(event_data, data[2..], "Unexpected video data");
        }

        message => panic!("Unexpected publisher message: {:?}", message),
    };
}

#[tokio::test]
async fn published_audio_received_if_first_byte_0xa0() {
    let mut context = TestContextBuilder::new().into_publisher().await;
    context.set_as_active_publisher().await;

    let data = Bytes::from(vec![0xa0, 2, 3, 4]);
    let timestamp = RtmpTimestamp::new(5);
    context.client.publish_audio(data.clone(), timestamp);

    let receiver = context.publish_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointPublisherMessage::NewAudioData { .. } => (),
        message => panic!("Unexpected publisher message: {:?}", message),
    };
}

#[tokio::test]
async fn no_published_audio_when_first_byte_is_not_0xa0() {
    let mut context = TestContextBuilder::new().into_publisher().await;
    context.set_as_active_publisher().await;

    let data = Bytes::from(vec![1, 2, 3, 4]);
    let timestamp = RtmpTimestamp::new(5);
    context.client.publish_audio(data.clone(), timestamp);

    let receiver = context.publish_receiver.as_mut().unwrap();
    test_utils::expect_mpsc_timeout(receiver).await;
}

#[tokio::test]
async fn published_audio_aac_sequence_header_if_second_byte_is_zero() {
    let mut context = TestContextBuilder::new().into_publisher().await;
    context.set_as_active_publisher().await;

    let data = Bytes::from(vec![0xa0, 0, 3, 4]);
    let timestamp = RtmpTimestamp::new(5);
    context.client.publish_audio(data.clone(), timestamp);

    let receiver = context.publish_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointPublisherMessage::NewAudioData {
            publisher: _,
            timestamp: _,
            data: _,
            is_sequence_header,
        } => {
            assert!(is_sequence_header, "Expected is sequence header to be true");
        }

        message => panic!("Unexpected publisher message: {:?}", message),
    };
}

#[tokio::test]
async fn published_audio_aac_not_sequence_header_if_second_byte_is_not_zero() {
    let mut context = TestContextBuilder::new().into_publisher().await;
    context.set_as_active_publisher().await;

    let data = Bytes::from(vec![0xa0, 1, 3, 4]);
    let timestamp = RtmpTimestamp::new(5);
    context.client.publish_audio(data.clone(), timestamp);

    let receiver = context.publish_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointPublisherMessage::NewAudioData {
            publisher: _,
            timestamp: _,
            data: _,
            is_sequence_header,
        } => {
            assert!(
                !is_sequence_header,
                "Expected is sequence header to be false"
            );
        }

        message => panic!("Unexpected publisher message: {:?}", message),
    };
}

#[tokio::test]
async fn stream_becoming_active_notification_when_watcher_connects() {
    let mut context = TestContextBuilder::new().into_watcher().await;
    context.client.perform_handshake().await;
    context
        .client
        .connect_to_app(context.rtmp_app.clone(), true)
        .await;

    context
        .client
        .watch_stream_key("key".to_string(), true)
        .await;

    let receiver = context.watch_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointWatcherNotification::StreamKeyBecameActive {
            stream_key,
            reactor_update_channel: _,
        } => {
            assert_eq!(stream_key.as_str(), "key");
        }

        message => panic!("Unexpected publisher message received: {:?}", message),
    };
}

#[tokio::test]
async fn stream_becomes_inactive_when_only_watcher_stops_playback() {
    let mut context = TestContextBuilder::new().into_watcher().await;
    context.set_as_active_watcher().await;
    context.client.stop_watching().await;

    let receiver = context.watch_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointWatcherNotification::StreamKeyBecameInactive { stream_key } => {
            assert_eq!(stream_key.as_str(), "key");
        }

        message => panic!("Unexpected publisher message received: {:?}", message),
    }
}

#[tokio::test]
async fn stream_becomes_inactive_when_only_watcher_disconnects() {
    let mut context = TestContextBuilder::new().into_watcher().await;
    context.set_as_active_watcher().await;
    context.client.disconnect();

    let receiver = context.watch_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointWatcherNotification::StreamKeyBecameInactive { stream_key } => {
            assert_eq!(stream_key.as_str(), "key");
        }

        message => panic!("Unexpected publisher message received: {:?}", message),
    }
}

#[tokio::test]
async fn watcher_receives_metadata() {
    let mut context = TestContextBuilder::new().into_watcher().await;
    context.set_as_active_watcher().await;

    let mut metadata = StreamMetadata::new();
    metadata.video_width = Some(1920);

    match context
        .media_sender
        .as_ref()
        .unwrap()
        .send(RtmpEndpointMediaMessage {
            stream_key: Arc::new("key".to_string()),
            data: RtmpEndpointMediaData::NewStreamMetaData {
                metadata: metadata.clone(),
            },
        }) {
        Ok(_) => (),
        Err(_) => panic!("Failed to send media message"),
    }

    let event = context
        .client
        .get_next_event()
        .await
        .expect("Expected an event returned");

    match event {
        ClientSessionEvent::StreamMetadataReceived {
            metadata: received_metadata,
        } => {
            assert_eq!(
                received_metadata, metadata,
                "Received metadata did not matc"
            );
        }

        event => panic!("Unexpected event raised: {:?}", event),
    }
}

#[tokio::test]
async fn watcher_receives_video_wrapped_in_flv_tag_denoting_non_keyframe() {
    let mut context = TestContextBuilder::new().into_watcher().await;
    context.set_as_active_watcher().await;

    let sent_data = Bytes::from(vec![1, 2, 3, 4]);
    let sent_timestamp = RtmpTimestamp::new(5);

    match context
        .media_sender
        .as_ref()
        .unwrap()
        .send(RtmpEndpointMediaMessage {
            stream_key: Arc::new("key".to_string()),
            data: RtmpEndpointMediaData::NewVideoData {
                codec: H264,
                data: sent_data.clone(),
                is_sequence_header: false,
                is_keyframe: false,
                timestamp: sent_timestamp,
                composition_time_offset: 0,
            },
        }) {
        Ok(_) => (),
        Err(_) => panic!("Failed to send media message"),
    }

    let event = context
        .client
        .get_next_event()
        .await
        .expect("Expected an event returned");

    match event {
        ClientSessionEvent::VideoDataReceived { data, timestamp } => {
            assert_eq!(
                &data,
                &vec![0x27, 1, 0, 0, 0, 1, 2, 3, 4],
                "Unexpected bytes"
            );
            assert_eq!(timestamp, sent_timestamp, "Unexpected timestamp");
        }

        event => panic!("Unexpected event raised: {:?}", event),
    }
}

#[tokio::test]
async fn watcher_receives_video_wrapped_in_flv_tag_denoting_keyframe() {
    let mut context = TestContextBuilder::new().into_watcher().await;
    context.set_as_active_watcher().await;

    let sent_data = Bytes::from(vec![1, 2, 3, 4]);
    let sent_timestamp = RtmpTimestamp::new(5);

    match context
        .media_sender
        .as_ref()
        .unwrap()
        .send(RtmpEndpointMediaMessage {
            stream_key: Arc::new("key".to_string()),
            data: RtmpEndpointMediaData::NewVideoData {
                codec: H264,
                data: sent_data.clone(),
                is_sequence_header: false,
                is_keyframe: true,
                timestamp: sent_timestamp,
                composition_time_offset: 0,
            },
        }) {
        Ok(_) => (),
        Err(_) => panic!("Failed to send media message"),
    }

    let event = context
        .client
        .get_next_event()
        .await
        .expect("Expected an event returned");

    match event {
        ClientSessionEvent::VideoDataReceived { data, timestamp } => {
            assert_eq!(
                &data,
                &vec![0x17, 1, 0, 0, 0, 1, 2, 3, 4],
                "Unexpected bytes"
            );
            assert_eq!(timestamp, sent_timestamp, "Unexpected timestamp");
        }

        event => panic!("Unexpected event raised: {:?}", event),
    }
}

#[tokio::test]
async fn watcher_does_not_receive_non_h264_video() {
    let mut context = TestContextBuilder::new().into_watcher().await;
    context.set_as_active_watcher().await;

    let sent_data = Bytes::from(vec![1, 2, 3, 4]);
    let sent_timestamp = RtmpTimestamp::new(5);

    match context
        .media_sender
        .as_ref()
        .unwrap()
        .send(RtmpEndpointMediaMessage {
            stream_key: Arc::new("key".to_string()),
            data: RtmpEndpointMediaData::NewVideoData {
                codec: Unknown,
                data: sent_data.clone(),
                is_sequence_header: false,
                is_keyframe: false,
                timestamp: sent_timestamp,
                composition_time_offset: 0,
            },
        }) {
        Ok(_) => (),
        Err(_) => panic!("Failed to send media message"),
    }

    let event = context.client.get_next_event().await;
    if let Some(event) = event {
        panic!("Expected no events, but got {:?}", event);
    }
}

#[tokio::test]
async fn aac_audio_has_flv_headers_added_for_sequence_header() {
    let mut context = TestContextBuilder::new().into_watcher().await;
    context.set_as_active_watcher().await;

    let sent_data = Bytes::from(vec![10, 11, 3, 4]);
    let sent_timestamp = RtmpTimestamp::new(5);

    match context
        .media_sender
        .as_ref()
        .unwrap()
        .send(RtmpEndpointMediaMessage {
            stream_key: Arc::new("key".to_string()),
            data: RtmpEndpointMediaData::NewAudioData {
                data: sent_data.clone(),
                is_sequence_header: true,
                timestamp: sent_timestamp,
            },
        }) {
        Ok(_) => (),
        Err(_) => panic!("Failed to send media message"),
    };

    let event = context
        .client
        .get_next_event()
        .await
        .expect("Expected event returned");
    match event {
        ClientSessionEvent::AudioDataReceived { data, timestamp } => {
            assert_eq!(&data, &vec![0xaf, 0, 10, 11, 3, 4], "Unexpected audio data");
            assert_eq!(timestamp, sent_timestamp, "Unexpected timestamp");
        }

        event => panic!("Unexpected event: {:?}", event),
    }
}

#[tokio::test]
async fn aac_audio_has_flv_headers_added_for_non_sequence_header() {
    let mut context = TestContextBuilder::new().into_watcher().await;
    context.set_as_active_watcher().await;

    let sent_data = Bytes::from(vec![10, 11, 3, 4]);
    let sent_timestamp = RtmpTimestamp::new(5);

    match context
        .media_sender
        .as_ref()
        .unwrap()
        .send(RtmpEndpointMediaMessage {
            stream_key: Arc::new("key".to_string()),
            data: RtmpEndpointMediaData::NewAudioData {
                data: sent_data.clone(),
                is_sequence_header: false,
                timestamp: sent_timestamp,
            },
        }) {
        Ok(_) => (),
        Err(_) => panic!("Failed to send media message"),
    };

    let event = context
        .client
        .get_next_event()
        .await
        .expect("Expected event returned");
    match event {
        ClientSessionEvent::AudioDataReceived { data, timestamp } => {
            assert_eq!(&data, &vec![0xaf, 1, 10, 11, 3, 4], "Unexpected audio data");
            assert_eq!(timestamp, sent_timestamp, "Unexpected timestamp");
        }

        event => panic!("Unexpected event: {:?}", event),
    }
}

#[tokio::test]
async fn watcher_does_not_receives_unknown_audio_codec() {
    let mut context = TestContextBuilder::new().into_watcher().await;
    context.set_as_active_watcher().await;

    let sent_data = Bytes::from(vec![1, 2, 3, 4]);
    let sent_timestamp = RtmpTimestamp::new(5);

    match context
        .media_sender
        .as_ref()
        .unwrap()
        .send(RtmpEndpointMediaMessage {
            stream_key: Arc::new("key".to_string()),
            data: RtmpEndpointMediaData::NewAudioData {
                data: sent_data.clone(),
                is_sequence_header: false,
                timestamp: sent_timestamp,
            },
        }) {
        Ok(_) => (),
        Err(_) => panic!("Failed to send media message"),
    };

    let event = context.client.get_next_event().await;
    if let Some(event) = event {
        panic!("Expected no events, but got {:?}", event);
    }
}

#[tokio::test]
async fn consumer_accepts_publisher() {
    let mut context = TestContextBuilder::new()
        .set_requires_registrant_approval(true)
        .into_publisher()
        .await;

    context.client.perform_handshake().await;
    context
        .client
        .connect_to_app(context.rtmp_app.clone(), true)
        .await;

    context
        .client
        .publish_to_stream_key("key".to_string(), false)
        .await;

    let receiver = context.publish_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointPublisherMessage::PublisherRequiringApproval {
            stream_key,
            connection_id,
            response_channel,
        } => {
            assert_eq!(stream_key.as_str(), "key", "Unexpected stream key");
            assert_eq!(
                connection_id.0.as_str(),
                rtmp_client::CONNECTION_ID,
                "Unexpected connection id"
            );

            let (_sender, receiver) = unbounded_channel();
            response_channel
                .send(ValidationResponse::Approve {
                    reactor_update_channel: receiver,
                })
                .expect("Failed to send approval")
        }

        message => panic!("Unexpected publisher message received: {:?}", message),
    }

    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointPublisherMessage::NewPublisherConnected {
            reactor_update_channel,
            connection_id,
            stream_id: _,
            stream_key,
        } => {
            assert_eq!(
                connection_id.0.as_str(),
                rtmp_client::CONNECTION_ID,
                "Unexpected connection id"
            );
            assert_eq!(stream_key.as_str(), "key", "Unexpected stream key");
            assert!(
                reactor_update_channel.is_some(),
                "Expected a reactor channel"
            );
        }

        message => panic!("Unexpected publisher message received: {:?}", message),
    }
}

#[tokio::test]
async fn consumer_rejectin_publisher_disconnects_client() {
    let mut context = TestContextBuilder::new()
        .set_requires_registrant_approval(true)
        .into_publisher()
        .await;

    context.client.perform_handshake().await;
    context
        .client
        .connect_to_app(context.rtmp_app.clone(), true)
        .await;

    context
        .client
        .publish_to_stream_key("key".to_string(), false)
        .await;

    let receiver = context.publish_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointPublisherMessage::PublisherRequiringApproval {
            stream_key,
            connection_id,
            response_channel,
        } => {
            assert_eq!(stream_key.as_str(), "key", "Unexpected stream key");
            assert_eq!(
                connection_id.0.as_str(),
                rtmp_client::CONNECTION_ID,
                "Unexpected connection id"
            );

            response_channel
                .send(ValidationResponse::Reject)
                .expect("Failed to send approval");
        }

        message => panic!("Unexpected publisher message received: {:?}", message),
    }

    context.client.assert_connection_sender_closed().await;
}

#[tokio::test]
async fn consumer_accepts_watcher() {
    let mut context = TestContextBuilder::new()
        .set_requires_registrant_approval(true)
        .into_watcher()
        .await;

    context.client.perform_handshake().await;
    context
        .client
        .connect_to_app(context.rtmp_app.clone(), true)
        .await;

    context
        .client
        .watch_stream_key("key".to_string(), false)
        .await;

    let receiver = context.watch_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointWatcherNotification::WatcherRequiringApproval {
            stream_key,
            connection_id,
            response_channel,
        } => {
            assert_eq!(stream_key.as_str(), "key", "Unexpected stream key");
            assert_eq!(
                connection_id.0.as_str(),
                rtmp_client::CONNECTION_ID,
                "Unexpected connection id"
            );

            let (_sender, receiver) = unbounded_channel();
            response_channel
                .send(ValidationResponse::Approve {
                    reactor_update_channel: receiver,
                })
                .expect("Failed to send approval")
        }

        message => panic!("Unexpected publisher message received: {:?}", message),
    }

    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointWatcherNotification::StreamKeyBecameActive {
            stream_key,
            reactor_update_channel,
        } => {
            assert_eq!(stream_key.as_str(), "key", "Unexpected stream key");
            assert!(
                reactor_update_channel.is_some(),
                "Expected reactor update channel"
            );
        }

        message => panic!("Unexpected publisher message received: {:?}", message),
    }
}

#[tokio::test]
async fn consumer_rejecting_watcher_disconnects_client() {
    let mut context = TestContextBuilder::new()
        .set_requires_registrant_approval(true)
        .into_watcher()
        .await;

    context.client.perform_handshake().await;
    context
        .client
        .connect_to_app(context.rtmp_app.clone(), true)
        .await;

    context
        .client
        .watch_stream_key("key".to_string(), false)
        .await;

    let receiver = context.watch_receiver.as_mut().unwrap();
    let response = test_utils::expect_mpsc_response(receiver).await;
    match response {
        RtmpEndpointWatcherNotification::WatcherRequiringApproval {
            stream_key,
            connection_id,
            response_channel,
        } => {
            assert_eq!(stream_key.as_str(), "key", "Unexpected stream key");
            assert_eq!(
                connection_id.0.as_str(),
                rtmp_client::CONNECTION_ID,
                "Unexpected connection id"
            );

            response_channel
                .send(ValidationResponse::Reject)
                .expect("Failed to send approval");
        }

        message => panic!("Unexpected publisher message received: {:?}", message),
    }

    context.client.assert_connection_sender_closed().await;
}
