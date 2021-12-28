use std::fmt::Debug;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot::Receiver;
use tokio::time::timeout;

pub async fn expect_mpsc_response<T>(receiver: &mut UnboundedReceiver<T>) -> T {
    match timeout(Duration::from_millis(10), receiver.recv()).await {
        Ok(Some(response)) => response,
        Ok(None) => panic!("Channel unexpectedly closed"),
        Err(_) => panic!("No response received within timeout period"),
    }
}

pub async fn expect_oneshot_response<T>(receiver: Receiver<T>) -> T {
    match timeout(Duration::from_millis(10), receiver).await {
        Ok(Ok(response)) => response,
        Ok(Err(_)) => panic!("Channel unexpectedly closed"),
        Err(_) => panic!("No response received within timeout period"),
    }
}

pub async fn expect_mpsc_timeout<T>(receiver: &mut UnboundedReceiver<T>)
where
    T: Debug,
{
    match timeout(Duration::from_millis(10), receiver.recv()).await {
        Ok(Some(response)) => panic!("Expected timeout, instead received {:?}", response),
        Ok(None) => panic!("Channel unexpectedly closed"),
        Err(_) => (),
    }
}
