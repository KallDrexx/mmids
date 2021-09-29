pub mod rtmp_receive;

use super::{MediaNotification};
use downcast_rs::{Downcast, impl_downcast};
use futures::future::BoxFuture;

pub trait StepFutureResult: Downcast {}
impl_downcast!(StepFutureResult);

#[derive(Clone, Debug, PartialEq)]
pub enum StepStatus {
    Created,
    Active,
    Error
}

pub struct StepInputs {
    pub media: Vec<MediaNotification>,
    pub notifications: Vec<Box<dyn StepFutureResult>>,
}

pub struct StepOutputs<'a> {
    pub media: Vec<MediaNotification>,
    pub futures: Vec<BoxFuture<'a, Box<dyn StepFutureResult>>>,
}

pub struct StepExecutionIO<'a> {
    pub inputs: StepInputs,
    pub outputs: StepOutputs<'a>,
}

pub trait WorkflowStep {
    fn init<'a>(&mut self) -> Vec<BoxFuture<'a, Box<dyn StepFutureResult>>>;
    fn get_status(&self) -> StepStatus;
    fn execute(&mut self, data: &mut StepExecutionIO);
}