//! This module provides abstractions over MPSC channels, which make it easy for workflow steps
//! to execute a future and send the results of those futures back to the correct workflow runner
//! with minimal allocations.

pub struct WorkflowStepFuturesChannel {}
