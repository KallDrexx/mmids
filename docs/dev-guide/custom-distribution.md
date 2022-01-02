# Custom Distribution

When creating custom components, or wanting to cut down on available functionality in mmids for specific purposes, you will need to create your own mmids application.  

The mmids application is responsible for creating the tokio runtime, creating all actors that are desired to be run, defining any HTTP routes, then letting the application run until it is desired to exit (e.g. exit on `ctrl+c`).  

You can use [the official mmids application](https://github.com/KallDrexx/mmids/blob/master/mmids-app/src/main.rs) as a guide, but the general order of operations are:

* Start logging infrastructure
    * Mmids uses the [tracing crate](https://github.com/tokio-rs/tracing) for logging
* Start all required endpoints
    * Since most workflow steps will need to interact with endpoints, they will need a reference to the already created channels at the time of their creation to function
    * This will also include starting the TCP socket manager if an endpoint is created that needs it.
* Start the event hub
    * A lot of different components will require the event hub, and thus it needs to be started early on
* Start reactor manager
    * First a `mmids_core::reactors::executors::ReactorExecutorFactory` needs to be created, and any executors you wish to have available should be registered.
    * Then the reactor manager can be started
    * Finally, you can use the reactor manager's channel to create all the reactors that are desired to be created
* Register available steps
    * Create a `mmids_core::workflows::steps::factory::WorkflowStepFactory`, and then register all workflow steps that should be included.  
    * Once all steps have been registered, wrap the factory in an `Arc`, to ensure it can be passed around as needed.
* Create workflow manager and initial workflows
    * Now the workflow manager can be created, and the provided channel can be used to start any workflows that should be started immediately.
* Start the HTTP Api
    * Now start the HTTP API.

With those steps, everything should be ready to go.


