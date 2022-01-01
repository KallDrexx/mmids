# Reactors

Reactors create and manage workflows in reactions to new streams, and is the "pull" mechanism for dynamic workflows in mmids.  

Certain workflow steps have the (optional) ability to request that a reactor create workflows for a given stream name.  The current workflow steps that support this so far are the [RTMP receive](steps/rtmp_receive.md), [RTMP watch](steps/rtmp_watch.md), and the [Workflow forwarder](steps/workflow_forwarder.md) steps.

When a reactor receives a request to create workflows for a specific stream name, the reactor will make a call to an external system based on how that reactor was configured.  

## Request Execution

The method that reactors call external systems are called `Reactor Executors`.  The official mmids distribution only contains a single executor, called `simple_http`.  This executor will make an HTTP `POST` call to the url set in the reactor's configuration.  The HTTP request will have a content type of `text/plain` and the body will only contain the name of the stream being queried.

The `simple_http` executor expects the server to respond with:

* `404` - The stream name is not valid or allowed
* `200` - The stream name **is** valid and allowed (even if no workflows are returned)

!!! note

    The `simple_http` executor has simple retry logic, where if it receives a status code that's not `400` or `200` it will try again 2 more times, once after 5 seconds and again after another 15 seconds.  It will consider the stream as not valid if the 3rd retry failse.


In order to respond to the executor with workflows, the target server **must** respond with one or more workflows [defined the same way you would in the configuration(configuration.md#Workflow%20Node)], with one small addition.

Workflows defined as responses to reactor queries come in two types, routable and non-routable.  Routable workflows have an argument `routed_by_reactor` added to the workflow node.  When the reactor sees a workflow as routable, it will return the name of that workflow in it's response to the workflow step that sent the request to the reactor.  

This instructs some workflow steps (such as the workflow forwarder) on where to redirect media streams with this stream name. 

An example of a reactor response would be: 

```
workflow abc_watch routed_by_reactor {
    rtmp_watch rtmp_app=watch stream_key=abc
}

workflow abc_ingest {
    ffmpeg_pull location=https://ll-hls-test.apple.com/llhls1/multi.m3u8 stream_name=abc
    workflow_forwarder target_workflow=original_workflow
}
```

This will have the reactor create two workflows, one named `abc_ingest` and another `abc_watch`.  The original workflow that called the reactor will only forward its media streams to `abc_watch` since `abc_ingest` is not marked as `routed_by_reactor`.  In most cases reactors will respond with worklows with `routed_by_reactor` enabled, but some advanced configurations such as the above can be used for viewer load balancing, where you only ingest media from the source when there is an active watcher.

!!! warning

    It is important to make sure that reactors return workflows with unique names for different stream names.  If two stream names cause reactors to manage the same workflow name, then it's possible that the workflow can change or be stopped unexpectedly.


## Auto Updating

When a reactor is configured with a `update_interval` argument that's greater than zero, the reactor will re-run execution based on the interval's value (in seconds) until the stream that requested it is gone.  This allows the workflow to dynamically change while the stream is active, including stopping any workflows that the external system decides is no longer valid after it has begun.  


