# Workflow Forwarder

The Workflow Forwarder step allows sending media streams to other workflows.  This allows for non-linear flows for a media stream.

For example, you can have a media stream forwarded to two different workflows for transcoding at different bitrates/resolutions without double transcoding.

## Configuration

The workflow forwarder step is utilized with the `workflow_forwarder` step type name.  The supported arguments are:

* `target_workflow=<name>`
    * Specifies the target workflow all media streams should be forwarded to
* `reactor=<name>`
    * Specifies the name of the reactor to check where to forward any given media stream to
    * Each media stream will be forwarded to different workflows depending on the results of the reactor.  If the reactor returns no workflows then that media stream won't be routed anywhere.

!!! note

    Both arguments are not supported together.  Either `target_workflow` or `reactor` can be specified, but not both.

