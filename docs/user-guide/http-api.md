# HTTP API

Mmids provides an HTTP based API for health checking and modification purposes.  It is the "push" method of changing workflows, allowing you to start, stop, and update workflows as needed.  

The API runs on the port specified by the `http_port` setting value.  If no port is specified, or the setting is not provided, then the HTTP API will be disabled.

The API is bound to `127.0.0.1`, and thus is not accessible from external machines.

## GET /

`GET` requests to the root (`/`) return information about the version of mmids that's currently running. It also works to act as a health check to know if mmids is currently running or not.

## GET /workflows

`GET` requests to `/workflows` will return a JSON array of workflows that are currently running within mmids.  

## GET /workflows/&lt;name&gt;

`GET` requests to `/workflows/<name>`, where `<name>` is the name of a workflow, will return details about that workflow in JSON format.  It will provide the current status of the workflow (e.g. `Running` or error details), which steps are active, and which steps are pending. 

Steps pending mean they are waiting for some action to be completed, such as registration with another system (e.g. the RTMP subsystem).  It's possible that a pending task can cause a workflow to enter an error'd state, and in this case this API call will make that clear.

If the workflow does not exist, than a `400 Not Found` will be returned.

## PUT /workflows

`PUT` requests to `/workflows` allows starting or updating a single workflow.  The definition of a workflow is specified in the HTTP request body in the same configuration format as specified in the `mmids.config` file [see the workflow node section for more info](configuration.md#Workflow%20Node).

If the workflow specified in the HTTP request body already exists, then the workflow will be updated to match what was requested.  Any workflow steps that currently exist but were not in the passed in workflow definition will be removed, and any workflow steps that are new will be created.  

!!! note

    If a workflow step is currently active, and the new workflow definition passed in the HTTP request has the step **with the same exact parameters**, then the step will be kept and not be recreated.  This means that an `rtmp_receive` and `rtmp_watch` step with the same exact parameters will not disconnect any clients that are active publishing or watching streams.  However, if any parameters are added to this step, the step will be recreated and thus all current connections to those steps will be disconnected.

!!! warning

    When inserting or removing workflow steps that come before other steps, you should be careful to understand when encoding parameters may be changed.  If you are transcoding a media stream before sending them to playback clients and you remove the transcode step, the playback clients will receive new video and audio h264 headers for the pre-transcoded media feed.  Many systems do not handle a change of encoding parameters mid stream and may not properly decode subsequent video calls.  

    Thus if adding or removing transcoding steps is desired, then downstream steps should be modified as well to ensure correct operations.

!!! note

    It's important to track if a workflow was created by a reactor before updating it.  If a reactor is managing the specific workflow and you change it, the reactor may update it again to put it back in it's previous state.

## DELETE /workflows/&lt;name&gt;

`DELETE` requests to `/workflows/<name>`, where `<name>` is the name of a workflow, will cause the workflow with the specified name to be stopped and all clients utilizing steps within that workflow will be removed.

!!! note

    Deleting a workflow managed by a reactor may only be temprorary, as the reactor may end up re-creating the workflow again.