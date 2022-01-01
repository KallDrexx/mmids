# Components

Mmids is based around several core components

## Workflow Steps

A workflow step is a process that is instructed to ingest, distribute, or manipulate media.  Some examples of functionality enabled by different workflow steps are:

* Receive RTMP video from publishers on specific stream keys
* Split media off to other workflows
* Instruct ffmpeg to transcode video to a new resolution
* Push media out to other servers via RTMP

## Workflows

A workflow is a media pipeline defined by a linear sequence of workflow steps.  When media is received by a workflow step, the media will be handed to the step defined afterwards for processing.  

There is no limit on the number of steps a single workflow can contain.

## Web Based API

Mmids contains a web based API that can be used to query information about running workflows, as well as starting, stopping, and updating workflows on the fly.  

The web based API is the main way to make updates to a running mmids instance on a proactive (e.g. "push") basis.  

!!! warning
    Any change to workflows made via the web based API will not be saved. To persist workflow changes after a restart you will either need to update the mmids.config file, or re-send your workflow updates via the web API after restart.

## Reactors

A reactor is a system which different workflow steps can ask about the validity of a stream, and request workflows to be created for that specific stream.  When a reactor gets a query for a stream name, it will reach out to the configured external service to ask what workflows are valid for the specified stream name.  

If no workflows are returned then the reactor assumes the stream name is not valid and will send a response stating such.

If at least one workflow is returned, then the reactor will create the specified workflows and tell the system that made the query that the stream name is valid, and which workflows it should route media on that stream to.