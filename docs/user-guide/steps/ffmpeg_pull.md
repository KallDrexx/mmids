# ffmpeg Pull

The ffmpeg Pull step creates an ffmpeg process to ingest the specified media file or url into the workflow.  

## Configuration

The ffmpeg Pull step is utilized with the step type name `ffmpeg_pull`.  It supports the following arguments:

* `location=<path>`
    * Specifies the file path or url of the media to ingest
* `stream_name=<name>`
    * Specifies the name the ingested media stream have internally.

