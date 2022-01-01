# ffmpeg Push

The ffmpeg Push step utilizes ffmpeg to push a media stream to a target location.

!!! warning

    The ffmpeg Push step does not support dynamic push targetting. If multiple media streams come into the step then they will all be sent to the target.

    This step is meant to be used in workflows that have a single media stream.

## Configuration

The ffmpeg Push step can be utilized with the step type name `ffmpeg_push`.  The supported arguments are:

* `target=<url>`
    * The url to send the media stream to

