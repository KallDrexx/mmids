# ffmpeg HLS

The ffmpeg HLS step passes all media streams it receives to ffmpeg to generate an HLS playlist for each. Each media stream an HLS playlist is generated for will have a file name based on the stream name. 

So for example, if the video comes in via a stream key of `abcd`, then the resulting HLS playlist will have the filename of `abcd.m3u8`.

!!! warning

    ffmpeg will overwrite the HLS playlist if one already exists with the same name.

## Configuration

The ffmpeg HLS step is utilized with the step type name of `ffmpeg_hls`.  It supports the following arguments:

* `path=<directory>`
    * This is a **required** argument that tells ffmpeg what directory to place the HLS playlist in.
* `duration=<number>`
    * Specifies how many seconds each HLS segment should be.
* `count=<number>`
    * Specifies the maximum number of HLS segments that should be in the HLS playlist.
    * If the number `0` is specified, then the HLS playlist will retain all segments

