# ffmpeg Transcode

The ffmpeg transcode step takes all media streams that are passed into it, passes it to ffmpeg with specific transcode operations, then the transcoded resuling media streams are passed into the next steps.

## Configuration

The ffmpeg transcode step is utliized by using the step type name `ffmpeg_transcode`.  It supports the following arguments:

* `vcodec=<codec>`
    * This parameter is **required**
    * The video codec to transcode the video stream with.
    * Supports:
        * `copy` to keep the current media stream's video properties
        * `h264` to encode the video as h264
* `h264_preset=<preset>`
    * When the `h264` `vcodec` is specified, this argument determines which video preset to use.
    * Supported values are: `ultrafast`, `superfast`, `veryfast`, `faster`, `fast`, `medium`, `slow`, `slower`, and `veryslow`
    * When the `h264` codec is specified, this parameter is **required**.
* `size=<width>x<height>`
    * When the `h264` `vcodec` is specified, this argument specifies the width and height of the resulting video.
    * If not specified then the video will retain its original size.
* `kbps=<kbps>`
    * When the `h264` `vcodec` is specified, this argument will attempt to constrain the bitrate of the video to the bitrate specified
    * The value provided will be used for the min and max bitrate parameters
