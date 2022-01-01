# Mmids

Mmids (Multi-Media Ingestion and Distribution System) is a powerful, user friendly, open source live video workflow server.  

* **User Friendly**
    * Complex video workflows can be easily configured by non-developers
* **Observable**
    * Logging with an emphasis on corelations.  Easily pinpoint logs relevant to a single stream on a busy server
* **Developer Friendly**
    * Trivially add new workflow logic, new network protocols, etc... all with your own open-source or proprietary components
* **Fully Dynamic**
    * Push and pull mechanisms to start, stop, and update workflows on the fly without disruptions.

## What Can The Server Do? 

Today, mmids can:

* Receive audio/video via RTMP/s publishers
* Serve audio/video to RTMP/s based clients
* Ingest audio and video from external sources, such as a remote
* Perform on the fly transcoding of live video
* Generate HLS feeds for video
* Push live video out to external RTMP servers.

The power of mmids comes from how all these type of systems can be composed together. Below is a fully working mmids configuration file:

```nginx
settings {
    ffmpeg_path c:\ffmpeg\bin\ffmpeg.exe    # location of ffmpeg executable
    log_path logs                           # place logs in a 'logs' subfolder 
    tls_cert_path cert.pfx                  # The certificate to use for rtmps
    tls_cert_password abcd                  # password for the certificate
    http_api_port 9011                      # port for the HTTP api
}

# This workflow allows RTMP publishers to send video to 
# `rtmp://server/basic_read/<stream_key>` for any stream key. These video streams 
# are then available to RTMP clients to watch on 
# `rtmp://server/basic_watch/<stream_key>, using the same stream key that the incoming 
# video came in on.
workflow basic_read_watch {
    rtmp_receive rtmp_app=basic_read stream_key=* 
    rtmp_watch rtmp_app=basic_watch stream_key=* 
}

# This workflow demonstrates a more complex video workflow.
# * We accept video via RTMP via `rtmp://server/receive/<stream_key>`
# * An HLS feed is created for archival purposes to `c:\temp\hls\archive`
# * Another HLS feed is created for live preview purposes to `c:\temp\hls\live`
# * Surface the video to RTMP clients via `rtmp://server/preview/<stream_key>`
# * Transcode the video to 640x480 at 1mbps
# * Create an HLS feed of the transcoded video to `c:\temp\hls\result`
# * Allow RTMP clients to watch the transcoded feed on `rtmp://server/watch/<stream_key>`
# * Finally, push the transcoded video to youtube's `abcdefg` stream key
workflow transcode_test {
    rtmp_receive rtmp_app=receive stream_key=*
    ffmpeg_hls path=c:\temp\hls\archive duration=2 count=0 
    ffmpeg_hls path=c:\temp\hls\preview duration=2 count=5 
    rtmp_watch rtmp_app=preview stream_key=*
    ffmpeg_transcode vcodec=h264 acodec=aac h264_preset=ultrafast size=640x480 kbps=1000
    ffmpeg_hls path=c:\temp\hls\result duration=2
    rtmp_watch rtmp_app=watch stream_key=*
    ffmpeg_push target=rtmp://a.rtmp.youtube.com/live2/abcdefg
}

# Simple workflow that shows multi-streaming capabilities.  We take in a video stream,
# expose it to RTMP clients for previewing, then push the video out to multiple other 
# external RTMP servers
workflow multi_streaming {
    rtmp_receive rtmp_app=multistream stream_key=abc1234
    rtmp_watch rtmp_app=multistream-watch stream_key=abc1234
    ffmpeg_push target=rtmp://a.rtmp.youtube.com/live2/some-youtube-key
    ffmpeg_push target=rtmps://live-api-s.facebook.com/rtmp/some-facebook-stream-key
}

# Pull video from a live HLS feed and allow rtmp players to watch 
workflow pull_test {
   ffmpeg_pull location=https://ll-hls-test.apple.com/llhls1/multi.m3u8 stream_name=pull_test
   rtmp_watch rtmp_app=pull stream_key=pull
}
```
