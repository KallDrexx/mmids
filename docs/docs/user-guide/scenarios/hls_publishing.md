# HLS Publishing

In this scenario we want publishers to be able to send video via RTMP into a mmids instance, and a CDN serve the created HLS feeds to viewers.

```mermaid
graph TD

Pub[Video Publisher]
mmids
Cdn
folder[Filesystem]
v1[Viewer 1]
v2[Viewer 2]
v3[Viewer 3]

Pub -->|rtmp://server/publish/stream1| mmids
mmids -->|/var/www/hls/stream1.m3u8| folder
folder --> Cdn


Cdn --> v1
Cdn -->|https://some-url/path/stream1.m3u8| v2
Cdn --> v3
```

This can be accomplished with the following configuration:

```
workflow hls {
    rtmp_receive rtmp_app=publis stream_key=*
    ffmpeg_hls path=/var/www/hls duration=2 count=5
}
```
