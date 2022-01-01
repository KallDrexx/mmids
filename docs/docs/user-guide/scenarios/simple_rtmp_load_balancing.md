# Simple RTMP Load Balancing

In this scenario we want to serve many RMTP clients for a single stream, but don't want to overload the ingestion server.  We want to be able to add new playback nodes on-demand.

```mermaid
graph TD
A[Video Publisher]
B[Ingestion mmids]
C1[Playback mmids 1]
C2[Playback mmids 2]
C3[Playback mmids 3]

LB[Load Balancer]

V1[Viewer]
V2[Viewer]
V3[Viewer]
V4[Viewer]
V5[Viewer]

A -->|rtmp://ingest-server/live/stream| B
B --> C1
B -->|pull rtmp://ingest-server/live/stream| C2
B --> C3

C1 --> LB
C2 --> LB
C3 --> LB

LB --> V1
LB --> V2
LB -->|rtmp://load-balancer/live/watch| V3
LB --> V4
LB --> V5
```

The configuration for the ingestion mmids instance would be:

```
workflow ingestion {
    rtmp_receive rtmp_app=live stream_key=stream
    rtmp_watch rtmp_app=live stream_key=stream
}
```

The configuration for each playback mmids instance would be:

```
workflow playback {
    ffmpeg_pull location=rtmp://ingest-server/live/stream stream_name=watch
    rtmp_watch rtmp_app=live stream_key=watch
}
```

