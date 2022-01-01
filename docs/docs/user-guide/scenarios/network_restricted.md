# Network Restricted Publishing

In this scenario, we only want to allow publishers from within the local intranet, and only on RTMPS.  RTMP viewers are unrestricted.

```mermaid
graph TD
A -->|rtmps://10.0.0.27/live/stream| B
B -->|rtmp://server/live/stream| C
B -->|rtmp://server/live/stream| D
B -->|rtmp://server/live/stream| E
subgraph 10.0.0.15
    A[Video Publisher]
end
subgraph 10.0.0.27
    B[mmids]
end
subgraph Public Internet
    C[Viewer 1]
    D[Viewer 2]
    E[Viewer 3]
end
```

This can be done with the following configuration:

```
workflow network_publish {
    rtmp_receive rtmp_app=live stream_key=* rtmps allow_ips=10.0.0.0/24
    rtmp_watch rtmp_app=live stream_key=*
}
```

