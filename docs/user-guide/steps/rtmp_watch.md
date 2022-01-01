# Rtmp Watch

The RTMP watch workflow step allows RTMP clients to connect to mmmids as a playback client, and watch an existing stream.  

The step will register with the internal RTMP subsystem based on the arguments given.  If the RTMP subsystem rejects the registration attempt, then the step will be in an errored state.  

The RTMP subsystem will usually only reject a registration if another workflow step is already registered for playback clients for the port/application/stream key combination, or if registering for RTMPS connections on a port already used for RTMP (or vice versa).

Playback clients will not be disconnected if they initiate playback on a stream that is not active yet. The client will be held and served video when the stream becomes active.

## Configuration

The RTMP Watch step is configured with the step type name of `rtmp_watch`.  It supports the following arguments:

* Required Arguments
    * `rtmp_app=<name>`
        * Specifies the name of the rtmp application the step expects playback clients to connect to
    * `stream_key=<key>`
        * The stream key that RTMP clients can use to watch media that is actively flowing through this step.  
        * When a stream key of `*` is given, all media streams will be playable by RTMP clients when they connect with the same stream key as the stream name of the media stream
        * What stream key this step should accept RTMP playback clients on (relative to the specified RTMP application.  The value can be given as `*` to accept any stream key on that RTMP application.
        * I
* Optional Arguments
    * `port=<number>`
        * The port number to accept RTMP connections on.  
        * If not specified port `1935` is used, unless `rtmps` flag is used in which case port `443` is the port used.
    * `rtmps`
        * Specifies that it will only accept connections with RTMPS.
    * `allow_ips=<ip_list>`
        * Contains one or more IP addresses or subnet masks that are allowed to watch. 
        * Multiple entries should be separated with a comma
        * Only IPv4 addresses are supported
        * E.g. `allow_ips=192.168.0.1,10.0.0.1,127.0.0.0/24`
    * `deny_ips=<ip_lists>`
        * Contains one or more IP addresses or subnet masks that are *not* allowed to watch.
        * Multiple entries should be separated with a comma
        * Only IPv4 addresses are supported
        * Not allowed to be used at the same time as `allow_ips`.
        * E.g. `deny_ips=192.168.0.1,10.0.0.1,127.0.0.0/24`
    * `reactor=<name>`
        * Specifies the reactor that stream keys should be validated with. When a new RTMP playback client connects, the Rtmp receive step will pass the stream key to the reactor.  If the reactor returns a result specifying the stream name is not valid then the playback client will be disconnected.

## Error Conditions

The RTMP receive step can go into an error state if the attempt to register with the RTMP subsystem is rejected.  

This usually happens when:

* The port cannot be opened due to it being in use for other (non-RTMP) purposes
* The port is used by the RTMP subsystem but used for RTMPS when requested to be non-RTMPS (or vice versa)
* The port, rtmp application, and stream key combination are already registered for playbakc clients
    * This includes if one workflow step registers for a wildcard but another workflow step registers for an exact stream key.
