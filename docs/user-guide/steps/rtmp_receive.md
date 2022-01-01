# Rtmp Receive

The RTMP receive workflow step allows RTMP clients to connect to mmids as a publisher and send video into a workflow. All media streams received by this step will have a stream name the same as the stream key the publisher sent video on.  The media streams received are then passed on to subsequent steps.

The step will register with the internal RTMP subsystem based on the arguments given.  If the RTMP subsystem rejects the registration attempt, then the step will be in an errored state.  

The RTMP subsystem will usually only reject a registration if another workflow step is already registered for publishers to the port/application/stream key combination, or if registering for RTMPS connections on a port already used for RTMP (or vice versa).

## Configuration

The RTMP Receive step is configured with the step type name of `rtmp_receive`.  It supports the following arguments:

* Required Arguments
    * `rtmp_app=<name>`
        * Specifies the name of the rtmp application the step expects publishers to connect to
    * `stream_key=<key>`
        * What stream key this step should accept RTMP publishers on (relative to the specified RTMP application.  The value can be given as `*` to accept any stream key on that RTMP application.
* Optional Arguments
    * `port=<number>`
        * The port number to accept RTMP connections on.  
        * If not specified port `1935` is used, unless `rtmps` flag is used in which case port `443` is the port used.
    * `rtmps`
        * Specifies that it will only accept connections with RTMPS.
    * `allow_ips=<ip_list>`
        * Contains one or more IP addresses or subnet masks that are allowed to publish. 
        * Multiple entries should be separated with a comma
        * Only IPv4 addresses are supported
        * E.g. `allow_ips=192.168.0.1,10.0.0.1,127.0.0.0/24`
    * `deny_ips=<ip_lists>`
        * Contains one or more IP addresses or subnet masks that are *not* allowed to publish.
        * Multiple entries should be separated with a comma
        * Only IPv4 addresses are supported
        * Not allowed to be used at the same time as `allow_ips`.
        * E.g. `deny_ips=192.168.0.1,10.0.0.1,127.0.0.0/24`
    * `reactor=<name>`
        * Specifies the reactor that stream keys should be validated with. When a new RTMP publisher connects, the Rtmp receive step will pass the stream key to the reactor.  If the reactor returns a result specifying the stream name is not valid then the publisher will be disconnected.

## Error Conditions

The RTMP receive step can go into an error state if the attempt to register with the RTMP subsystem is rejected.  

This usually happens when:

* The port cannot be opened due to it being in use for other (non-RTMP) purposes
* The port is used by the RTMP subsystem but used for RTMPS when requested to be non-RTMPS (or vice versa)
* The port, rtmp application, and stream key combination are already registered for publishers
    * This includes if one workflow step registers for a wildcard but another workflow step registers for an exact stream key.

