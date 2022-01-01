# Configuration

Mmids is configured by writing into a file named `mmids.config`.  The configuration syntax is a custom node based syntax, inspired by the NGINX configuration format.

In general, you write configurations by defining nodes, giving those nodes arguments, and then define child nodes within the root nodes.  The basic concept is:

```
<node type> <arguments> {
    <child-node> <arguments>
    <child-node> <arguments>
    ...
}
```

Node types are predefined, and the type of arguments supported by each depends on the node itself.

Any line can contain comments by placing a number sign (#) and the comments after it.  All characters after the number sign are treated as comments and ignored until it reaches the end of the line.

## Settings Node

Only one setting node is allowed, and the node itself has no arguments.  Inside the setting node, each setting should be specified followed by a single optional (depending on the setting being specified) argument.  Valid settings are:

* `ffmpeg_path` - This is the relative or absolute path to the ffmpeg executable.  This setting is required for mmids to run.
* `log_path` - This is the relative or absolute path to where mmids should write logs.  This setting is required for mmids to run.
* `http_api_port` - This is the port that the HTTP API will run on.  If not specified than the HTTP API will be disabled
* `tls_cert_path` - This is the relative or absolute path to where a pfx certificate can be found. This certificate will be used for RTMPS connections.  If not specified than RTMPS support will be disabled.
* `tls_cert_password` - This is the password that can be used to open the pfx certificate.  If not specified than RTMPS support will be disabled

An example settings configuration would be

```
settings {
    ffmpeg_path c:\tools\ffmpeg\bin\ffmpeg.exe
    log_path logs
    tls_cert_path cert.pfx
    tls_cert_password abcd
    http_api_port 9011
}
```

## Reactor Node

Multiple reactor nodes can be specified.  This is mostly meant to allow for different URLs to be accessed in different circumstances.

All reactor configurations in the official mmids application will have the following look

```
reactor <name> executor=simple_http update_interval=<interval> {
    url <url>
}
```

* `<name>` - The name for this reactor.  The name is used so workflow steps know which reactor to send queries for.  Every reactor must have a unique name. Names can-not have spaces in them.
* `<interval>` - How many seconds until the reactor should execute another query.  This is used for a reactor to auto-update workflows after it has started managing them.  An update interval of 0 disables auto-updating.
* `<url>` - This is the full URL the reactor should use for queries.

## Workflow Node

Multiple workflow nodes can be specified, with workflow steps defined as their child nodes.  Workflow nodes are configured as:

```
workflow <name> {
    <steps>
}
```

* `<name>` - the name to give to the workflow.  Every defined workflow must have a unique name.  This name will be the same used when querying or modifying the workflow via the HTTP API.  
* `<steps>` - One or more workflow steps that this workflow should contain.  The order in which steps are defined dictate the order in which media will be processed.  For example, placing a step to allow video playback before a transcode step will cause the pre-transcoded video to be played back, while placing the playback step after the transcode step will cause the transcoded video to be played back.

## Workflow Steps

Each workflow step is configured in the following format:

```
    <step type> <arguments>
```

* `<step_type>` - This is the name of the step to be used.  The names of each step are predetermined based on the workflow step.
* `<arguments>` - One or more arguments that are specific to the step being requested.

For details on how to configure any specific step, see [the workflow steps documentation](workflow-steps.md).