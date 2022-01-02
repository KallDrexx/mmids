# Quick Start

## Download mmids application

Official releases of the mmids server can be downloaded from the [official releases page](https://github.com/KallDrexx/mmids/releases).

Extract the files in the whatever directory you wish to run it from, e.g. c:\mmids.

## Download an FFMPEG executable

Download a release of ffmpeg from [their official site](https://ffmpeg.org/) and place it in any location.

Ffmpeg is currently required to pull, transcode, push, and create HLS feeds.

## Configuration
Navigate to the directory the mmids release was extracted to and create a new file called `mmids.config`. Open the file in any text editor and set the context to the following:

```
settings {
  http_api_port 9011
  
  # Replace <path-to-ffmpeg.exe> with the path to the ffmpeg downloaded
  ffmpeg_path <path-to-ffmpeg.exe> 
}

workflow simple {
  rtmp_receive rtmp_app=publish stream_key=*
  rtmp_watch rtmp_app=watch stream_key=*
}
```

## Run mmids

Navigate to the directory you created the `mmids.config` file into and run `mmids-app.exe`.

!!! note 
    
    Running the executable from within a command prompt is preferred when making configuration changes to ensure visiblity of configuration errors. 

You should now see the console filling with messages and should see a message such as:

> 2021-12-31T17:57:26.706818Z INFO mmids_core::workflows::runner: All pending steps moved to active  
> at C:\Users\me\code\mmids\mmids-core\mmids-core\src\workflows\runner\mod.rs:594   
> in mmids_core::workflows::runner::Workflow Execution with , workflow_name: simple

## Verification via HTTP API

Open a browser to `http://localhost:9011`.  You should see: 

> Mmids version x.x.x

Next you can navigate to `http://localhost:9011/workflows`, and you should see your single defined "simple" workflow running.  

```json
[
  {
    "name": "simple"
  }
]
```

Finally, you can get details about the running workflow by navigating to `http://localhost:9011/workflows/simple`, which should show

```json
{
  "status": "Running",
  "active_steps": [
    {
      "step_id": "17261577973137769032",
      "step_type": "rtmp_receive",
      "parameters": {
        "rtmp_app": "publish",
        "stream_key": "*"
      },
      "status": "Active"
    },
    {
      "step_id": "8917233449957578608",
      "step_type": "rtmp_watch",
      "parameters": {
        "rtmp_app": "watch",
        "stream_key": "*"
      },
      "status": "Active"
    }
  ],
  "pending_steps": []
}
```

## Publishing and Watching Video
Open up your favorite encoder and send video to `rtmp://localhost/publish/test`.  You can then load any video player that supports RTMP (such as VLC) and watch the published stream by connecting to `rtmp://localhost/watch/test`.
