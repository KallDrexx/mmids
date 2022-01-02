# mmids

Mmids (Multi-Media Ingestion and Distribution System) is a powerful, user friendly, open source live video workflow server.

* **User Friendly**
    * Complex video workflows can be easily configured by non-developers
* **Observable**
    * Logging with an emphasis on corelations.  Easily pinpoint logs relevant to a single stream on a busy server
* **Developer Friendly**
    * Trivially add new workflow logic, new network protocols, etc... all with your own open-source or proprietary components
* **Fully Dynamic**
    * Push and pull mechanisms to start, stop, and update workflows on the fly without disruptions.

See the [official documentation](https://kalldrexx.github.io/mmids/) for more details.
  
## What's In This Repository?

* [mmids-app](mmids-app) 
  * This is the official mmids application
* [mmids-core](mmids-core) 
  * This is the main crate which contains all the logic which runs mmids.  It also contains all types needed to extend mmids, or create your own custom distribution
* [reactor-test-server](reactor-test-server)
  * This contains a basic HTTP server that can respond to `simple_http` reactor executor queries.  
  * When a `POST` request comes in to `http://localhost:9055/<category>`  with a stream name in the body, it will look for the file `workflows/<category>/<stream name>.mmids`.  If one exists it will return the content of that file with a `200`, otherwise returns a `404`.
  * Primarily used for testing reactors
* [validators](validators)
  * These are different applications that were written to independently test different components.
  * [echo-server](validators/echo-server) - Used to test the TCP socket manager
  * [ffmpeg-runner](validators/ffmpeg-runner) - Used to test the ffmpeg endpoint
  * [rtmp-server](validators/rtmp-server) - Used to test the rtmp endpoint



