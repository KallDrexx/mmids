//! Endpoints are components that handle interactions with external systems.  The external
//! systems may be other programs (managed via shell/process calls) or networked systems (such as
//! the logic for handling inbound or outbound RTMP connections).  Endpoints are usually idle until
//! invoked by workflow steps.

pub mod ffmpeg;
pub mod rtmp_server;
pub mod webrtc_server;
