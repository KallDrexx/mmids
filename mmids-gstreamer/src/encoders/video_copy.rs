use crate::encoders::{SampleResult, VideoEncoder, VideoEncoderGenerator};
use crate::utils::create_gst_element;
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use gstreamer::prelude::*;
use gstreamer::{Element, FlowError, FlowSuccess, Pipeline};
use gstreamer_app::{AppSink, AppSinkCallbacks, AppSrc};
use mmids_core::codecs::VideoCodec;
use mmids_core::workflows::MediaNotificationContent;
use mmids_core::VideoTimestamp;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::UnboundedSender;
use tracing::error;

pub struct VideoCopyEncoderGenerator {}

impl VideoEncoderGenerator for VideoCopyEncoderGenerator {
    fn create(
        &self,
        pipeline: &Pipeline,
        _parameters: &HashMap<String, Option<String>>,
        media_sender: UnboundedSender<MediaNotificationContent>,
    ) -> Result<Box<dyn VideoEncoder>> {
        Ok(Box::new(VideoCopyEncoder::new(media_sender, pipeline)?))
    }
}

struct CodecInfo {
    codec: VideoCodec,
    sequence_header: Bytes,
}

struct VideoCopyEncoder {
    source: AppSrc,
    codec_data: Arc<Mutex<Option<CodecInfo>>>,
}

impl VideoCopyEncoder {
    fn new(
        media_sender: UnboundedSender<MediaNotificationContent>,
        pipeline: &Pipeline,
    ) -> Result<VideoCopyEncoder> {
        // While we won't be mutating the stream, we want to pass it through a gstreamer pipeline
        // so the packets will be synchronized with audio in case of transcoding delay.

        let appsrc = create_gst_element("appsrc")?;
        let queue = create_gst_element("queue")?;
        let appsink = create_gst_element("appsink")?;

        pipeline
            .add_many(&[&appsrc, &queue, &appsink])
            .with_context(|| "Failed to add video copy encoder's elements to the pipeline")?;

        Element::link_many(&[&appsrc, &queue, &appsink])
            .with_context(|| "Failed to link video copy encoder's elements together")?;

        let appsink = appsink
            .dynamic_cast::<AppSink>()
            .or_else(|_| Err(anyhow!("Video copy encoder's appsink could not be casted")))?;

        let codec_data: Arc<Mutex<Option<CodecInfo>>> = Arc::new(Mutex::new(None));
        let copy_of_codec_data = codec_data.clone();
        let mut sent_codec_data = false;
        let mut codec_data_error_raised = false;
        let mut codec = VideoCodec::Unknown;
        appsink.set_callbacks(
            AppSinkCallbacks::builder()
                .new_sample(move |sink| {
                    if !sent_codec_data {
                        let data = match copy_of_codec_data.lock() {
                            Ok(data) => data,
                            Err(_) => {
                                if !codec_data_error_raised {
                                    error!("codec data lock was poisoned");
                                    codec_data_error_raised = true;
                                }

                                return Err(FlowError::Error);
                            }
                        };

                        if let Some(info) = &*data {
                            let _ = media_sender.send(MediaNotificationContent::Video {
                                codec: info.codec.clone(),
                                data: info.sequence_header.clone(),
                                timestamp: VideoTimestamp::from_zero(),
                                is_keyframe: false,
                                is_sequence_header: true,
                            });

                            codec = info.codec.clone();
                            sent_codec_data = true;
                        } else {
                            if !codec_data_error_raised {
                                error!("Received data prior to codec data being set. This shouldn't happen");
                                codec_data_error_raised = true;
                            }
                        }
                    }

                    let sample = SampleResult::from_sink(sink)
                        .or_else(|_| Err(FlowError::CustomError))?;

                    let _ = media_sender.send(MediaNotificationContent::Video {
                        codec,
                        data: sample.content,
                        timestamp: sample.timestamp,
                        is_sequence_header: false,
                        is_keyframe: false, // TODO: preserve this somehow
                    });

                    Ok(FlowSuccess::Ok)
                })
            .build(),
        );

        let appsrc = appsrc
            .dynamic_cast::<AppSrc>()
            .or_else(|_| Err(anyhow!("Video copy encoder's appsrc could not be casted")))?;

        Ok(VideoCopyEncoder {
            source: appsrc,
            codec_data,
        })
    }
}

impl VideoEncoder for VideoCopyEncoder {
    fn push_data(
        &self,
        codec: VideoCodec,
        data: Bytes,
        timestamp: VideoTimestamp,
        is_sequence_header: bool,
    ) -> Result<()> {
        if is_sequence_header {
            let mut codec_data = self
                .codec_data
                .lock()
                .or_else(|_| Err(anyhow!("Video copy encoder's lock was poisoned")))?;

            *codec_data = Some(CodecInfo {
                codec,
                sequence_header: data,
            })
        } else {
            let buffer =
                crate::utils::set_gst_buffer(data, Some(timestamp.dts()), Some(timestamp.pts()))
                    .with_context(|| "Failed to set buffer")?;

            self.source
                .push_buffer(buffer)
                .with_context(|| "Could not push buffer into copy encoder's source")?;
        }

        Ok(())
    }
}
