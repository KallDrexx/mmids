use crate::encoders::{AudioEncoder, AudioEncoderGenerator, SampleResult};
use crate::utils::{create_gst_element, set_gst_buffer};
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use gstreamer::prelude::*;
use gstreamer::{Element, FlowError, FlowSuccess, Pipeline};
use gstreamer_app::{AppSink, AppSinkCallbacks, AppSrc};
use mmids_core::codecs::AudioCodec;
use mmids_core::workflows::MediaNotificationContent;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tracing::error;

/// Generates an audio encoder that passes audio packets to the output channel without modification.
pub struct AudioCopyEncoderGenerator {}

impl AudioEncoderGenerator for AudioCopyEncoderGenerator {
    fn create(
        &self,
        pipeline: &Pipeline,
        _parameters: &HashMap<String, Option<String>>,
        media_sender: UnboundedSender<MediaNotificationContent>,
    ) -> Result<Box<dyn AudioEncoder>> {
        Ok(Box::new(AudioCopyEncoder::new(media_sender, pipeline)?))
    }
}

struct CodecInfo {
    codec: AudioCodec,
    sequence_header: Bytes,
}

struct AudioCopyEncoder {
    source: AppSrc,
    codec_data: Arc<Mutex<Option<CodecInfo>>>,
}

impl AudioCopyEncoder {
    fn new(
        media_sender: UnboundedSender<MediaNotificationContent>,
        pipeline: &Pipeline,
    ) -> Result<AudioCopyEncoder> {
        // While we won't be mutating the stream, we want to pass it through a gstreamer pipeline
        // so the packets will be synchronized with possibly transcoded video delay.

        let appsrc = create_gst_element("appsrc")?;
        let queue = create_gst_element("queue")?;
        let appsink = create_gst_element("appsink")?;

        pipeline
            .add_many(&[&appsrc, &queue, &appsink])
            .with_context(|| "Failed to add audio copy encoder's elements to the pipeline")?;

        Element::link_many(&[&appsrc, &queue, &appsink])
            .with_context(|| "Failed to link audio copy encoder's elements together")?;

        let appsink = appsink
            .dynamic_cast::<AppSink>()
            .map_err(|_| anyhow!("Audio copy encoder's appsink could not be casted"))?;

        let codec_data: Arc<Mutex<Option<CodecInfo>>> = Arc::new(Mutex::new(None));
        let copy_of_codec_data = codec_data.clone();
        let mut sent_codec_data = false;
        let mut codec_data_error_raised = false;
        let mut codec = AudioCodec::Unknown;
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
                            let _ = media_sender.send(MediaNotificationContent::Audio {
                                codec: info.codec,
                                data: info.sequence_header.clone(),
                                timestamp: Duration::new(0, 0),
                                is_sequence_header: true,
                            });

                            codec = info.codec;
                            sent_codec_data = true;
                        } else if !codec_data_error_raised {
                            error!("Received data prior to codec data being set. This shouldn't happen");
                            codec_data_error_raised = true;
                        }
                    }

                    let sample = SampleResult::from_sink(sink)
                        .map_err(|_| FlowError::CustomError)?;

                    let _ = media_sender.send(MediaNotificationContent::Audio {
                        codec,
                        data: sample.content,
                        timestamp: sample.dts.unwrap_or(Duration::new(0, 0)),
                        is_sequence_header: false,
                    });

                    Ok(FlowSuccess::Ok)
                })
                .build(),
        );

        let appsrc = appsrc
            .dynamic_cast::<AppSrc>()
            .map_err(|_| anyhow!("Audio copy encoder's appsrc could not be casted"))?;

        Ok(AudioCopyEncoder {
            source: appsrc,
            codec_data,
        })
    }
}

impl AudioEncoder for AudioCopyEncoder {
    fn push_data(
        &self,
        codec: AudioCodec,
        data: Bytes,
        timestamp: Duration,
        is_sequence_header: bool,
    ) -> Result<()> {
        if is_sequence_header {
            let mut codec_data = self
                .codec_data
                .lock()
                .map_err(|_| anyhow!("Audio copy encoder's lock was poisoned"))?;

            *codec_data = Some(CodecInfo {
                codec,
                sequence_header: data,
            })
        } else {
            let buffer = set_gst_buffer(data, Some(timestamp), None)
                .with_context(|| "Failed to set audio buffer")?;

            self.source
                .push_buffer(buffer)
                .with_context(|| "Could not push buffer into audio copy encoder's source")?;
        }

        Ok(())
    }
}
