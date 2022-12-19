use crate::encoders::{SampleResult, VideoEncoder, VideoEncoderGenerator};
use crate::utils::create_gst_element;
use anyhow::{anyhow, Context, Result};
use bytes::{Bytes, BytesMut};
use gstreamer::prelude::*;
use gstreamer::{Element, FlowError, FlowSuccess, Pipeline};
use gstreamer_app::{AppSink, AppSinkCallbacks, AppSrc};
use mmids_core::codecs::{VIDEO_CODEC_H264_AVC, VideoCodec};
use mmids_core::workflows::{MediaNotificationContent, MediaType};
use mmids_core::VideoTimestamp;
use std::collections::HashMap;
use std::iter;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tracing::error;
use mmids_core::workflows::metadata::{MediaPayloadMetadataCollection, MetadataEntry, MetadataKey, MetadataValue};

/// Creates an encoder that passes video packets through to the output channel without modification
pub struct VideoCopyEncoderGenerator {
    pub pts_offset_metadata_key: MetadataKey,
}

impl VideoEncoderGenerator for VideoCopyEncoderGenerator {
    fn create(
        &self,
        pipeline: &Pipeline,
        _parameters: &HashMap<String, Option<String>>,
        media_sender: UnboundedSender<MediaNotificationContent>,
    ) -> Result<Box<dyn VideoEncoder>> {
        Ok(Box::new(VideoCopyEncoder::new(media_sender, pipeline, self.pts_offset_metadata_key)?))
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
        pts_offset_metadata_key: MetadataKey,
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
            .map_err(|_| anyhow!("Video copy encoder's appsink could not be casted"))?;

        let codec_data: Arc<Mutex<Option<CodecInfo>>> = Arc::new(Mutex::new(None));
        let copy_of_codec_data = codec_data.clone();
        let mut sent_codec_data = false;
        let mut codec_data_error_raised = false;
        let mut codec = VideoCodec::Unknown;
        let mut metadata_buffer = BytesMut::new();
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
                            let _ = media_sender.send(MediaNotificationContent::MediaPayload {
                                media_type: MediaType::Video,
                                payload_type: VIDEO_CODEC_H264_AVC.clone(),
                                timestamp: Duration::new(0, 0),
                                is_required_for_decoding: true,
                                data: info.sequence_header.clone(),
                                metadata: MediaPayloadMetadataCollection::new(iter::empty(), &mut metadata_buffer),
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

                    let timestamp = sample.to_video_timestamp();
                    let pts_offset = MetadataEntry::new(
                        pts_offset_metadata_key,
                        MetadataValue::I32(timestamp.pts_offset()),
                        &mut metadata_buffer,
                    ).unwrap(); // Can only panic if the key is not for an i32

                    let _ = media_sender.send(MediaNotificationContent::MediaPayload {
                        media_type: MediaType::Video,
                        payload_type: VIDEO_CODEC_H264_AVC.clone(),
                        timestamp: timestamp.dts(),
                        is_required_for_decoding: false,
                        data: sample.content,
                        metadata: MediaPayloadMetadataCollection::new(
                            [pts_offset].into_iter(),
                            &mut metadata_buffer,
                        ),
                    });

                    Ok(FlowSuccess::Ok)
                })
            .build(),
        );

        let appsrc = appsrc
            .dynamic_cast::<AppSrc>()
            .map_err(|_| anyhow!("Video copy encoder's appsrc could not be casted"))?;

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
                .map_err(|_| anyhow!("Video copy encoder's lock was poisoned"))?;

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
