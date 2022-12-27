use crate::encoders::{SampleResult, VideoEncoder, VideoEncoderGenerator};
use crate::utils::{create_gst_element, get_codec_data_from_element};
use anyhow::{anyhow, Context, Result};
use bytes::{Bytes, BytesMut};
use gstreamer::prelude::*;
use gstreamer::{Caps, Element, FlowError, FlowSuccess, Fraction, Pipeline};
use gstreamer_app::{AppSink, AppSinkCallbacks, AppSrc};
use mmids_core::codecs::{VIDEO_CODEC_H264_AVC, VideoCodec};
use mmids_core::workflows::{MediaNotificationContent, MediaType};
use mmids_core::VideoTimestamp;
use std::collections::HashMap;
use std::iter;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, warn};
use mmids_core::workflows::metadata::{MediaPayloadMetadataCollection, MetadataEntry, MetadataKey, MetadataValue};

/// Creates a video encoder that uses the gstreamer `x264enc` encoder to encode video into h264
/// video.
///
/// This encoder supports the following optional parameters:
/// * `width` - How many pixels wide the resulting video should be
/// * `height` - How many pixels high the resulting video should be
/// * `fps` - The exact fps the resulting video should be
/// * `bitrate` - the desired bitrate specified in **kbps**.  Output will be encoded with constant bitrate
/// * `preset` - The `speed-preset` value to use in the encoder.  Valid values are: `ultrafast`,
/// `superfast`, `veryfast`, `faster`, `fast`, `medium`, `slow`, `slower`, `veryslow`.  The default
/// is `medium`.
pub struct X264EncoderGenerator {
    pub pts_offset_metadata_key: MetadataKey,
}

impl VideoEncoderGenerator for X264EncoderGenerator {
    fn create(
        &self,
        pipeline: &Pipeline,
        parameters: &HashMap<String, Option<String>>,
        media_sender: UnboundedSender<MediaNotificationContent>,
    ) -> Result<Box<dyn VideoEncoder>> {
        Ok(Box::new(X264Encoder::new(
            media_sender,
            parameters,
            pipeline,
            self.pts_offset_metadata_key,
        )?))
    }
}

struct X264Encoder {
    source: AppSrc,
}

impl X264Encoder {
    fn new(
        media_sender: UnboundedSender<MediaNotificationContent>,
        parameters: &HashMap<String, Option<String>>,
        pipeline: &Pipeline,
        pts_offset_metadata_key: MetadataKey,
    ) -> Result<X264Encoder> {
        let height = get_number(parameters, "height");
        let width = get_number(parameters, "width");
        let preset = parameters.get("preset").unwrap_or(&None);
        let fps = get_number(parameters, "fps");
        let bitrate = get_number(parameters, "bitrate");

        let appsrc = create_gst_element("appsrc")?;
        let queue = create_gst_element("queue")?;
        let decoder = create_gst_element("decodebin")?;
        let scale = create_gst_element("videoscale")?;
        let rate_changer = create_gst_element("videorate")?;
        let capsfilter = create_gst_element("capsfilter")?;
        let encoder = create_gst_element("x264enc")?;
        let output_parser = create_gst_element("h264parse")?;
        let appsink = create_gst_element("appsink")?;

        pipeline
            .add_many(&[
                &appsrc,
                &queue,
                &decoder,
                &scale,
                &rate_changer,
                &capsfilter,
                &encoder,
                &output_parser,
                &appsink,
            ])
            .with_context(|| "Failed to add x264 encoder's elements to pipeline")?;

        Element::link_many(&[&appsrc, &queue, &decoder])
            .with_context(|| "Failed to link appsrc -> queue -> decoder")?;

        Element::link_many(&[
            &scale,
            &rate_changer,
            &capsfilter,
            &encoder,
            &output_parser,
            &appsink,
        ])
        .with_context(|| "Failed to link scale to sink")?;

        // decodebin's video pad is added dynamically
        let link_destination = scale;
        decoder.connect_pad_added(move |src, src_pad| {
            match src.link_pads(
                Some(&src_pad.name()),
                &link_destination.clone(),
                Some("sink"),
            ) {
                Ok(_) => (),
                Err(_) => error!(
                    src_caps = ?src_pad.caps(),
                    dest_caps = ?link_destination.static_pad("sink").unwrap().caps(),
                    "Failed to link `decodebin`'s {} pad to videoscale element",
                    src_pad.name()
                ),
            }
        });

        let mut caps = Caps::builder("video/x-raw");
        if let Some(height) = height {
            caps = caps.field("height", height as i32);
        }

        if let Some(width) = width {
            caps = caps.field("width", width as i32);
        }

        if let Some(fps) = fps {
            caps = caps.field("framerate", Fraction::new(fps as i32, 1));
        }

        let caps = caps.build();
        capsfilter.set_property("caps", caps);

        encoder.set_property_from_str("tune", "zerolatency");

        if let Some(preset) = preset {
            encoder.set_property_from_str("speed-preset", preset.as_str());
        }

        if let Some(bitrate) = bitrate {
            encoder.set_property("bitrate", bitrate);
        }

        let appsink = appsink
            .dynamic_cast::<AppSink>()
            .map_err(|_| anyhow!("appsink could not be cast to 'AppSink'"))?;

        let mut sent_codec_data = false;
        let mut metadata_buffer = BytesMut::new();
        appsink.set_callbacks(
            AppSinkCallbacks::builder()
                .new_sample(move |sink| {
                    match sample_received(
                        sink,
                        &mut sent_codec_data,
                        &output_parser,
                        media_sender.clone(),
                        pts_offset_metadata_key,
                        &mut metadata_buffer,
                    ) {
                        Ok(_) => Ok(FlowSuccess::Ok),
                        Err(error) => {
                            error!("new_sample callback error received: {:?}", error);
                            Err(FlowError::Error)
                        }
                    }
                })
                .build(),
        );

        let appsrc = appsrc
            .dynamic_cast::<AppSrc>()
            .map_err(|_| anyhow!("source element could not be cast to 'Appsrc'"))?;

        Ok(X264Encoder { source: appsrc })
    }
}

impl VideoEncoder for X264Encoder {
    fn push_data(
        &self,
        payload_type: Arc<String>,
        data: Bytes,
        timestamp: VideoTimestamp,
        is_sequence_header: bool,
    ) -> Result<()> {
        let buffer =
            crate::utils::set_gst_buffer(data, Some(timestamp.dts()), Some(timestamp.pts()))
                .with_context(|| "Failed to set buffer")?;

        if is_sequence_header {
            crate::utils::set_source_video_sequence_header(&self.source, payload_type, buffer)
                .with_context(|| "Failed to set sequence header for x264 encoder")?;
        } else {
            self.source
                .push_buffer(buffer)
                .with_context(|| "Failed to push the buffer into video source")?;
        }

        Ok(())
    }
}

fn get_number(parameters: &HashMap<String, Option<String>>, key: &str) -> Option<u32> {
    if let Some(Some(inner)) = parameters.get(key) {
        match inner.parse() {
            Ok(num) => return Some(num),
            Err(_) => warn!("Parameter {key} had a value of '{inner}', which is not a number"),
        }
    }

    None
}

fn sample_received(
    sink: &AppSink,
    codec_data_sent: &mut bool,
    output_parser: &Element,
    media_sender: UnboundedSender<MediaNotificationContent>,
    pts_offset_metadata_key: MetadataKey,
    metadata_buffer: &mut BytesMut,
) -> Result<()> {
    if !*codec_data_sent {
        // Pull the codec_data/sequence header out from the output parser
        let codec_data = get_codec_data_from_element(output_parser)?;

        let _ = media_sender.send(MediaNotificationContent::MediaPayload {
            media_type: MediaType::Video,
            payload_type: VIDEO_CODEC_H264_AVC.clone(),
            timestamp: Duration::from_millis(0),
            is_required_for_decoding: true,
            data: codec_data,
            metadata: MediaPayloadMetadataCollection::new(iter::empty(), metadata_buffer),
        });

        *codec_data_sent = true;
    }

    let sample = SampleResult::from_sink(sink).with_context(|| "Failed to get x264enc sample")?;
    let timestamp = sample.to_video_timestamp();
    let pts_offset = MetadataEntry::new(
        pts_offset_metadata_key,
        MetadataValue::I32(timestamp.pts_offset()),
        metadata_buffer,
    ).unwrap(); // Can only panic if the key is not for an i32

    let _ = media_sender.send(MediaNotificationContent::MediaPayload {
        media_type: MediaType::Video,
        payload_type: VIDEO_CODEC_H264_AVC.clone(),
        timestamp: timestamp.dts(),
        is_required_for_decoding: false,
        data: sample.content,
        metadata: MediaPayloadMetadataCollection::new(
            [pts_offset].into_iter(),
            metadata_buffer,
        ),
    });

    Ok(())
}
