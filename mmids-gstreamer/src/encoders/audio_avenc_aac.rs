use crate::encoders::{AudioEncoder, AudioEncoderGenerator, SampleResult};
use crate::utils::{
    create_gst_element, get_codec_data_from_element, set_gst_buffer,
    set_source_audio_sequence_header,
};
use anyhow::{anyhow, Context, Result};
use bytes::{Bytes, BytesMut};
use gstreamer::prelude::*;
use gstreamer::{Element, FlowError, FlowSuccess, Pipeline};
use gstreamer_app::{AppSink, AppSinkCallbacks, AppSrc};
use mmids_core::codecs::AUDIO_CODEC_AAC_RAW;
use mmids_core::workflows::metadata::MediaPayloadMetadataCollection;
use mmids_core::workflows::{MediaNotificationContent, MediaType};
use std::collections::HashMap;
use std::iter;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, warn};

/// Creates an audio encoder that uses the gstreamer `avenc_aac` encoder to encode audio into aac.
///
/// This encoder supports the following optional parameters:
/// * `bitrate` - The average **bytes** per second to target.
pub struct AvencAacEncoderGenerator {}

impl AudioEncoderGenerator for AvencAacEncoderGenerator {
    fn create(
        &self,
        pipeline: &Pipeline,
        parameters: &HashMap<String, Option<String>>,
        media_sender: UnboundedSender<MediaNotificationContent>,
    ) -> Result<Box<dyn AudioEncoder>> {
        Ok(Box::new(AvencAacEncoder::new(
            media_sender,
            parameters,
            pipeline,
        )?))
    }
}

struct AvencAacEncoder {
    source: AppSrc,
}

impl AvencAacEncoder {
    fn new(
        media_sender: UnboundedSender<MediaNotificationContent>,
        parameters: &HashMap<String, Option<String>>,
        pipeline: &Pipeline,
    ) -> Result<AvencAacEncoder> {
        let bitrate = get_number(parameters, "bitrate");

        let appsrc = create_gst_element("appsrc")?;
        let queue = create_gst_element("queue")?;
        let decodebin = create_gst_element("decodebin")?;
        let convert = create_gst_element("audioconvert")?;
        let encoder = create_gst_element("avenc_aac")?;
        let output_parser = create_gst_element("aacparse")?;
        let appsink = create_gst_element("appsink")?;

        pipeline
            .add_many(&[
                &appsrc,
                &queue,
                &decodebin,
                &encoder,
                &output_parser,
                &appsink,
                &convert,
            ])
            .with_context(|| "Failed to add avenc_aac encoder's elements to the pipeline")?;

        Element::link_many(&[&appsrc, &queue, &decodebin])
            .with_context(|| "Failed to link appsrc -> queue -> decodebin for avenc_aac encoder")?;

        Element::link_many(&[&convert, &encoder, &output_parser, &appsink])
            .with_context(|| "Failed to link avenc_aac -> aacparse -> appsink")?;

        // decodebin's pad is added dynamically
        let link_destination = convert;
        decodebin.connect_pad_added(move |src, src_pad| {
            match src.link_pads(Some(&src_pad.name()), &link_destination.clone(), None) {
                Ok(_) => (),
                Err(_) => error!(
                    "Failed to link `decodebin`'s {} pad to the avenc_aac element",
                    src_pad.name()
                ),
            }
        });

        if let Some(bitrate) = bitrate {
            encoder.set_property("bitrate", bitrate);
        }

        let appsink = appsink
            .dynamic_cast::<AppSink>()
            .map_err(|_| anyhow!("appsink could not be cast to `AppSink`"))?;

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
            .map_err(|_| anyhow!("source element could not be cast to `AppSrc`"))?;

        Ok(AvencAacEncoder { source: appsrc })
    }
}

impl AudioEncoder for AvencAacEncoder {
    fn push_data(
        &self,
        payload_type: Arc<String>,
        data: Bytes,
        timestamp: Duration,
        is_sequence_header: bool,
    ) -> Result<()> {
        let buffer = set_gst_buffer(data, Some(timestamp), None)
            .with_context(|| "Failed to create aac buffer")?;

        if is_sequence_header {
            set_source_audio_sequence_header(&self.source, payload_type, buffer)
                .with_context(|| " Failed to set aac sequence header into pipeline")?;
        } else {
            self.source
                .push_buffer(buffer)
                .with_context(|| "Failed to push buffer into audio source")?;
        }

        Ok(())
    }
}

fn get_number(parameters: &HashMap<String, Option<String>>, key: &str) -> Option<i32> {
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
    metadata_buffer: &mut BytesMut,
) -> Result<()> {
    if !*codec_data_sent {
        // Pull the codec_data out of the output parser to get the sequence header
        let codec_data = get_codec_data_from_element(output_parser)?;
        let _ = media_sender.send(MediaNotificationContent::MediaPayload {
            payload_type: AUDIO_CODEC_AAC_RAW.clone(),
            media_type: MediaType::Audio,
            timestamp: Duration::from_millis(0),
            is_required_for_decoding: true,
            data: codec_data,
            metadata: MediaPayloadMetadataCollection::new(iter::empty(), metadata_buffer),
        });

        *codec_data_sent = true;
    }

    let sample = SampleResult::from_sink(sink).with_context(|| "Failed to get aac sample")?;

    if let Some(dts) = sample.dts {
        let _ = media_sender.send(MediaNotificationContent::MediaPayload {
            payload_type: AUDIO_CODEC_AAC_RAW.clone(),
            media_type: MediaType::Audio,
            timestamp: dts,
            is_required_for_decoding: false,
            data: sample.content,
            metadata: MediaPayloadMetadataCollection::new(iter::empty(), metadata_buffer),
        });

        Ok(())
    } else {
        Err(anyhow!(
            "No dts found for AAC sample, and thus timestamp is unknown!"
        ))
    }
}
