use std::collections::HashMap;
use std::time::Duration;
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use gstreamer::{Element, FlowError, FlowSuccess, Pipeline};
use gstreamer::prelude::*;
use gstreamer_app::{AppSink, AppSinkCallbacks, AppSrc};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, warn};
use mmids_core::codecs::AudioCodec;
use mmids_core::workflows::MediaNotificationContent;
use crate::encoders::{AudioEncoder, AudioEncoderGenerator, SampleResult};
use crate::utils::{create_gst_element, get_codec_data_from_element, set_gst_buffer, set_source_audio_sequence_header};

/// Creates an audio encoder that uses the gstreamer `faac` encoder to encode audio into aac.
///
/// This encoder supports the following optional parameters:
/// * `bitrate` - The average **bytes** per second to target.  Default is 128,000 bps.
pub struct FaacEncoderGenerator {}

impl AudioEncoderGenerator for FaacEncoderGenerator {
    fn create(
        &self,
        pipeline: &Pipeline,
        parameters: &HashMap<String, Option<String>>,
        media_sender: UnboundedSender<MediaNotificationContent>,
    ) -> Result<Box<dyn AudioEncoder>> {
        Ok(Box::new(FaacEncoder::new(media_sender, parameters, pipeline)?))
    }
}

struct FaacEncoder {
    source: AppSrc,
}

impl FaacEncoder {
    fn new(
        media_sender: UnboundedSender<MediaNotificationContent>,
        parameters: &HashMap<String, Option<String>>,
        pipeline: &Pipeline,
    ) -> Result<FaacEncoder> {
        let bitrate = get_number(parameters, "bitrate");

        let appsrc = create_gst_element("appsrc")?;
        let queue = create_gst_element("queue")?;
        let decodebin = create_gst_element("decodebin")?;
        let encoder = create_gst_element("faac")?;
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
            ])
            .with_context(|| "Failed to add Faac encoder's elements to the pipeline")?;

        Element::link_many(&[&appsrc, &queue, &decodebin])
            .with_context(|| "Failed to link appsrc -> queue -> decodebin for faac encoder")?;

        Element::link_many(&[&encoder, &output_parser, &appsink])
            .with_context(|| "Failed to link faac -> aacparse -> appsink")?;

        // decodebin's pad is added dynamically
        let link_destination = encoder.clone();
        decodebin.connect_pad_added(move |src, src_pad| {
            match src.link_pads(
                Some(&src_pad.name()),
                &link_destination.clone(),
                None,
            ) {
                Ok(_) => (),
                Err(_) => error!("Failed to link `decodebin`'s {} pad to the faac element", src_pad.name()),
            }
        });

        if let Some(bitrate) = bitrate {
            encoder.set_property("bitrate", bitrate);
        }

        let appsink = appsink
            .dynamic_cast::<AppSink>()
            .or_else(|_| Err(anyhow!("appsink could not be cast to `AppSink`")))?;

        let mut sent_codec_data = false;
        appsink.set_callbacks(
            AppSinkCallbacks::builder()
                .new_sample(move |sink| {
                    match sample_received(sink, &mut sent_codec_data, &output_parser, media_sender.clone()) {
                        Ok(_) => Ok(FlowSuccess::Ok),
                        Err(error) => {
                            error!("new_sample callback error received: {:?}", error);
                            Err(FlowError::Error)
                        }
                    }
                })
                .build()
        );

        let appsrc = appsrc
            .dynamic_cast::<AppSrc>()
            .or_else(|_| Err(anyhow!("source element could not be cast to `AppSrc`")))?;

        Ok(FaacEncoder { source: appsrc})
    }
}

impl AudioEncoder for FaacEncoder {
    fn push_data(
        &self,
        codec: AudioCodec,
        data: Bytes,
        timestamp: Duration,
        is_sequence_header: bool,
    ) -> Result<()> {
        let buffer = set_gst_buffer(data, Some(timestamp), None)
            .with_context(|| "Failed to create aac buffer")?;

        if is_sequence_header {
            set_source_audio_sequence_header(&self.source, codec, buffer)
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
    if let Some(outer) = parameters.get(key) {
        if let Some(inner) = outer {
            match inner.parse() {
                Ok(num) => return Some(num),
                Err(_) => warn!("Parameter {key} had a value of '{inner}', which is not a number"),
            }
        }
    }

    None
}

fn sample_received(
    sink: &AppSink,
    codec_data_sent: &mut bool,
    output_parser: &Element,
    media_sender: UnboundedSender<MediaNotificationContent>,
) -> Result<()> {
    if !*codec_data_sent {
        // Pull the codec_data out of the output parser to get the sequence header
        let codec_data = get_codec_data_from_element(&output_parser)?;
        let _ = media_sender.send(MediaNotificationContent::Audio {
            codec: AudioCodec::Aac,
            timestamp: Duration::from_millis(0),
            is_sequence_header: true,
            data: codec_data,
        });

        *codec_data_sent = true;
    }

    let sample = SampleResult::from_sink(sink)
        .with_context(|| "Failed to get aac sample")?;

    if let Some(dts) = sample.dts {
        let _ = media_sender.send(MediaNotificationContent::Audio {
            codec: AudioCodec::Aac,
            timestamp: dts,
            is_sequence_header: false,
            data: sample.content,
        });

        Ok(())
    } else {
        Err(anyhow!("No dts found for AAC sample, and thus timestamp is unknown!"))
    }
}