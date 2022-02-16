use std::collections::HashMap;
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use gstreamer::{Buffer, Caps, Element, ElementFactory, FlowError, FlowSuccess, Fraction, Pipeline};
use gstreamer::prelude::*;
use gstreamer_app::{AppSink, AppSinkCallbacks, AppSrc};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{error, warn};
use mmids_core::codecs::VideoCodec;
use mmids_core::VideoTimestamp;
use mmids_core::workflows::MediaNotificationContent;
use crate::encoders::{SampleResult, VideoEncoder};

struct X264Encoder {
    source: AppSrc,
}

impl X264Encoder {
    fn new(
        media_sender: UnboundedSender<MediaNotificationContent>,
        parameters: HashMap<String, Option<String>>,
        pipeline: &Pipeline,
    ) -> Result<X264Encoder> {
        let height = get_number(&parameters, "video_height");
        let width = get_number(&parameters, "video_width");
        let preset = parameters.get("video_preset") .unwrap_or(&None);
        let fps = get_number(&parameters, "video_fps");

        let appsrc = create_element("appsrc")?;
        let queue = create_element("queue")?;
        let decoder = create_element("decodebin")?;
        let scale = create_element("videoscale")?;
        let rate_changer = create_element("videorate")?;
        let capsfilter = create_element("capsfilter")?;
        let encoder = create_element("x264enc")?;
        let output_parser = create_element("h264parse")?;
        let appsink = create_element("appsink")?;

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
                &appsink
            ])
            .with_context(|| "Failed to add elements to pipeline")?;

        Element::link_many(&[&appsrc, &queue, &decoder])
            .with_context(|| "Failed to link appsrc -> queue -> decoder")?;

        Element::link_many(&[&scale, &rate_changer, &capsfilter, &encoder, &output_parser, &appsink])
            .with_context(|| "Failed to link scale to sink")?;

        // decodebin's video pad is added dynamically
        decoder.connect_pad_added(move |src, src_pad| {
            match src.link_pads(Some(&src_pad.name()), &scale.clone(), None) {
                Ok(_) => (),
                Err(_) => error!("Failed to link `decodebin`'s {} pad to scaler element", src_pad.name()),
            }
        });

        let mut caps = Caps::builder("video/x-raw");
        if let Some(height) = height {
            caps = caps.field("height", height);
        }

        if let Some(width) = width {
            caps = caps.field("width", width);
        }

        if let Some(fps) = fps {
            caps = caps.field("framerate", Fraction::new(fps as i32, 1));
        }

        capsfilter.set_property("caps", caps.build());
        encoder.set_property_from_str("tune", "zerolatency");

        if let Some(preset) = preset {
            encoder.set_property_from_str("speed-preset", preset.as_str());
        }

        let cloned_pipeline = pipeline.clone();
        let appsink = appsink.dynamic_cast::<AppSink>()
            .or_else(|_| Err(anyhow::anyhow!("appsink could not be cast to 'AppSink'")))?;

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
                .build(),
        );

        let appsrc = appsrc.dynamic_cast::<AppSrc>()
            .or_else(|_| Err(anyhow::anyhow!("source element could not be cast to 'Appsrc'")))?;

        Ok(X264Encoder {
            source: appsrc,
        })
    }
}

impl VideoEncoder for X264Encoder {
    fn push_data(&mut self, data: Bytes, timestamp: VideoTimestamp, is_sequence_header: bool) -> anyhow::Result<()> {
        todo!()
    }
}

fn create_element(name: &str) -> Result<Element> {
    ElementFactory::make(name, None)
        .with_context(|| format!("Failed to create element '{}'", name))
}

fn get_number(parameters: &HashMap<String, Option<String>>, key: &str) -> Option<u32> {
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
        // Pull the codec_data/sequence header out from the output parser
        let pad = output_parser.static_pad("src")
            .with_context(|| "Failed to get src pad of the output parser")?;

        let caps = pad.caps()
            .with_context(|| "No caps on output parser pad")?;

        let structure = caps.structure(0)
            .with_context(|| "output parser caps had no structure")?;

        let codec_data = structure.get::<Buffer>("codec_data")
            .with_context(|| "output parser had no codec data in its caps")?;

        let map = codec_data.map_readable()
            .with_context(|| "Could no make codec data buffer readable")?;

        let _ = media_sender.send(MediaNotificationContent::Video {
            codec: VideoCodec::H264,
            timestamp: VideoTimestamp::from_zero(),
            is_sequence_header: true,
            is_keyframe: false,
            data: Bytes::copy_from_slice(map.as_slice()),
        });

        *codec_data_sent = true;
    }

    let sample = SampleResult::from_sink(sink)
        .with_context(|| "Failed to get x264enc sample")?;

    let _ = media_sender.send(MediaNotificationContent::Video {
        codec: VideoCodec::H264,
        timestamp: sample.timestamp,
        is_sequence_header: false,
        is_keyframe: false, // TODO, figure out how to compute this
        data: sample.content,
    });

    Ok(())
}