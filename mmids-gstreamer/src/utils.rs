use std::time::Duration;
use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use gstreamer::{Buffer, BufferMap, Caps, ClockTime, Element, ElementFactory};
use gstreamer::buffer::Readable;
use gstreamer::prelude::*;
use gstreamer_app::AppSrc;
use mmids_core::codecs::VideoCodec;

pub fn set_gst_buffer(
    data: Bytes,
    dts: Option<Duration>,
    pts: Option<Duration>,
) -> Result<Buffer> {
    let mut buffer = Buffer::with_size(data.len())
        .with_context(|| format!("Could not create a buffer with size {}", data.len()))?;

    {
        let buffer = buffer
            .get_mut()
            .with_context(|| "Could not get mutable buffer")?;

        if let Some(dts) = dts {
            buffer.set_dts(ClockTime::from_mseconds(dts.as_millis() as u64));
        }

        if let Some(pts) = pts {
            buffer.set_pts(ClockTime::from_mseconds(pts.as_millis() as u64));
        }

        let mut sample = buffer
            .map_writable()
            .with_context(|| "Failed to map buffer to writable buffer map")?;

        {
            let sample = sample.as_mut_slice();
            sample.copy_from_slice(&data);
        }
    }

    Ok(buffer)
}

pub fn set_source_video_sequence_header(
    source: &AppSrc,
    codec: VideoCodec,
    buffer: Buffer,
) -> Result<()> {
    match codec {
        VideoCodec::H264 => {
            let caps = Caps::builder("video/x-h264")
                .field("codec_data", buffer)
                .build();

            source.set_caps(Some(&caps));

            Ok(())
        }

        VideoCodec::Unknown => {
            Err(anyhow!(
                "Video codec is not known, and thus we can't prepare the gstreamer pipeline to \
                accept it."
            ))
        }
    }
}

pub fn create_gst_element(name: &str) -> Result<Element> {
    ElementFactory::make(name, None)
        .with_context(|| format!("Failed to create element '{}'", name))
}

pub fn get_codec_data_from_element(element: &Element) -> Result<BufferMap<Readable>> {
    let pad = element.static_pad("src")
        .with_context(|| format!("Failed to get src pad of the {} element", element.name()))?;

    let caps = pad.caps()
        .with_context(|| format!("No caps on src pad of the {} element", element.name()))?;

    let structure = caps.structure(0)
        .with_context(|| format!("No structure on the pad of the {} element", element.name()))?;

    let codec_data = structure.get::<Buffer>("codec_data")
        .with_context(|| format!("The src pad of the {} element did not have a 'codec_data' field", element.name()))?;

    let map = codec_data
        .map_readable()
        .with_context(|| format!("Element {}'s codec data's buffer could not be made readable", element.name()))?;

    Ok(map)
}
