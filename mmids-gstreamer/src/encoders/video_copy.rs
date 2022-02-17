use gstreamer::{Element, FlowSuccess, Pipeline};
use gstreamer_app::{AppSink, AppSinkCallbacks, AppSrc};
use tokio::sync::mpsc::UnboundedSender;
use mmids_core::workflows::MediaNotificationContent;
use anyhow::{Context, Result};
use gstreamer::prelude::*;
use crate::utils::{create_gst_element, get_codec_data_from_element};

struct VideoCopyEncoder {
    source: AppSrc,

}

impl VideoCopyEncoder {
    fn new(
        media_sender: UnboundedSender<MediaNotificationContent>,
        pipeline: &Pipeline,
    ) -> Result<VideoCopyEncoder> {

        // While we won't be mutating the stream, we want to pass it through a gstreamer pipeline
        // so the packets will be synchronized with audio in case of transcoding delay.

        let appsrc =create_gst_element("appsrc")?;
        let queue = create_gst_element("queue")?;
        let appsink = create_gst_element("appsink")?;

        pipeline.add_many(&[&appsrc, &queue, &appsink])
            .with_context(|| "Failed to add video copy encoder's elements to the pipeline")?;

        Element::link_many(&[&appsrc, &queue, &appsink])
            .with_context(|| "Failed to link video copy encoder's elements together")?;

        let appsink = appsink.dynamic_cast::<AppSink>()
            .with_context(|| "Video copy encoder's appsink could not be casted")?;

        let mut sent_codec_data = false;
        let cloned_appsrc = appsrc.clone();
        appsink.set_callbacks(
            AppSinkCallbacks::builder()
                .new_sample(move |sink| {
                    let codec_data = get_codec_data_from_element(&cloned_appsrc)?;

                    let _ = media_sender.send()

                    Ok(FlowSuccess::Ok)
                })
            .build(),
        );

        let appsrc = appsrc.dynamic_cast::<AppSrc>()
            .with_context(|| "Video copy encoder's appsrc could not be casted")?;

        Ok(VideoCopyEncoder{
            source: appsrc,
        })
    }
}