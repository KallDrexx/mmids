//! This crate contains all components for mmids applications to interact with with gstreamer
//! pipelines.

#[macro_use]
extern crate lazy_static;

pub mod encoders;
pub mod endpoints;
pub mod steps;
pub mod utils;

use gstreamer::glib;
use gstreamer::DebugLevel;
use tracing::{error, info, warn};

lazy_static! {
    /// Retrieves the result of initializing gstreamer.
    ///
    /// When this is first called, gstreamer will be initialized and logging handlers will be set
    /// up, so that gstreamer logs are written via normal `tracing` log handlers.  This also sets
    /// up default logging at the WARN level.
    ///
    /// All gstreamer initialization should be done by getting the result of this value instead of
    /// manual calls to `gstreamer::init()`.
    pub static ref GSTREAMER_INIT_RESULT: Result<(), glib::Error> = {
        match gstreamer::init() {
            Ok(_) => (),
            Err(error) => {
                error!("Failed to initialize gstreamer: {:?}", error);
                return Err(error);
            }
        }

        // Remove stdout debug logging
        gstreamer::debug_remove_default_log_function();

        // Add custom logging handler
        gstreamer::debug_add_log_function(|category, level, file, function, _line, object, message| {
            let message = message.get().map(|o| o.to_string()).unwrap_or_else(|| "".to_string());
            let object_name = object.map(|o| o.to_string()).unwrap_or_else(|| "<NO OBJECT>".to_string());

            match &level {
                DebugLevel::Error => error!(
                        category = %category.name(),
                        level = %level.name(),
                        file = %file,
                        function = %function,
                        object = %object_name,
                        message = %message,
                        "Gstreamer error ({}): {}", category.name(), message
                ),

                DebugLevel::Warning => warn!(
                        category = %category.name(),
                        level = %level.name(),
                        file = %file,
                        function = %function,
                        object = %object_name,
                        message = %message,
                        "Gstreamer warning ({}): {}", category.name(), message
                ),

                _ => info!(
                        category = %category.name(),
                        level = %level.name(),
                        file = %file,
                        function = %function,
                        object = %object_name,
                        message = %message,
                        "Gstreamer {} ({}): {}", level.name(), category.name(), message
                ),
            }
        });

        // By default log warning and above
        gstreamer::debug_set_default_threshold(DebugLevel::Warning);

        info!("Gstreamer successfully initialized");

        Ok(())
    };
}

/// Used to update the gstreamer default log level to info, which can be used for getting more
/// information about failures.  Should not be used in production due to the amount of logs that
/// will be raised, and it will not be easy to correlate gstreamer info logs to specific mmids
/// streams.
///
/// **Must** be called after the first invocation of `GSTREAMER_INIT_RESULT`, otherwise the default
/// log level will be overridden to warning.
pub fn set_gstreamer_log_level_to_info() {
    gstreamer::debug_set_default_threshold(DebugLevel::Info);
}
