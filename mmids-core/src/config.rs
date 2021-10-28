use crate::workflows::definitions::{WorkflowDefinition, WorkflowStepType, WorkflowStepDefinition};
use std::collections::HashMap;
use pest::iterators::Pair;
use pest::Parser;
use thiserror::Error;

pub struct MmidsConfig {
    pub settings: HashMap<String, Option<String>>,
    pub workflows: HashMap<String, WorkflowDefinition>,
}

#[derive(Error, Debug)]
pub enum ConfigParseError {
    #[error("The config provided could not be parsed")]
    InvalidConfig(#[from] pest::error::Error<Rule>),

    #[error("Found unexpected rule '{rule:?}' in the {section} section")]
    UnexpectedRule {rule: Rule, section: String},

    #[error("Duplicate workflow name: '{name}'")]
    DuplicateWorkflowName {name: String},
}

#[derive(Parser)]
#[grammar = "config.pest"]
struct RawConfigParser;

pub fn parse(content: &str) -> Result<MmidsConfig, ConfigParseError> {
    let mut config = MmidsConfig {
        settings: HashMap::new(),
        workflows: HashMap::new(),
    };

    let pairs = RawConfigParser::parse(Rule::content, content)?;
    for pair in pairs {
        let rule = pair.as_rule();
        match &rule {
            Rule::setting_block => handle_setting_block(&mut config, pair)?,
            Rule::workflow_block => handle_workflow_block(&mut config, pair)?,
            _ => (),
        }
    }

    Ok(config)
}

fn handle_setting_block(config: &mut MmidsConfig, pair: Pair<Rule>) -> Result<(), ConfigParseError> {
    let mut current_setting_name = None;
    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::setting_name => {
                if let Some(name) = current_setting_name {
                    // previous setting had no value, was a flag
                    config.settings.insert(name, None);
                    current_setting_name = None;
                }

                if pair.as_str().trim() != "" {
                    // TODO: figure out why grammar is treating an empty line as a name/value pair
                    current_setting_name = Some(pair.as_str().to_string());
                }
            }

            Rule::setting_value => {
                let raw_value = pair.as_str().to_string();
                let mut quoted_value = None;
                for pair in pair.into_inner() {
                    match pair.as_rule() {
                        Rule::quoted_string_value => quoted_value = Some(pair.as_str().to_string()),
                        _ => ()
                    }
                }

                if let Some(name) = current_setting_name {
                    let value = if let Some(value) = quoted_value {value} else {raw_value};
                    if value.trim() == "" {
                        config.settings.insert(name, None);
                    } else {
                        config.settings.insert(name, Some(value));
                    }

                    current_setting_name = None;
                }
            }

            _ => return Err(ConfigParseError::UnexpectedRule {rule: pair.as_rule(), section: "setting_block".to_string()})
        }
    }

    if let Some(name) = current_setting_name {
        config.settings.insert(name.to_string(), None);
    }

    Ok(())
}

fn handle_workflow_block(config: &mut MmidsConfig, pair: Pair<Rule>) -> Result<(), ConfigParseError> {
    let mut workflow = WorkflowDefinition {
        name: "".to_string(),
        steps: Vec::new(),
    };

    let mut current_step = None;

    for pair in pair.into_inner() {
        match pair.as_rule() {
            Rule::workflow_name => workflow.name = pair.as_str().to_string(),
            Rule::step_type => {
                if let Some(step) = current_step {
                    workflow.steps.push(step);
                }

                current_step = Some(WorkflowStepDefinition {
                    step_type: WorkflowStepType(pair.as_str().to_string()),
                    parameters: HashMap::new(),
                });
            }

            Rule::arguments => {
                let arguments  = pair.into_inner()
                    .filter(|p| p.as_rule() == Rule::argument)
                    .map(|p| p.into_inner())
                    .flatten();

                for argument in arguments {
                    match argument.as_rule() {
                        Rule::key_value_pair => {
                            let mut key = "".to_string();
                            let mut value = "".to_string();
                            for inner in argument.into_inner() {
                                match inner.as_rule() {
                                    Rule::key => key = inner.as_str().to_string(),
                                    Rule::value => {
                                        value = inner.as_str().to_string();
                                        for inner in inner.into_inner() {
                                            match inner.as_rule() {
                                                Rule::quoted_string_value => value = inner.as_str().to_string(),
                                                x => return Err(ConfigParseError::UnexpectedRule {
                                                    rule: x,
                                                    section: "key value pair value".to_string(),
                                                })
                                            }
                                        }
                                    },

                                    x => return Err(ConfigParseError::UnexpectedRule {
                                        rule: x,
                                        section: "key value pair".to_string(),
                                    })
                                }
                            }

                            current_step.as_mut().unwrap().parameters.insert(key, value);
                        }

                        Rule::quoted_string_value => {
                            current_step.as_mut().unwrap().parameters.insert(argument.as_str().to_string(), "".to_string());
                        }

                        Rule::argument_flag => {
                            current_step.as_mut().unwrap().parameters.insert(argument.as_str().to_string(), "".to_string());
                        }

                        x => return Err(ConfigParseError::UnexpectedRule {
                            rule: x,
                            section: "argument".to_string()
                        })
                    }
                }
            }

            x => return Err(ConfigParseError::UnexpectedRule {
                rule: x,
                section: "workflow block".to_string(),
            })
        }
    }

    if let Some(step) = current_step {
        workflow.steps.push(step);
    }

    if let Some(prev_workflow) = config.workflows.insert(workflow.name.clone(), workflow) {
        return Err(ConfigParseError::DuplicateWorkflowName {name: prev_workflow.name});
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_parse_settings() {
        let content = "
settings {
    first a
    second \"C:\\program files\\ffmpeg\\bin\\ffmpeg.exe\"
    flag

}
";

        let config = parse(content).unwrap();
        assert_eq!(config.settings.len(), 3, "Unexpected number of settings");
        assert_eq!(config.settings.get("first"), Some(&Some("a".to_string())), "Unexpected first value");
        assert_eq!(config.settings.get("second"), Some(&Some("C:\\program files\\ffmpeg\\bin\\ffmpeg.exe".to_string())), "Unexpected second value");
        assert_eq!(config.settings.get("flag"), Some(&None), "Unexpected flag value");
    }

    #[test]
    fn can_read_single_workflow() {
        let content = "
workflow name {
    rtmp_receive port=1935 app=receive stream_key=*
    hls path=c:\\temp\\test.m3u8 segment_size=\"3\" size=640x480 flag
}
";
        let config = parse(content).unwrap();
        assert_eq!(config.workflows.len(), 1, "Unexpected number of workflows");
        assert!(config.workflows.contains_key("name"), "workflow 'name' did not exist");

        let workflow = config.workflows.get("name").unwrap();
        assert_eq!(workflow.name, "name".to_string(), "Unexpected workflow name");
        assert_eq!(workflow.steps.len(), 2, "Unexpected number of workflow steps");

        let step1 = workflow.steps.get(0).unwrap();
        assert_eq!(step1.step_type.0, "rtmp_receive".to_string(), "Unexpected type of step 1");
        assert_eq!(step1.parameters.len(), 3, "Unexpected number of parameters");
        assert_eq!(step1.parameters.get("port"), Some(&"1935".to_string()), "Unexpected step 1 port value");
        assert_eq!(step1.parameters.get("app"), Some(&"receive".to_string()), "Unexpected step 1 app value");
        assert_eq!(step1.parameters.get("stream_key"), Some(&"*".to_string()), "Unexpected step 1 stream_key value");

        let step2 = workflow.steps.get(1).unwrap();
        assert_eq!(step2.step_type.0, "hls".to_string(), "Unexpected type of step 1");
        assert_eq!(step2.parameters.len(), 4, "Unexpected number of parameters");
        assert_eq!(step2.parameters.get("path"), Some(&"c:\\temp\\test.m3u8".to_string()), "Unexpected step 2 path value");
        assert_eq!(step2.parameters.get("segment_size"), Some(&"3".to_string()), "Unexpected step 2 segment_size value");
        assert_eq!(step2.parameters.get("size"), Some(&"640x480".to_string()), "Unexpected step 2 size value");
        assert_eq!(step2.parameters.get("flag"), Some(&"".to_string()), "Unexpected step 2 flag value");
    }

    #[test]
    fn can_read_multiple_workflows() {
        let content = "
workflow name {
    rtmp_receive port=1935 app=receive stream_key=*
    hls path=c:\\temp\\test.m3u8 segment_size=\"3\" size=640x480 flag
}

workflow name2 {
    another a
}
";
        let config = parse(content).unwrap();

        assert_eq!(config.workflows.len(), 2, "Unexpected number of workflows");
        assert!(config.workflows.contains_key("name"), "Could not find a workflow named 'name'");
        assert!(config.workflows.contains_key("name2"), "Could not find a workflow named 'name2'");
    }

    #[test]
    fn duplicate_workflow_name_returns_error() {
        let content = "
workflow name {
    rtmp_receive port=1935 app=receive stream_key=*
    hls path=c:\\temp\\test.m3u8 segment_size=\"3\" size=640x480 flag
}

workflow name {
    another a
}
";
        match parse(content) {
            Err(ConfigParseError::DuplicateWorkflowName {name }) => {
                if name != "name".to_string() {
                    panic!("Unexpected name in workflow: '{}'", name);
                }
            }
            Err(e) => panic!("Expected duplicate workflow name error, instead got: {:?}", e),
            Ok(_) => panic!("Received successful parse, but an error was expected"),
        }
    }

    #[test]
    fn full_config_can_be_parsed() {
        let content = "
# comment
settings {
    first a # another comment
    second \"C:\\program files\\ffmpeg\\bin\\ffmpeg.exe\"
    flag

}

workflow name { #workflow comment
    rtmp_receive port=1935 app=receive stream_key=* #step comment
    hls path=c:\\temp\\test.m3u8 segment_size=\"3\" size=640x480 flag
}

workflow name2 {
    another a
}
";
        parse(content).unwrap();
    }
}