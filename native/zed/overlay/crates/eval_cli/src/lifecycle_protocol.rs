use std::collections::BTreeMap;
use std::io::{BufRead, Write};

use serde::Deserialize;
use serde_json::{Value, json};

pub const MAX_FRAME: usize = 64 * 1024;
pub const MAX_RESPONSE: usize = 1024 * 1024;
pub const CACHE_LIMIT: usize = 128;
pub const QUEUE_LIMIT: usize = 32;
pub const MODEL: &str = "gpt-6.1-sol";
pub const PROVIDER: &str = "openai-subscribed";
pub const PROFILE: &str = "lifecycle-proof";

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub v: u8,
    pub id: String,
    pub op: Operation,
    pub text: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Status,
    Prompt,
    Interrupt,
    Checkpoint,
    Shutdown,
}

pub fn parse(frame: &[u8]) -> Result<Request, &'static str> {
    if frame.len() > MAX_FRAME {
        return Err("frame_too_large");
    }
    let request: Request = serde_json::from_slice(frame).map_err(|_| "invalid_request")?;
    if request.v != 1 || request.id.is_empty() || request.id.len() > 256 {
        return Err("invalid_request");
    }
    match request.op {
        Operation::Prompt => {
            let text = request.text.as_deref().ok_or("invalid_prompt")?.trim();
            if text.is_empty() || text.starts_with('/') {
                return Err("invalid_prompt");
            }
        }
        _ if request.text.is_some() => return Err("unexpected_text"),
        _ => {}
    }
    Ok(request)
}

pub fn read_frame(reader: &mut impl BufRead) -> Result<Option<Vec<u8>>, &'static str> {
    let mut frame = Vec::new();
    let started = std::time::Instant::now();
    loop {
        if started.elapsed() >= std::time::Duration::from_secs(2) {
            return Err("read_timeout");
        }
        let buffer = reader.fill_buf().map_err(|_| "read_failed")?;
        if buffer.is_empty() {
            return if frame.is_empty() {
                Ok(None)
            } else {
                Err("incomplete_frame")
            };
        }
        let end = buffer.iter().position(|byte| *byte == b'\n');
        let count = end.map_or(buffer.len(), |index| index + 1);
        if frame.len() + count > MAX_FRAME {
            return Err("frame_too_large");
        }
        frame.extend_from_slice(&buffer[..count]);
        reader.consume(count);
        if end.is_some() {
            return Ok(Some(frame));
        }
    }
}

pub fn success(id: &str, result: Value) -> Value {
    json!({"v":1, "id":id, "ok":true, "result":result})
}
pub fn failure(id: &str, code: &str) -> Value {
    json!({"v":1, "id":id, "ok":false, "error":{"code":code}})
}
pub fn write_response(writer: &mut impl Write, response: &Value) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(response)?;
    if bytes.len() > MAX_RESPONSE {
        return Err(std::io::Error::other("response exceeds limit"));
    }
    writer.write_all(&bytes)?;
    writer.write_all(b"\n")?;
    writer.flush()
}

#[derive(Default)]
pub struct Control {
    pub sequence: u64,
    pub turn: u64,
    pub managed_epoch: Option<String>,
    pub running: bool,
    pub active_control: bool,
    pub frozen: bool,
    pub held: bool,
    pub interrupted: bool,
    pub policy_violation: bool,
    pub refused: bool,
    pub last_result: Option<&'static str>,
    pub last_control_error: Option<&'static str>,
    prompts: BTreeMap<String, (Request, Value)>,
}

impl Control {
    pub fn state(&self) -> &'static str {
        if self.held {
            "held"
        } else if self.frozen {
            "shutting_down"
        } else if self.active_control {
            "controlling"
        } else if self.running {
            "running"
        } else {
            "idle"
        }
    }

    pub fn admit(&mut self, request: &Request) -> Result<Value, &'static str> {
        if let Some((original, response)) = self.prompts.get(&request.id) {
            return if original == request {
                Ok(response.clone())
            } else {
                Err("id_conflict")
            };
        }
        if self.held || self.frozen || self.policy_violation {
            return Err("admission_closed");
        }
        if self.running || self.active_control {
            return Err("busy");
        }
        if self.prompts.len() == CACHE_LIMIT {
            return Err("prompt_cache_full");
        }
        self.turn += 1;
        self.sequence += 1;
        self.running = true;
        self.interrupted = false;
        self.refused = false;
        let response = success(
            &request.id,
            json!({"admitted":true,"turn_id":self.turn_value()}),
        );
        self.prompts
            .insert(request.id.clone(), (request.clone(), response.clone()));
        Ok(response)
    }

    pub fn turn_value(&self) -> Value {
        match &self.managed_epoch {
            Some(epoch) => json!(format!("{epoch}:{}", self.turn)),
            None => json!(self.turn),
        }
    }

    pub fn is_cached(&self, id: &str) -> bool {
        self.prompts.contains_key(id)
    }

    pub fn check_control(&self, request: &Request) -> Result<(), &'static str> {
        if self.prompts.contains_key(&request.id) {
            return Err("id_conflict");
        }
        if self.held {
            return Err("termination_unknown");
        }
        if self.active_control {
            return Err("busy");
        }
        if self.frozen && request.op != Operation::Shutdown {
            return Err("admission_closed");
        }
        if request.op == Operation::Checkpoint && self.running {
            return Err("busy");
        }
        Ok(())
    }

    pub fn finish_turn(&mut self, result: &'static str) {
        self.running = false;
        self.last_result = Some(if self.policy_violation {
            "policy_violation"
        } else if self.interrupted {
            "interrupted"
        } else if self.refused {
            "refused"
        } else {
            result
        });
        self.sequence += 1;
    }

    pub fn finish_control(&mut self, saved: bool) {
        self.active_control = saved && self.frozen;
        self.sequence += 1;
        self.last_control_error = (!saved).then_some("save_failed");
    }
}

pub fn guard_session(
    provider: &str,
    model: &str,
    profile: &str,
    standard_speed: bool,
) -> Result<(), &'static str> {
    if provider != PROVIDER || model != MODEL {
        return Err("model_mismatch");
    }
    if profile != PROFILE {
        return Err("profile_mismatch");
    }
    if !standard_speed {
        return Err("speed_mismatch");
    }
    Ok(())
}

pub fn policy_settings() -> Value {
    let model = json!({"provider":PROVIDER,"model":MODEL,"speed":"standard"});
    // This global switch prevents Project::local from starting workspace MCP servers.
    // Explicit NativeAgent submissions do not use the editor's AI enablement switch.
    json!({"disable_ai":true,"agent":{
        "default_model":model,"thread_summary_model":model,"compaction_model":model,
        "default_profile":PROFILE,"auto_compact":{"enabled":false},
        "profiles":{(PROFILE):{"name":"Lifecycle proof","tools":{},
            "enable_all_context_servers":false,"context_servers":{}}},
        "tool_permissions":{"default":"deny"},"prevent_idle_sleep":false
    },"context_servers":{},"autosave":"off","format_on_save":"off"})
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    fn prompt(id: &str) -> Request {
        Request {
            v: 1,
            id: id.into(),
            op: Operation::Prompt,
            text: Some("hello".into()),
        }
    }
    #[test]
    fn strict_frames() {
        for input in [
            r#"{}"#,
            r#"{"v":2,"id":"x","op":"status"}"#,
            r#"{"v":1,"id":"x","op":"other"}"#,
            r#"{"v":1,"id":"x","op":"status","extra":1}"#,
            r#"{"v":1,"id":"x","op":"status","text":"secret"}"#,
            r#"{"v":1,"id":"x","op":"prompt","text":" /compact"}"#,
        ] {
            assert!(parse(input.as_bytes()).is_err(), "{input}");
        }
        assert_eq!(
            parse(&vec![b'x'; MAX_FRAME + 1]).unwrap_err(),
            "frame_too_large"
        );
        assert_eq!(
            read_frame(&mut Cursor::new(vec![b'x'; MAX_FRAME + 1])).unwrap_err(),
            "frame_too_large"
        );
        assert_eq!(
            read_frame(&mut Cursor::new(b"{}")).unwrap_err(),
            "incomplete_frame"
        );
        assert_eq!(
            parse(br#"{"v":1,"id":"s","op":"status"}"#).unwrap().op,
            Operation::Status
        );
    }
    #[test]
    fn duplicate_busy_and_changed_payload() {
        let mut state = Control::default();
        let first = prompt("a");
        assert_eq!(
            state.admit(&first).unwrap(),
            json!({"v":1,"id":"a","ok":true,"result":{"admitted":true,"turn_id":1}})
        );
        assert_eq!(state.admit(&first).unwrap()["result"]["turn_id"], 1);
        assert_eq!(state.admit(&prompt("b")).unwrap_err(), "busy");
        let mut changed = first.clone();
        changed.text = Some("different".into());
        assert_eq!(state.admit(&changed).unwrap_err(), "id_conflict");
        state.finish_turn("completed");
        assert_eq!(state.admit(&first).unwrap()["result"]["turn_id"], 1);
        assert_eq!(state.turn, 1);
    }
    #[test]
    fn full_cache_keeps_controls_available() {
        let mut state = Control::default();
        for index in 0..CACHE_LIMIT {
            state.admit(&prompt(&index.to_string())).unwrap();
            state.finish_turn("completed");
        }
        assert_eq!(
            state.admit(&prompt("overflow")).unwrap_err(),
            "prompt_cache_full"
        );
        for op in [
            Operation::Interrupt,
            Operation::Checkpoint,
            Operation::Shutdown,
        ] {
            assert!(
                state
                    .check_control(&Request {
                        v: 1,
                        id: "control".into(),
                        op,
                        text: None
                    })
                    .is_ok()
            );
        }
        assert_eq!(state.state(), "idle");
        assert_eq!(state.prompts.len(), 128);
    }
    #[test]
    fn cancellation_save_failure_and_unknown_are_not_success() {
        let mut state = Control::default();
        state.admit(&prompt("a")).unwrap();
        state.interrupted = true;
        state.finish_turn("completed");
        assert_eq!(state.last_result, Some("interrupted"));
        state.frozen = true;
        state.finish_control(false);
        assert_eq!(state.last_result, Some("interrupted"));
        assert_eq!(state.last_control_error, Some("save_failed"));
        assert_eq!(state.admit(&prompt("b")).unwrap_err(), "admission_closed");
        state.held = true;
        assert_eq!(
            state
                .check_control(&Request {
                    v: 1,
                    id: "q".into(),
                    op: Operation::Shutdown,
                    text: None
                })
                .unwrap_err(),
            "termination_unknown"
        );
        assert_eq!(
            failure("q", "save_failed"),
            json!({"v":1,"id":"q","ok":false,"error":{"code":"save_failed"}})
        );
    }
    #[test]
    fn loaded_model_profile_and_effective_speed_guard() {
        assert!(guard_session(PROVIDER, MODEL, PROFILE, true).is_ok());
        for (provider, model, profile, standard, error) in [
            ("openai", MODEL, PROFILE, true, "model_mismatch"),
            (PROVIDER, "other", PROFILE, true, "model_mismatch"),
            (PROVIDER, MODEL, "code", true, "profile_mismatch"),
            (PROVIDER, MODEL, PROFILE, false, "speed_mismatch"),
        ] {
            assert_eq!(
                guard_session(provider, model, profile, standard).unwrap_err(),
                error
            );
        }
    }
    #[test]
    fn native_policy_disables_tools_and_pins_all_models() {
        let settings = policy_settings();
        let agent = &settings["agent"];
        for name in ["default_model", "thread_summary_model", "compaction_model"] {
            assert_eq!(
                agent[name],
                json!({"provider":"openai-subscribed","model":"gpt-6.1-sol","speed":"standard"})
            );
        }
        assert_eq!(agent["profiles"][PROFILE]["tools"], json!({}));
        assert_eq!(agent["profiles"][PROFILE]["context_servers"], json!({}));
        assert_eq!(
            agent["profiles"][PROFILE]["enable_all_context_servers"],
            false
        );
        assert_eq!(agent["auto_compact"]["enabled"], false);
        assert_eq!(settings["context_servers"], json!({}));
        assert_eq!(settings["disable_ai"], true);
    }
    #[test]
    fn framing_is_jsonl_and_bounded() {
        let response = success("s", json!({"state":"running"}));
        let mut bytes = Vec::new();
        write_response(&mut bytes, &response).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), response);
        assert!(
            write_response(
                &mut Vec::new(),
                &success("s", json!("x".repeat(MAX_RESPONSE)))
            )
            .is_err()
        );
    }
}

pub fn managed_policy_settings() -> Value {
    let mut settings = policy_settings();
    settings["agent"]["default_profile"] = json!(super::managed_tools::PROFILE);
    settings["agent"]["profiles"] = json!({(super::managed_tools::PROFILE):{
        "name":"Factory managed v1","tools":{"managed_files":true,"managed_terminal":true,"factory_helper":true},
        "enable_all_context_servers":false,"context_servers":{}
    }});
    settings
}
