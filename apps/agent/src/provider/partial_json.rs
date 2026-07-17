use serde_json::{Map, Value};

const VALID_ESCAPES: &[char] = &['"', '\\', '/', 'b', 'f', 'n', 'r', 't', 'u'];

pub fn parse_streaming(input: &str) -> Value {
    if input.trim().is_empty() {
        return empty_object();
    }

    if let Ok(value) = serde_json::from_str(input) {
        return value;
    }

    let repaired = repair_json(input);
    if repaired != input
        && let Ok(value) = serde_json::from_str(&repaired)
    {
        return value;
    }

    if repaired == input
        && let Some(value) = parse_partial(input)
    {
        return value;
    }

    if repaired != input
        && let Some(value) = parse_partial(&repaired)
    {
        return value;
    }

    empty_object()
}

pub fn parse_final(input: &str) -> Result<Value, String> {
    match serde_json::from_str(input) {
        Ok(value) => Ok(value),
        Err(original_error) => {
            let repaired = repair_json(input);
            if repaired == input {
                return Err(original_error.to_string());
            }

            serde_json::from_str(&repaired).map_err(|repaired_error| {
                format!("invalid JSON ({original_error}); repair also failed ({repaired_error})")
            })
        }
    }
}

fn empty_object() -> Value {
    Value::Object(Map::new())
}

fn repair_json(input: &str) -> String {
    let chars: Vec<char> = input.chars().collect();
    let mut repaired = String::with_capacity(input.len());
    let mut in_string = false;
    let mut index = 0;

    while index < chars.len() {
        let character = chars[index];

        if !in_string {
            repaired.push(character);
            if character == '"' {
                in_string = true;
            }
            index += 1;
            continue;
        }

        if character == '"' {
            repaired.push(character);
            in_string = false;
            index += 1;
            continue;
        }

        if character == '\\' {
            let Some(next) = chars.get(index + 1).copied() else {
                repaired.push_str("\\\\");
                index += 1;
                continue;
            };

            if next == 'u'
                && chars
                    .get(index + 2..index + 6)
                    .is_some_and(|digits| digits.iter().all(char::is_ascii_hexdigit))
            {
                repaired.extend(chars[index..index + 6].iter());
                index += 6;
                continue;
            }

            if VALID_ESCAPES.contains(&next) {
                repaired.push('\\');
                repaired.push(next);
                index += 2;
                continue;
            }

            repaired.push_str("\\\\");
            index += 1;
            continue;
        }

        if character.is_control() && (character as u32) <= 0x1f {
            push_escaped_control(&mut repaired, character);
        } else {
            repaired.push(character);
        }
        index += 1;
    }

    repaired
}

fn push_escaped_control(output: &mut String, character: char) {
    match character {
        '\u{0008}' => output.push_str("\\b"),
        '\u{000c}' => output.push_str("\\f"),
        '\n' => output.push_str("\\n"),
        '\r' => output.push_str("\\r"),
        '\t' => output.push_str("\\t"),
        _ => output.push_str(&format!("\\u{:04x}", character as u32)),
    }
}

fn parse_partial(input: &str) -> Option<Value> {
    let mut boundaries: Vec<usize> = input.char_indices().map(|(index, _)| index).collect();
    boundaries.push(input.len());

    for end in boundaries.into_iter().rev() {
        let prefix = input[..end].trim_end();
        if let Some(completed) = close_open_structures(prefix)
            && let Ok(value) = serde_json::from_str(&completed)
        {
            return Some(value);
        }
    }

    None
}

fn close_open_structures(prefix: &str) -> Option<String> {
    let mut stack = Vec::new();
    let mut in_string = false;
    let mut escaped = false;

    for character in prefix.chars() {
        if in_string {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
            }
            continue;
        }

        match character {
            '"' => in_string = true,
            '{' | '[' => stack.push(character),
            '}' if stack.pop() != Some('{') => return None,
            ']' if stack.pop() != Some('[') => return None,
            '}' | ']' => {}
            _ => {}
        }
    }

    let mut completed = prefix.to_owned();
    if in_string {
        if escaped {
            completed.push('\\');
        }
        completed.push('"');
    }
    for opening in stack.into_iter().rev() {
        completed.push(if opening == '{' { '}' } else { ']' });
    }
    Some(completed)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_complete_json_values() {
        let cases = [
            (r#"{"name":"Sumi"}"#, json!({"name": "Sumi"})),
            (r#"[1,true,null]"#, json!([1, true, null])),
            (r#""text""#, json!("text")),
            ("42", json!(42)),
            ("null", Value::Null),
        ];

        for (input, expected) in cases {
            assert_eq!(parse_streaming(input), expected, "{input}");
            assert_eq!(parse_final(input), Ok(expected), "{input}");
        }
    }

    #[test]
    fn repairs_control_characters_inside_strings() {
        let cases = [
            (
                "{\"value\":\"line\nbreak\"}",
                json!({"value": "line\nbreak"}),
            ),
            ("{\"value\":\"tab\there\"}", json!({"value": "tab\there"})),
            (
                "{\"value\":\"carriage\rreturn\"}",
                json!({"value": "carriage\rreturn"}),
            ),
            (
                "{\"value\":\"backspace\u{0008}here\"}",
                json!({"value": "backspace\u{0008}here"}),
            ),
            (
                "{\"value\":\"formfeed\u{000c}here\"}",
                json!({"value": "formfeed\u{000c}here"}),
            ),
            (
                "{\"value\":\"nul\u{0000}here\"}",
                json!({"value": "nul\u{0000}here"}),
            ),
        ];

        for (input, expected) in cases {
            assert_eq!(parse_final(input), Ok(expected), "{input:?}");
        }
    }

    #[test]
    fn repairs_invalid_escape_sequences() {
        let cases = [
            (r#"{"path":"a\q"}"#, json!({"path": r"a\q"})),
            (r#"{"path":"a\v"}"#, json!({"path": r"a\v"})),
            (r#"{"path":"a\."}"#, json!({"path": r"a\."})),
            (r#"{"value":"\x12"}"#, json!({"value": r"\x12"})),
        ];

        for (input, expected) in cases {
            assert_eq!(parse_final(input), Ok(expected), "{input}");
        }
        assert!(parse_final(r#"{"value":"\u12"}"#).is_err());
    }

    #[test]
    fn preserves_valid_escape_sequences() {
        let input = r#"{"quote":"\"","slash":"\\","line":"\n","unicode":"\u65e5"}"#;
        assert_eq!(
            parse_final(input),
            Ok(json!({
                "quote": "\"",
                "slash": "\\",
                "line": "\n",
                "unicode": "日"
            }))
        );
    }

    #[test]
    fn completes_partial_objects_and_arrays() {
        let cases = [
            ("{", json!({})),
            ("[", json!([])),
            (r#"{"name":"Sumi"#, json!({"name": "Sumi"})),
            (r#"{"outer":{"inner":1"#, json!({"outer": {"inner": 1}})),
            (r#"{"items":[1,2,3"#, json!({"items": [1, 2, 3]})),
            (r#"["alpha","bet"#, json!(["alpha", "bet"])),
            (r#"{"日本語":"途中"#, json!({"日本語": "途中"})),
        ];

        for (input, expected) in cases {
            assert_eq!(parse_streaming(input), expected, "{input}");
        }
    }

    #[test]
    fn removes_incomplete_trailing_members_and_tokens() {
        let cases = [
            (r#"{"ready":true,"#, json!({"ready": true})),
            (r#"{"ready":true,"next":"#, json!({"ready": true})),
            (
                r#"{"ready":true,"next":"va"#,
                json!({"ready": true, "next": "va"}),
            ),
            (
                "{\"ready\":true,\"next\":\"",
                json!({"ready": true, "next": ""}),
            ),
            (
                r#"{"ready":true,"next":"x","#,
                json!({"ready": true, "next": "x"}),
            ),
            (
                r#"{"ready":true,"next":"x","later"#,
                json!({"ready": true, "next": "x"}),
            ),
            (
                r#"{"ready":true,"next":"x","later":"#,
                json!({"ready": true, "next": "x"}),
            ),
            (r#"{"ready":true,"next":tru"#, json!({"ready": true})),
            (
                r#"{"ready":true,"next":1."#,
                json!({"ready": true, "next": 1}),
            ),
            (r#"[1,2,"#, json!([1, 2])),
        ];

        for (input, expected) in cases {
            assert_eq!(parse_streaming(input), expected, "{input}");
        }
    }

    #[test]
    fn repair_and_partial_completion_can_be_combined() {
        let cases = [
            (r#"{"path":"a\q"#, json!({"path": r"a\q"})),
            ("{\"text\":\"line\nbreak", json!({"text": "line\nbreak"})),
            (r#"{"path":"trailing\"#, json!({"path": "trailing\\"})),
        ];

        for (input, expected) in cases {
            assert_eq!(parse_streaming(input), expected, "{input:?}");
        }
    }

    #[test]
    fn final_parser_does_not_accept_incomplete_json() {
        for input in [r#"{"name":"Sumi"#, r#"{"ready":"#, "[1,2,"] {
            assert!(parse_final(input).is_err(), "{input}");
        }
    }

    #[test]
    fn streaming_parser_falls_back_to_empty_object() {
        for input in ["", "   ", "not json", "}", "]", "\u{0000}"] {
            assert_eq!(parse_streaming(input), json!({}), "{input:?}");
        }
    }
}
