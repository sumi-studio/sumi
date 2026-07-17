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
    // Keep the number of serde attempts bounded. Streaming tool arguments can
    // grow on every delta, so retrying every character prefix makes a single
    // parse quadratic and a complete stream cubic.
    if let Some(value) = parse_completed_prefix(input) {
        return Some(value);
    }

    if let Some(number_prefix) = truncate_incomplete_number(input)
        && let Some(value) = parse_completed_prefix(number_prefix)
    {
        return Some(value);
    }

    if let Some(comma) = last_structural_comma(input)
        && let Some(value) = parse_completed_prefix(input[..comma].trim_end())
    {
        return Some(value);
    }

    None
}

fn parse_completed_prefix(prefix: &str) -> Option<Value> {
    close_open_structures(prefix).and_then(|completed| serde_json::from_str(&completed).ok())
}

fn truncate_incomplete_number(input: &str) -> Option<&str> {
    let trimmed = input.trim_end();
    let token_start = trimmed
        .rfind(|character: char| {
            character.is_ascii_whitespace() || matches!(character, ':' | ',' | '[' | '{')
        })
        .map_or(0, |index| index + 1);
    let token = &trimmed[token_start..];
    let valid_end = longest_complete_number_prefix(token)?;
    (valid_end < token.len()).then_some(&trimmed[..token_start + valid_end])
}

fn longest_complete_number_prefix(token: &str) -> Option<usize> {
    let bytes = token.as_bytes();
    let mut index = 0;
    if bytes.first() == Some(&b'-') {
        index += 1;
    }

    match bytes.get(index) {
        Some(b'0') => index += 1,
        Some(b'1'..=b'9') => {
            index += 1;
            while bytes.get(index).is_some_and(u8::is_ascii_digit) {
                index += 1;
            }
        }
        _ => return None,
    }
    let mut last_complete = index;

    if bytes.get(index) == Some(&b'.') {
        index += 1;
        let fraction_start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if index > fraction_start {
            last_complete = index;
        }
    }

    if matches!(bytes.get(index), Some(b'e' | b'E')) {
        index += 1;
        if matches!(bytes.get(index), Some(b'+' | b'-')) {
            index += 1;
        }
        let exponent_start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            index += 1;
        }
        if index > exponent_start {
            last_complete = index;
        }
    }

    Some(last_complete)
}

fn last_structural_comma(input: &str) -> Option<usize> {
    let mut last_comma = None;
    let mut in_string = false;
    let mut escaped = false;

    for (index, character) in input.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
            }
        } else {
            match character {
                '"' => in_string = true,
                ',' => last_comma = Some(index),
                _ => {}
            }
        }
    }

    last_comma
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
            // 閉じ括弧のガードは stack.pop() の副作用で対応する開き括弧を
            // 消費する。ガード不成立(=対応が取れた)場合も pop 済み。
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
            assert_eq!(parse_streaming(input), expected, "{input:?}");
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
            assert_eq!(parse_streaming(input), expected, "{input}");
        }
        assert_eq!(parse_streaming(r#"{"value":"\u12"}"#), json!({}));
    }

    #[test]
    fn preserves_valid_escape_sequences() {
        let input = r#"{"quote":"\"","slash":"\\","line":"\n","unicode":"\u65e5"}"#;
        assert_eq!(
            parse_streaming(input),
            json!({
                "quote": "\"",
                "slash": "\\",
                "line": "\n",
                "unicode": "日"
            })
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
    fn partial_parser_uses_a_bounded_number_of_candidates() {
        let input = format!(r#"{{"value":"{}","next":tru"#, "x".repeat(1_000_000));
        assert_eq!(
            parse_streaming(&input),
            json!({"value": "x".repeat(1_000_000)})
        );
    }

    #[test]
    fn recovers_incomplete_numbers() {
        let cases = [
            (r#"{"value":1."#, json!({"value": 1})),
            (r#"{"value":-12.5e+"#, json!({"value": -12.5})),
            (r#"{"value":12e3"#, json!({"value": 12e3})),
        ];
        for (input, expected) in cases {
            assert_eq!(parse_streaming(input), expected, "{input}");
        }
    }

    #[test]
    fn streaming_parser_falls_back_to_empty_object() {
        for input in ["", "   ", "not json", "}", "]", "\u{0000}"] {
            assert_eq!(parse_streaming(input), json!({}), "{input:?}");
        }
    }
}
