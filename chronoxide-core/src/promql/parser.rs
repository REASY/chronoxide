use promql_parser::parser as parser_promql;

use super::{
    ast::{PromqlQuery, PromqlQueryError, PromqlSelector},
    lower::{lower_expr, lower_vector_selector},
    normalize::{is_label_first, is_metric_first, is_metric_rest, normalize_label_name},
};

pub fn parse_vector_selector(input: &str) -> Result<PromqlSelector, PromqlQueryError> {
    match parse_external_expr(input)? {
        parser_promql::Expr::VectorSelector(selector) => {
            if selector.offset.is_some() {
                return Err(PromqlQueryError::Unsupported(
                    "offset modifiers require full PromQL query parsing".to_string(),
                ));
            }
            lower_vector_selector(&selector)
        }
        _ => Err(PromqlQueryError::Unsupported(
            "PromQL expressions are not implemented".to_string(),
        )),
    }
}

pub fn parse_query(input: &str) -> Result<PromqlQuery, PromqlQueryError> {
    lower_expr(&parse_external_expr(input)?)
}

fn parse_external_expr(input: &str) -> Result<parser_promql::Expr, PromqlQueryError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(PromqlQueryError::Invalid("empty selector".to_string()));
    }
    let aliased = rewrite_promql_function_aliases(input);
    let parser_input = if aliased == input {
        input
    } else {
        aliased.as_str()
    };
    match parser_promql::parse(parser_input) {
        Ok(expr) => Ok(expr),
        Err(primary_err) => {
            let rewritten = rewrite_otlp_style_identifiers(parser_input);
            if rewritten == parser_input {
                return Err(PromqlQueryError::Invalid(primary_err));
            }
            parser_promql::parse(&rewritten).map_err(|rewrite_err| {
                PromqlQueryError::Invalid(format!(
                    "{primary_err}; after OTLP identifier rewrite: {rewrite_err}"
                ))
            })
        }
    }
}

fn rewrite_promql_function_aliases(input: &str) -> String {
    const HOLT_WINTERS: &str = "holt_winters";
    const DOUBLE_EXPONENTIAL_SMOOTHING: &str = "double_exponential_smoothing";

    let mut out = String::with_capacity(input.len());
    let mut cursor = 0;
    while cursor < input.len() {
        let Some((ch, next)) = next_char(input, cursor) else {
            break;
        };
        if is_quote(ch) {
            cursor = copy_quoted(input, cursor, &mut out);
            continue;
        }
        if input[cursor..].starts_with(HOLT_WINTERS)
            && alias_has_left_boundary(input, cursor)
            && alias_is_function_call(input, cursor + HOLT_WINTERS.len())
        {
            out.push_str(DOUBLE_EXPONENTIAL_SMOOTHING);
            cursor += HOLT_WINTERS.len();
            continue;
        }
        out.push(ch);
        cursor = next;
    }
    out
}

fn alias_has_left_boundary(input: &str, cursor: usize) -> bool {
    input[..cursor]
        .chars()
        .next_back()
        .is_none_or(|ch| !is_metric_rest(ch))
}

fn alias_is_function_call(input: &str, mut cursor: usize) -> bool {
    while cursor < input.len() {
        let Some((ch, next)) = next_char(input, cursor) else {
            return false;
        };
        if ch.is_whitespace() {
            cursor = next;
            continue;
        }
        return ch == '(';
    }
    false
}

fn rewrite_otlp_style_identifiers(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut cursor = 0;
    while cursor < input.len() {
        let Some((ch, next)) = next_char(input, cursor) else {
            break;
        };
        if is_quote(ch) {
            cursor = copy_quoted(input, cursor, &mut out);
            continue;
        }
        if ch == '{' {
            out.push(ch);
            cursor = rewrite_label_matchers(input, next, &mut out);
            continue;
        }
        if is_metric_first(ch) {
            let start = cursor;
            cursor = next;
            while let Some((next_ch, next_cursor)) = next_char(input, cursor) {
                if !is_otlp_metric_rest(next_ch) {
                    break;
                }
                cursor = next_cursor;
            }
            let token = &input[start..cursor];
            if token.contains('.') {
                cursor = rewrite_dotted_metric_token(input, cursor, token, &mut out);
            } else if matches!(
                token,
                "by" | "without" | "on" | "ignoring" | "group_left" | "group_right"
            ) && matches!(
                next_char(input, skip_whitespace(input, cursor)),
                Some(('(', _))
            ) {
                out.push_str(token);
                cursor = rewrite_grouping_labels(input, cursor, &mut out);
            } else {
                out.push_str(token);
            }
            continue;
        }
        out.push(ch);
        cursor = next;
    }
    out
}

fn rewrite_dotted_metric_token(
    input: &str,
    cursor: usize,
    metric_name: &str,
    out: &mut String,
) -> usize {
    let selector_start = skip_whitespace(input, cursor);
    if matches!(next_char(input, selector_start), Some(('(', _))) {
        out.push_str(metric_name);
        return cursor;
    }

    out.push_str("{__name__=");
    push_quoted(metric_name, out);

    if let Some(('{', after_open)) = next_char(input, selector_start) {
        let after_matcher_ws = skip_whitespace(input, after_open);
        if let Some(('}', after_close)) = next_char(input, after_matcher_ws) {
            out.push('}');
            return after_close;
        }
        out.push(',');
        rewrite_label_matchers(input, after_open, out)
    } else {
        out.push('}');
        cursor
    }
}

fn rewrite_label_matchers(input: &str, mut cursor: usize, out: &mut String) -> usize {
    while cursor < input.len() {
        let Some((ch, next)) = next_char(input, cursor) else {
            break;
        };
        if is_quote(ch) {
            cursor = copy_quoted(input, cursor, out);
            continue;
        }
        if ch == '}' {
            out.push(ch);
            return next;
        }
        if is_label_first(ch) {
            let start = cursor;
            cursor = next;
            while let Some((next_ch, next_cursor)) = next_char(input, cursor) {
                if !is_otlp_label_rest(next_ch) {
                    break;
                }
                cursor = next_cursor;
            }
            let token = &input[start..cursor];
            if token.contains('.')
                && matcher_operator_starts_at(input, skip_whitespace(input, cursor))
            {
                push_quoted(token, out);
            } else {
                out.push_str(token);
            }
            continue;
        }
        out.push(ch);
        cursor = next;
    }
    cursor
}

fn rewrite_grouping_labels(input: &str, mut cursor: usize, out: &mut String) -> usize {
    while let Some((ch, next)) = next_char(input, cursor) {
        out.push(ch);
        cursor = next;
        if ch == '(' {
            break;
        }
    }

    while cursor < input.len() {
        let Some((ch, next)) = next_char(input, cursor) else {
            break;
        };
        if ch == ')' {
            out.push(ch);
            return next;
        }
        if is_label_first(ch) {
            let start = cursor;
            cursor = next;
            while let Some((next_ch, next_cursor)) = next_char(input, cursor) {
                if !is_otlp_label_rest(next_ch) {
                    break;
                }
                cursor = next_cursor;
            }
            let token = &input[start..cursor];
            if token.contains('.') {
                out.push_str(&normalize_label_name(token));
            } else {
                out.push_str(token);
            }
            continue;
        }
        out.push(ch);
        cursor = next;
    }
    cursor
}

fn copy_quoted(input: &str, cursor: usize, out: &mut String) -> usize {
    let Some((quote, mut next)) = next_char(input, cursor) else {
        return cursor;
    };
    out.push(quote);
    while next < input.len() {
        let Some((ch, after_ch)) = next_char(input, next) else {
            return next;
        };
        if quote != '`' && ch == '\\' {
            if let Some((escaped, after_escaped)) = next_char(input, after_ch) {
                if escaped == '/' {
                    out.push(escaped);
                } else if is_standard_string_escape(escaped) {
                    out.push(ch);
                    out.push(escaped);
                } else {
                    out.push(ch);
                    out.push(ch);
                    out.push(escaped);
                }
                next = after_escaped;
            } else {
                out.push(ch);
                next = after_ch;
            }
            continue;
        }
        out.push(ch);
        next = after_ch;
        if ch == quote {
            return next;
        }
    }
    next
}

fn push_quoted(value: &str, out: &mut String) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(ch),
        }
    }
    out.push('"');
}

fn skip_whitespace(input: &str, mut cursor: usize) -> usize {
    while let Some((ch, next)) = next_char(input, cursor) {
        if !ch.is_whitespace() {
            break;
        }
        cursor = next;
    }
    cursor
}

fn matcher_operator_starts_at(input: &str, cursor: usize) -> bool {
    input[cursor..].starts_with('=')
        || input[cursor..].starts_with("!=")
        || input[cursor..].starts_with("=~")
        || input[cursor..].starts_with("!~")
}

fn is_quote(ch: char) -> bool {
    ch == '"' || ch == '\'' || ch == '`'
}

fn is_standard_string_escape(ch: char) -> bool {
    matches!(
        ch,
        'a' | 'b' | 'f' | 'n' | 'r' | 't' | 'v' | '\\' | '"' | '\'' | 'x' | 'u' | 'U' | '0'..='7'
    )
}

fn is_otlp_metric_rest(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == ':' || ch == '.'
}

fn is_otlp_label_rest(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == '.'
}

fn next_char(input: &str, cursor: usize) -> Option<(char, usize)> {
    input[cursor..]
        .chars()
        .next()
        .map(|ch| (ch, cursor + ch.len_utf8()))
}
