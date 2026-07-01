use std::collections::BTreeMap;
use std::fmt;

pub use crate::labels::METRIC_NAME_LABEL;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalLabel {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalLabelSet {
    labels: Vec<CanonicalLabel>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromqlSelector {
    pub metric_name: Option<String>,
    pub matchers: Vec<PromqlMatcher>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromqlQuery {
    Vector(PromqlSelector),
    RangeFunction(PromqlRangeFunction),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromqlRangeFunction {
    pub kind: PromqlRangeFunctionKind,
    pub selector: PromqlSelector,
    pub range_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromqlRangeFunctionKind {
    Rate,
    Increase,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromqlMatcher {
    pub name: String,
    pub op: PromqlMatcherOp,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromqlMatcherOp {
    Eq,
    NotEq,
    Regex,
    NotRegex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromqlQueryError {
    Invalid(String),
    Unsupported(String),
    LimitExceeded { limit: String, max: u64 },
    Storage(String),
}

impl fmt::Display for PromqlQueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => write!(f, "invalid PromQL selector: {message}"),
            Self::Unsupported(message) => write!(f, "unsupported PromQL query: {message}"),
            Self::LimitExceeded { limit, max } => {
                write!(f, "PromQL query exceeded {limit} limit of {max}")
            }
            Self::Storage(message) => write!(f, "storage query failed: {message}"),
        }
    }
}

impl std::error::Error for PromqlQueryError {}

impl From<std::io::Error> for PromqlQueryError {
    fn from(value: std::io::Error) -> Self {
        Self::Storage(value.to_string())
    }
}

impl CanonicalLabelSet {
    pub fn labels(&self) -> &[CanonicalLabel] {
        &self.labels
    }
}

pub fn normalize_metric_name(original: &str) -> String {
    normalize_name(original, is_metric_first, is_metric_rest, false)
}

pub fn normalize_label_name(original: &str) -> String {
    normalize_name(original, is_label_first, is_label_rest, true)
}

pub fn canonicalize_labelset(metric_name: &str, labels: &[(&str, &str)]) -> CanonicalLabelSet {
    let mut canonical = BTreeMap::new();
    canonical.insert(
        METRIC_NAME_LABEL.to_string(),
        normalize_metric_name(metric_name),
    );

    for (name, value) in labels {
        canonical.insert(normalize_label_name(name), (*value).to_string());
    }

    CanonicalLabelSet {
        labels: canonical
            .into_iter()
            .map(|(name, value)| CanonicalLabel { name, value })
            .collect(),
    }
}

pub fn series_id(canonical: &CanonicalLabelSet) -> u64 {
    let mut bytes = Vec::new();
    for label in canonical.labels() {
        bytes.extend_from_slice(label.name.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(label.value.as_bytes());
        bytes.push(0xff);
    }
    xxhash64(&bytes)
}

pub fn parse_vector_selector(input: &str) -> Result<PromqlSelector, PromqlQueryError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(PromqlQueryError::Invalid("empty selector".to_string()));
    }
    let mut parser = SelectorParser::new(input);
    parser.parse()
}

pub fn parse_query(input: &str) -> Result<PromqlQuery, PromqlQueryError> {
    let input = input.trim();
    if input.is_empty() {
        return Err(PromqlQueryError::Invalid("empty selector".to_string()));
    }
    let mut parser = SelectorParser::new(input);
    parser.parse_query()
}

struct SelectorParser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> SelectorParser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn parse_query(&mut self) -> Result<PromqlQuery, PromqlQueryError> {
        self.skip_ws();
        if self.peek_char() == Some('{') {
            return self.parse().map(PromqlQuery::Vector);
        }
        let start = self.pos;
        let name = self.parse_identifier("metric name")?;
        self.skip_ws();
        if self.peek_char() != Some('(') {
            self.pos = start;
            return self.parse().map(PromqlQuery::Vector);
        }

        let kind = match name.as_str() {
            "rate" => PromqlRangeFunctionKind::Rate,
            "increase" => PromqlRangeFunctionKind::Increase,
            _ => {
                return Err(PromqlQueryError::Unsupported(format!(
                    "unsupported PromQL function {name}"
                )));
            }
        };

        self.bump_char();
        let selector = self.parse_selector_prefix()?;
        self.skip_ws();
        let range_ms = self.parse_range_duration_ms()?;
        self.skip_ws();
        if self.peek_char() != Some(')') {
            return Err(self.invalid("expected ')'"));
        }
        self.bump_char();
        self.skip_ws();
        if !self.is_eof() {
            return Err(self.invalid("unexpected trailing input"));
        }

        Ok(PromqlQuery::RangeFunction(PromqlRangeFunction {
            kind,
            selector,
            range_ms,
        }))
    }

    fn parse(&mut self) -> Result<PromqlSelector, PromqlQueryError> {
        let selector = self.parse_selector_prefix()?;
        self.skip_ws();
        if !self.is_eof() {
            if self.peek_char().is_some_and(is_expression_syntax) {
                return Err(PromqlQueryError::Unsupported(
                    "PromQL expressions are not implemented".to_string(),
                ));
            }
            return Err(self.invalid("unexpected trailing input"));
        }
        Ok(selector)
    }

    fn parse_selector_prefix(&mut self) -> Result<PromqlSelector, PromqlQueryError> {
        self.skip_ws();
        let metric_name = if self.peek_char() == Some('{') {
            None
        } else {
            Some(self.parse_identifier("metric name")?)
        };
        self.skip_ws();

        let mut selector = PromqlSelector {
            metric_name,
            matchers: Vec::new(),
        };

        if self.peek_char() == Some('{') {
            self.bump_char();
            self.parse_matchers(&mut selector)?;
        }

        if selector.metric_name.is_none() && selector.matchers.is_empty() {
            return Err(self.invalid("selector must include a metric name or matcher"));
        }

        Ok(selector)
    }

    fn parse_matchers(&mut self, selector: &mut PromqlSelector) -> Result<(), PromqlQueryError> {
        loop {
            self.skip_ws();
            if self.peek_char() == Some('}') {
                self.bump_char();
                return Ok(());
            }

            let name = self.parse_identifier("label name")?;
            self.skip_ws();
            let op = self.parse_matcher_op()?;
            self.skip_ws();
            let value = self.parse_quoted_string()?;

            if name == METRIC_NAME_LABEL {
                if op != PromqlMatcherOp::Eq && op != PromqlMatcherOp::Regex {
                    return Err(PromqlQueryError::Unsupported(
                        "__name__ currently supports only equality or regex".to_string(),
                    ));
                }
                if let Some(existing) = &selector.metric_name
                    && op == PromqlMatcherOp::Eq
                    && existing != &value
                {
                    return Err(self.invalid("conflicting metric names"));
                }
                if op == PromqlMatcherOp::Eq {
                    selector.metric_name = Some(value);
                } else {
                    selector.matchers.push(PromqlMatcher { name, op, value });
                }
            } else {
                selector.matchers.push(PromqlMatcher { name, op, value });
            }

            self.skip_ws();
            match self.peek_char() {
                Some(',') => {
                    self.bump_char();
                }
                Some('}') => {
                    self.bump_char();
                    return Ok(());
                }
                Some(_) => return Err(self.invalid("expected ',' or '}'")),
                None => return Err(self.invalid("unterminated matcher list")),
            }
        }
    }

    fn parse_range_duration_ms(&mut self) -> Result<u64, PromqlQueryError> {
        if self.peek_char() != Some('[') {
            return Err(self.invalid("expected range duration"));
        }
        self.bump_char();
        let start = self.pos;
        while self.peek_char().is_some_and(|ch| ch != ']') {
            self.bump_char();
        }
        if self.peek_char() != Some(']') {
            return Err(self.invalid("unterminated range duration"));
        }
        let duration = &self.input[start..self.pos];
        self.bump_char();
        parse_duration_ms(duration).map_err(|message| self.invalid(message))
    }

    fn parse_identifier(&mut self, what: &str) -> Result<String, PromqlQueryError> {
        let start = self.pos;
        let Some(ch) = self.peek_char() else {
            return Err(self.invalid(format!("expected {what}")));
        };
        if !is_promql_ident_first(ch) {
            return Err(self.invalid(format!("expected {what}")));
        }
        self.bump_char();
        while self.peek_char().is_some_and(is_promql_ident_rest) {
            self.bump_char();
        }
        Ok(self.input[start..self.pos].to_string())
    }

    fn parse_matcher_op(&mut self) -> Result<PromqlMatcherOp, PromqlQueryError> {
        if self.consume("!=") {
            Ok(PromqlMatcherOp::NotEq)
        } else if self.consume("=~") {
            Ok(PromqlMatcherOp::Regex)
        } else if self.consume("!~") {
            Ok(PromqlMatcherOp::NotRegex)
        } else if self.consume("=") {
            Ok(PromqlMatcherOp::Eq)
        } else {
            Err(self.invalid("expected matcher operator"))
        }
    }

    fn parse_quoted_string(&mut self) -> Result<String, PromqlQueryError> {
        if self.peek_char() != Some('"') {
            return Err(self.invalid("expected quoted string"));
        }
        self.bump_char();

        let mut out = String::new();
        loop {
            let Some(ch) = self.bump_char() else {
                return Err(self.invalid("unterminated quoted string"));
            };
            match ch {
                '"' => return Ok(out),
                '\\' => {
                    let Some(escaped) = self.bump_char() else {
                        return Err(self.invalid("unterminated escape sequence"));
                    };
                    let value = match escaped {
                        'n' => '\n',
                        't' => '\t',
                        'r' => '\r',
                        '\\' => '\\',
                        '"' => '"',
                        '/' => '/',
                        other => other,
                    };
                    out.push(value);
                }
                other => out.push(other),
            }
        }
    }

    fn skip_ws(&mut self) {
        while self.peek_char().is_some_and(char::is_whitespace) {
            self.bump_char();
        }
    }

    fn consume(&mut self, value: &str) -> bool {
        if self.input[self.pos..].starts_with(value) {
            self.pos += value.len();
            true
        } else {
            false
        }
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn bump_char(&mut self) -> Option<char> {
        let ch = self.peek_char()?;
        self.pos += ch.len_utf8();
        Some(ch)
    }

    fn is_eof(&self) -> bool {
        self.pos == self.input.len()
    }

    fn invalid(&self, message: impl Into<String>) -> PromqlQueryError {
        PromqlQueryError::Invalid(message.into())
    }
}

fn parse_duration_ms(input: &str) -> Result<u64, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("empty range duration".to_string());
    }

    let mut pos = 0usize;
    let mut total = 0u64;
    while pos < input.len() {
        let digits_start = pos;
        while input[pos..]
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_digit())
        {
            pos += input[pos..].chars().next().unwrap().len_utf8();
        }
        if digits_start == pos {
            return Err("expected duration number".to_string());
        }
        let value: u64 = input[digits_start..pos]
            .parse()
            .map_err(|_| "duration number is too large".to_string())?;

        let unit_start = pos;
        while input[pos..]
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_alphabetic())
        {
            pos += input[pos..].chars().next().unwrap().len_utf8();
        }
        if unit_start == pos {
            return Err("expected duration unit".to_string());
        }
        let unit = &input[unit_start..pos];
        let multiplier = match unit {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            "w" => 604_800_000,
            "y" => 31_536_000_000,
            _ => return Err(format!("unsupported duration unit {unit}")),
        };
        let component = value
            .checked_mul(multiplier)
            .ok_or_else(|| "duration is too large".to_string())?;
        total = total
            .checked_add(component)
            .ok_or_else(|| "duration is too large".to_string())?;
    }

    if total == 0 {
        return Err("range duration must be positive".to_string());
    }
    Ok(total)
}

fn normalize_name(
    original: &str,
    first_ok: fn(char) -> bool,
    rest_ok: fn(char) -> bool,
    label_rules: bool,
) -> String {
    let mut out = String::with_capacity(original.len().max(1));
    for ch in original.chars() {
        if rest_ok(ch) {
            out.push(ch);
        } else {
            out.push('_');
        }
    }

    if out.is_empty() {
        out.push('_');
    } else if !out.chars().next().is_some_and(first_ok) {
        out.insert(0, '_');
    }

    if label_rules && (out == METRIC_NAME_LABEL || out.starts_with("__")) {
        out.insert_str(0, "otel_");
    }

    if out != original {
        out.push_str("_x");
        out.push_str(&format!("{:016x}", xxhash64(original.as_bytes())));
    }

    out
}

fn is_metric_first(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_' || ch == ':'
}

fn is_metric_rest(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == ':'
}

fn is_label_first(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_label_rest(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn is_promql_ident_first(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_' || ch == ':'
}

fn is_promql_ident_rest(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == ':' || ch == '.'
}

fn is_expression_syntax(ch: char) -> bool {
    matches!(
        ch,
        '+' | '-' | '*' | '/' | '%' | '^' | '>' | '<' | '(' | ')' | '[' | ']'
    )
}

fn xxhash64(input: &[u8]) -> u64 {
    const P1: u64 = 11_400_714_785_074_694_791;
    const P2: u64 = 14_029_467_366_897_019_727;
    const P3: u64 = 1_609_587_929_392_839_161;
    const P4: u64 = 9_650_029_242_287_828_579;
    const P5: u64 = 2_870_177_450_012_600_261;

    let mut cursor = 0usize;
    let mut h64;

    if input.len() >= 32 {
        let mut v1 = P1.wrapping_add(P2);
        let mut v2 = P2;
        let mut v3 = 0;
        let mut v4 = 0u64.wrapping_sub(P1);

        while cursor + 32 <= input.len() {
            v1 = xxh64_round(v1, read_u64(input, cursor));
            cursor += 8;
            v2 = xxh64_round(v2, read_u64(input, cursor));
            cursor += 8;
            v3 = xxh64_round(v3, read_u64(input, cursor));
            cursor += 8;
            v4 = xxh64_round(v4, read_u64(input, cursor));
            cursor += 8;
        }

        h64 = v1
            .rotate_left(1)
            .wrapping_add(v2.rotate_left(7))
            .wrapping_add(v3.rotate_left(12))
            .wrapping_add(v4.rotate_left(18));
        h64 = xxh64_merge_round(h64, v1);
        h64 = xxh64_merge_round(h64, v2);
        h64 = xxh64_merge_round(h64, v3);
        h64 = xxh64_merge_round(h64, v4);
    } else {
        h64 = P5;
    }

    h64 = h64.wrapping_add(input.len() as u64);

    while cursor + 8 <= input.len() {
        let k1 = xxh64_round(0, read_u64(input, cursor));
        h64 ^= k1;
        h64 = h64.rotate_left(27).wrapping_mul(P1).wrapping_add(P4);
        cursor += 8;
    }

    if cursor + 4 <= input.len() {
        h64 ^= u64::from(read_u32(input, cursor)).wrapping_mul(P1);
        h64 = h64.rotate_left(23).wrapping_mul(P2).wrapping_add(P3);
        cursor += 4;
    }

    while cursor < input.len() {
        h64 ^= u64::from(input[cursor]).wrapping_mul(P5);
        h64 = h64.rotate_left(11).wrapping_mul(P1);
        cursor += 1;
    }

    h64 ^= h64 >> 33;
    h64 = h64.wrapping_mul(P2);
    h64 ^= h64 >> 29;
    h64 = h64.wrapping_mul(P3);
    h64 ^ (h64 >> 32)
}

fn xxh64_round(acc: u64, input: u64) -> u64 {
    const P1: u64 = 11_400_714_785_074_694_791;
    const P2: u64 = 14_029_467_366_897_019_727;

    acc.wrapping_add(input.wrapping_mul(P2))
        .rotate_left(31)
        .wrapping_mul(P1)
}

fn xxh64_merge_round(acc: u64, value: u64) -> u64 {
    const P1: u64 = 11_400_714_785_074_694_791;
    const P4: u64 = 9_650_029_242_287_828_579;

    (acc ^ xxh64_round(0, value))
        .wrapping_mul(P1)
        .wrapping_add(P4)
}

fn read_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(input[offset..offset + 8].try_into().unwrap())
}

fn read_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(input[offset..offset + 4].try_into().unwrap())
}
