use std::{collections::BTreeMap, fmt, time::Duration};

use promql_parser::{label::MatchOp as ParserMatchOp, parser as parser_promql};

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

#[derive(Debug, Clone, PartialEq)]
pub enum PromqlQuery {
    Vector(PromqlSelector),
    Scalar(f64),
    RangeFunction(PromqlRangeFunction),
    Aggregation(PromqlAggregation),
    HistogramQuantile(PromqlHistogramQuantile),
    BinaryExpression(PromqlBinaryExpression),
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

#[derive(Debug, Clone, PartialEq)]
pub struct PromqlAggregation {
    pub op: PromqlAggregationOp,
    pub grouping: PromqlAggregationGrouping,
    pub input: Box<PromqlQuery>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromqlAggregationOp {
    Sum,
    Count,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromqlAggregationGrouping {
    All,
    By(Vec<String>),
    Without(Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromqlHistogramQuantile {
    pub quantile: f64,
    pub input: Box<PromqlQuery>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromqlBinaryExpression {
    pub op: PromqlBinaryOp,
    pub left: Box<PromqlQuery>,
    pub right: Box<PromqlQuery>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromqlBinaryOp {
    Add,
    Sub,
    Mul,
    Div,
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
    match parse_external_expr(input)? {
        parser_promql::Expr::VectorSelector(selector) => lower_vector_selector(&selector),
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
    match parser_promql::parse(input) {
        Ok(expr) => Ok(expr),
        Err(primary_err) => {
            let rewritten = rewrite_otlp_style_identifiers(input);
            if rewritten == input {
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
            } else if matches!(token, "by" | "without")
                && matches!(
                    next_char(input, skip_whitespace(input, cursor)),
                    Some(('(', _))
                )
            {
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

fn lower_expr(expr: &parser_promql::Expr) -> Result<PromqlQuery, PromqlQueryError> {
    match expr {
        parser_promql::Expr::VectorSelector(selector) => {
            lower_vector_selector(selector).map(PromqlQuery::Vector)
        }
        parser_promql::Expr::NumberLiteral(number) => Ok(PromqlQuery::Scalar(number.val)),
        parser_promql::Expr::Unary(unary) => lower_unary_expression(unary),
        parser_promql::Expr::Paren(paren) => lower_expr(&paren.expr),
        parser_promql::Expr::Call(call) => lower_call(call),
        parser_promql::Expr::Aggregate(aggregation) => lower_aggregation(aggregation),
        parser_promql::Expr::Binary(binary) => lower_binary_expression(binary),
        parser_promql::Expr::MatrixSelector(_)
        | parser_promql::Expr::Subquery(_)
        | parser_promql::Expr::StringLiteral(_)
        | parser_promql::Expr::Extension(_) => Err(PromqlQueryError::Unsupported(
            "unsupported PromQL expression".to_string(),
        )),
    }
}

fn lower_unary_expression(
    unary: &parser_promql::UnaryExpr,
) -> Result<PromqlQuery, PromqlQueryError> {
    match lower_expr(&unary.expr)? {
        PromqlQuery::Scalar(value) => Ok(PromqlQuery::Scalar(-value)),
        _ => Err(PromqlQueryError::Unsupported(
            "unary operators are supported only for scalar literals".to_string(),
        )),
    }
}

fn lower_call(call: &parser_promql::Call) -> Result<PromqlQuery, PromqlQueryError> {
    match call.func.name {
        "rate" | "increase" => {
            if call.args.len() != 1 {
                return Err(PromqlQueryError::Invalid(format!(
                    "{} expects one argument",
                    call.func.name
                )));
            }
            let Some(arg) = call.args.args.first() else {
                return Err(PromqlQueryError::Invalid(format!(
                    "{} expects one argument",
                    call.func.name
                )));
            };
            let parser_promql::Expr::MatrixSelector(matrix) = arg.as_ref() else {
                return Err(PromqlQueryError::Unsupported(format!(
                    "{} currently supports only selector range arguments",
                    call.func.name
                )));
            };
            let kind = if call.func.name == "rate" {
                PromqlRangeFunctionKind::Rate
            } else {
                PromqlRangeFunctionKind::Increase
            };
            Ok(PromqlQuery::RangeFunction(PromqlRangeFunction {
                kind,
                selector: lower_vector_selector(&matrix.vs)?,
                range_ms: duration_ms(matrix.range)?,
            }))
        }
        "histogram_quantile" => lower_histogram_quantile(call),
        other => Err(PromqlQueryError::Unsupported(format!(
            "unsupported PromQL function {other}"
        ))),
    }
}

fn lower_histogram_quantile(call: &parser_promql::Call) -> Result<PromqlQuery, PromqlQueryError> {
    if call.args.len() != 2 {
        return Err(PromqlQueryError::Invalid(
            "histogram_quantile expects two arguments".to_string(),
        ));
    }
    let quantile = lower_quantile_argument(call.args.args[0].as_ref())?;
    let input = lower_expr(call.args.args[1].as_ref())?;
    Ok(PromqlQuery::HistogramQuantile(PromqlHistogramQuantile {
        quantile,
        input: Box::new(input),
    }))
}

fn lower_quantile_argument(expr: &parser_promql::Expr) -> Result<f64, PromqlQueryError> {
    match expr {
        parser_promql::Expr::NumberLiteral(number) if number.val.is_finite() => Ok(number.val),
        parser_promql::Expr::Unary(unary) => {
            let PromqlQuery::Scalar(value) = lower_unary_expression(unary)? else {
                return Err(PromqlQueryError::Invalid(
                    "histogram_quantile quantile must be a scalar literal".to_string(),
                ));
            };
            if value.is_finite() {
                Ok(value)
            } else {
                Err(PromqlQueryError::Invalid(
                    "histogram_quantile quantile must be finite".to_string(),
                ))
            }
        }
        _ => Err(PromqlQueryError::Invalid(
            "histogram_quantile quantile must be a finite scalar literal".to_string(),
        )),
    }
}

fn lower_aggregation(
    aggregation: &parser_promql::AggregateExpr,
) -> Result<PromqlQuery, PromqlQueryError> {
    if aggregation.param.is_some() {
        return Err(PromqlQueryError::Unsupported(
            "aggregation parameters are not implemented".to_string(),
        ));
    }
    let op = match aggregation.op.to_string().as_str() {
        "sum" => PromqlAggregationOp::Sum,
        "count" => PromqlAggregationOp::Count,
        "avg" => PromqlAggregationOp::Avg,
        "min" => PromqlAggregationOp::Min,
        "max" => PromqlAggregationOp::Max,
        other => {
            return Err(PromqlQueryError::Unsupported(format!(
                "unsupported aggregation operator {other}"
            )));
        }
    };
    let grouping = match &aggregation.modifier {
        None => PromqlAggregationGrouping::All,
        Some(parser_promql::LabelModifier::Include(labels)) => {
            PromqlAggregationGrouping::By(labels.labels.clone())
        }
        Some(parser_promql::LabelModifier::Exclude(labels)) => {
            PromqlAggregationGrouping::Without(labels.labels.clone())
        }
    };
    Ok(PromqlQuery::Aggregation(PromqlAggregation {
        op,
        grouping,
        input: Box::new(lower_expr(&aggregation.expr)?),
    }))
}

fn lower_binary_expression(
    binary: &parser_promql::BinaryExpr,
) -> Result<PromqlQuery, PromqlQueryError> {
    if binary.modifier.is_some() {
        return Err(PromqlQueryError::Unsupported(
            "binary vector matching modifiers are not implemented".to_string(),
        ));
    }
    let op = match binary.op.to_string().as_str() {
        "+" => PromqlBinaryOp::Add,
        "-" => PromqlBinaryOp::Sub,
        "*" => PromqlBinaryOp::Mul,
        "/" => PromqlBinaryOp::Div,
        other => {
            return Err(PromqlQueryError::Unsupported(format!(
                "unsupported binary operator {other}"
            )));
        }
    };
    Ok(PromqlQuery::BinaryExpression(PromqlBinaryExpression {
        op,
        left: Box::new(lower_expr(&binary.lhs)?),
        right: Box::new(lower_expr(&binary.rhs)?),
    }))
}

fn lower_vector_selector(
    selector: &parser_promql::VectorSelector,
) -> Result<PromqlSelector, PromqlQueryError> {
    if selector.offset.is_some() || selector.at.is_some() {
        return Err(PromqlQueryError::Unsupported(
            "offset and @ modifiers are not implemented".to_string(),
        ));
    }
    if !selector.matchers.or_matchers.is_empty() {
        return Err(PromqlQueryError::Unsupported(
            "or matchers are not implemented".to_string(),
        ));
    }

    let mut metric_name = selector.name.clone();
    let mut matchers = Vec::new();
    for matcher in &selector.matchers.matchers {
        lower_matcher(matcher, &mut metric_name, &mut matchers)?;
    }

    if metric_name.is_none() && matchers.is_empty() {
        return Err(PromqlQueryError::Invalid(
            "selector must include a metric name or matcher".to_string(),
        ));
    }
    Ok(PromqlSelector {
        metric_name,
        matchers,
    })
}

fn lower_matcher(
    matcher: &promql_parser::label::Matcher,
    metric_name: &mut Option<String>,
    matchers: &mut Vec<PromqlMatcher>,
) -> Result<(), PromqlQueryError> {
    let op = match &matcher.op {
        ParserMatchOp::Equal => PromqlMatcherOp::Eq,
        ParserMatchOp::NotEqual => PromqlMatcherOp::NotEq,
        ParserMatchOp::Re(_) => PromqlMatcherOp::Regex,
        ParserMatchOp::NotRe(_) => PromqlMatcherOp::NotRegex,
    };

    if matcher.name == METRIC_NAME_LABEL {
        if op != PromqlMatcherOp::Eq && op != PromqlMatcherOp::Regex {
            return Err(PromqlQueryError::Unsupported(
                "__name__ currently supports only equality or regex".to_string(),
            ));
        }
        if let Some(existing) = metric_name
            && op == PromqlMatcherOp::Eq
            && existing != &matcher.value
        {
            return Err(PromqlQueryError::Invalid(
                "conflicting metric names".to_string(),
            ));
        }
        if op == PromqlMatcherOp::Eq {
            *metric_name = Some(matcher.value.clone());
            return Ok(());
        }
    }

    matchers.push(PromqlMatcher {
        name: matcher.name.clone(),
        op,
        value: matcher.value.clone(),
    });
    Ok(())
}

fn duration_ms(duration: Duration) -> Result<u64, PromqlQueryError> {
    u64::try_from(duration.as_millis())
        .map_err(|_| PromqlQueryError::Invalid("range duration is too large".to_string()))
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
