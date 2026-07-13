use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    time::Duration,
};

use promql_parser::{label::MatchOp as ParserMatchOp, parser as parser_promql};

use crate::util::xxhash64;

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
    Time,
    VectorFunction(PromqlVectorFunction),
    ScalarFunction(PromqlScalarFunction),
    Offset(PromqlOffset),
    LabelReplace(PromqlLabelReplace),
    LabelJoin(PromqlLabelJoin),
    RangeFunction(PromqlRangeFunction),
    QuantileOverTime(PromqlQuantileOverTime),
    PredictLinear(PromqlPredictLinear),
    DoubleExponentialSmoothing(PromqlDoubleExponentialSmoothing),
    Aggregation(PromqlAggregation),
    Absent(PromqlAbsent),
    AbsentOverTime(PromqlAbsentOverTime),
    InstantFunction(PromqlInstantFunction),
    HistogramQuantile(PromqlHistogramQuantile),
    HistogramFraction(PromqlHistogramFraction),
    HistogramScalarFunction(PromqlHistogramScalarFunction),
    BinaryExpression(PromqlBinaryExpression),
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromqlVectorFunction {
    pub input: Box<PromqlQuery>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromqlScalarFunction {
    pub input: Box<PromqlQuery>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromqlOffset {
    pub input: Box<PromqlQuery>,
    pub offset_ms: i128,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromqlLabelReplace {
    pub input: Box<PromqlQuery>,
    pub dst_label: String,
    pub replacement: String,
    pub src_label: String,
    pub regex: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromqlLabelJoin {
    pub input: Box<PromqlQuery>,
    pub dst_label: String,
    pub separator: String,
    pub src_labels: Vec<String>,
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
    Delta,
    Irate,
    Idelta,
    Changes,
    Resets,
    LastOverTime,
    CountOverTime,
    PresentOverTime,
    SumOverTime,
    AvgOverTime,
    StddevOverTime,
    StdvarOverTime,
    MinOverTime,
    MaxOverTime,
    Deriv,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromqlQuantileOverTime {
    pub quantile: f64,
    pub selector: PromqlSelector,
    pub range_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromqlPredictLinear {
    pub selector: PromqlSelector,
    pub range_ms: u64,
    pub seconds: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromqlDoubleExponentialSmoothing {
    pub selector: PromqlSelector,
    pub range_ms: u64,
    pub smoothing_factor: f64,
    pub trend_factor: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromqlAggregation {
    pub op: PromqlAggregationOp,
    pub grouping: PromqlAggregationGrouping,
    pub input: Box<PromqlQuery>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PromqlAggregationOp {
    Sum,
    Count,
    Avg,
    Min,
    Max,
    Stddev,
    Stdvar,
    Group,
    TopK(usize),
    BottomK(usize),
    Quantile(f64),
    CountValues(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromqlAggregationGrouping {
    All,
    By(Vec<String>),
    Without(Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromqlAbsent {
    pub labels: Vec<(String, String)>,
    pub input: Box<PromqlQuery>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromqlAbsentOverTime {
    pub labels: Vec<(String, String)>,
    pub selector: PromqlSelector,
    pub range_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromqlInstantFunction {
    pub kind: PromqlInstantFunctionKind,
    pub input: Box<PromqlQuery>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PromqlInstantFunctionKind {
    Sort,
    SortDesc,
    Abs,
    Ceil,
    Floor,
    Round { to_nearest: f64 },
    Clamp { min: Option<f64>, max: Option<f64> },
    Ln,
    Log2,
    Log10,
    Sgn,
    Acos,
    Acosh,
    Asin,
    Asinh,
    Atan,
    Atanh,
    Cos,
    Cosh,
    Sin,
    Sinh,
    Tan,
    Tanh,
    Deg,
    Rad,
    Minute,
    Hour,
    DayOfMonth,
    DayOfWeek,
    DayOfYear,
    DaysInMonth,
    Month,
    Year,
    Timestamp,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromqlHistogramQuantile {
    pub quantile: f64,
    pub input: Box<PromqlQuery>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromqlHistogramFraction {
    pub lower: f64,
    pub upper: f64,
    pub input: Box<PromqlQuery>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromqlHistogramScalarFunction {
    pub kind: PromqlHistogramScalarFunctionKind,
    pub input: Box<PromqlQuery>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromqlHistogramScalarFunctionKind {
    Count,
    Sum,
    Avg,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PromqlBinaryExpression {
    pub op: PromqlBinaryOp,
    pub return_bool: bool,
    pub vector_matching: Option<PromqlVectorMatching>,
    pub left: Box<PromqlQuery>,
    pub right: Box<PromqlQuery>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromqlVectorMatching {
    pub mode: PromqlVectorMatchingMode,
    pub labels: Vec<String>,
    pub cardinality: PromqlVectorMatchingCardinality,
    pub include_labels: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromqlVectorMatchingCardinality {
    OneToOne,
    ManyToOne,
    OneToMany,
    ManyToMany,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromqlVectorMatchingMode {
    On,
    Ignoring,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromqlBinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Eq,
    NotEq,
    Gt,
    Gte,
    Lt,
    Lte,
    And,
    Or,
    Unless,
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

pub fn format_promql_float_label(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_positive() {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        };
    }
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0".to_string()
        } else {
            "0".to_string()
        };
    }

    let raw = value.to_string();
    if raw.contains('e') || raw.contains('E') {
        return normalize_promql_exponent(&raw);
    }

    let abs = value.abs();
    if !(1e-4..1e6).contains(&abs) {
        return decimal_to_promql_scientific(&raw);
    }

    raw
}

fn decimal_to_promql_scientific(raw: &str) -> String {
    let (negative, unsigned) = raw
        .strip_prefix('-')
        .map_or((false, raw), |stripped| (true, stripped));
    let (digits, exponent) = if let Some((integer, fraction)) = unsigned.split_once('.') {
        if integer != "0" {
            (
                format!("{integer}{fraction}"),
                integer.len().saturating_sub(1) as i32,
            )
        } else if let Some(first_non_zero) = fraction.bytes().position(|value| value != b'0') {
            (
                fraction[first_non_zero..].to_string(),
                -((first_non_zero as i32) + 1),
            )
        } else {
            ("0".to_string(), 0)
        }
    } else {
        (
            unsigned.to_string(),
            unsigned.len().saturating_sub(1) as i32,
        )
    };

    format_promql_scientific(negative, digits, exponent)
}

fn normalize_promql_exponent(raw: &str) -> String {
    let Some(exponent_separator) = raw.find(['e', 'E']) else {
        return raw.to_string();
    };
    let mantissa = &raw[..exponent_separator];
    let exponent = raw[exponent_separator + 1..].parse::<i32>().unwrap_or(0);
    format!("{mantissa}{}", format_promql_exponent(exponent))
}

fn format_promql_scientific(negative: bool, digits: String, exponent: i32) -> String {
    let digits = digits.trim_end_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    let mut chars = digits.chars();
    if let Some(first) = chars.next() {
        out.push(first);
    }
    let rest = chars.as_str();
    if !rest.is_empty() {
        out.push('.');
        out.push_str(rest);
    }
    out.push_str(&format_promql_exponent(exponent));
    out
}

fn format_promql_exponent(exponent: i32) -> String {
    let sign = if exponent >= 0 { '+' } else { '-' };
    let magnitude = exponent.unsigned_abs();
    if magnitude < 10 {
        format!("e{sign}0{magnitude}")
    } else {
        format!("e{sign}{magnitude}")
    }
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

fn lower_expr(expr: &parser_promql::Expr) -> Result<PromqlQuery, PromqlQueryError> {
    match expr {
        parser_promql::Expr::VectorSelector(selector) => lower_vector_selector_query(selector),
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
        query => Ok(PromqlQuery::BinaryExpression(PromqlBinaryExpression {
            op: PromqlBinaryOp::Mul,
            return_bool: false,
            vector_matching: None,
            left: Box::new(PromqlQuery::Scalar(-1.0)),
            right: Box::new(query),
        })),
    }
}

fn lower_call(call: &parser_promql::Call) -> Result<PromqlQuery, PromqlQueryError> {
    match call.func.name {
        "rate" | "increase" | "delta" | "irate" | "idelta" | "changes" | "resets" | "deriv"
        | "last_over_time" | "count_over_time" | "present_over_time" | "sum_over_time"
        | "avg_over_time" | "stddev_over_time" | "stdvar_over_time" | "min_over_time"
        | "max_over_time" => {
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
            let kind = match call.func.name {
                "rate" => PromqlRangeFunctionKind::Rate,
                "increase" => PromqlRangeFunctionKind::Increase,
                "delta" => PromqlRangeFunctionKind::Delta,
                "irate" => PromqlRangeFunctionKind::Irate,
                "idelta" => PromqlRangeFunctionKind::Idelta,
                "changes" => PromqlRangeFunctionKind::Changes,
                "resets" => PromqlRangeFunctionKind::Resets,
                "deriv" => PromqlRangeFunctionKind::Deriv,
                "last_over_time" => PromqlRangeFunctionKind::LastOverTime,
                "count_over_time" => PromqlRangeFunctionKind::CountOverTime,
                "present_over_time" => PromqlRangeFunctionKind::PresentOverTime,
                "sum_over_time" => PromqlRangeFunctionKind::SumOverTime,
                "avg_over_time" => PromqlRangeFunctionKind::AvgOverTime,
                "stddev_over_time" => PromqlRangeFunctionKind::StddevOverTime,
                "stdvar_over_time" => PromqlRangeFunctionKind::StdvarOverTime,
                "min_over_time" => PromqlRangeFunctionKind::MinOverTime,
                "max_over_time" => PromqlRangeFunctionKind::MaxOverTime,
                _ => unreachable!("range function name matched above"),
            };
            wrap_offset(
                PromqlQuery::RangeFunction(PromqlRangeFunction {
                    kind,
                    selector: lower_vector_selector(&matrix.vs)?,
                    range_ms: duration_ms(matrix.range)?,
                }),
                matrix.vs.offset.as_ref(),
            )
        }
        "quantile_over_time" => lower_quantile_over_time(call),
        "predict_linear" => lower_predict_linear(call),
        "double_exponential_smoothing" | "holt_winters" => lower_double_exponential_smoothing(call),
        "histogram_quantile" => lower_histogram_quantile(call),
        "histogram_fraction" => lower_histogram_fraction(call),
        "absent" => lower_absent(call),
        "absent_over_time" => lower_absent_over_time(call),
        "time" => lower_time_function(call),
        "vector" => lower_vector_function(call),
        "scalar" => lower_scalar_function(call),
        "pi" => lower_pi_function(call),
        "label_replace" => lower_label_replace_function(call),
        "label_join" => lower_label_join_function(call),
        "sort" => lower_instant_function(call, PromqlInstantFunctionKind::Sort, "sort"),
        "sort_desc" => {
            lower_instant_function(call, PromqlInstantFunctionKind::SortDesc, "sort_desc")
        }
        "abs" => lower_instant_function(call, PromqlInstantFunctionKind::Abs, "abs"),
        "ceil" => lower_instant_function(call, PromqlInstantFunctionKind::Ceil, "ceil"),
        "floor" => lower_instant_function(call, PromqlInstantFunctionKind::Floor, "floor"),
        "round" => lower_round_function(call),
        "clamp" => lower_clamp_function(call, "clamp", true, true),
        "clamp_min" => lower_clamp_function(call, "clamp_min", true, false),
        "clamp_max" => lower_clamp_function(call, "clamp_max", false, true),
        "ln" => lower_instant_function(call, PromqlInstantFunctionKind::Ln, "ln"),
        "log2" => lower_instant_function(call, PromqlInstantFunctionKind::Log2, "log2"),
        "log10" => lower_instant_function(call, PromqlInstantFunctionKind::Log10, "log10"),
        "sgn" => lower_instant_function(call, PromqlInstantFunctionKind::Sgn, "sgn"),
        "acos" => lower_instant_function(call, PromqlInstantFunctionKind::Acos, "acos"),
        "acosh" => lower_instant_function(call, PromqlInstantFunctionKind::Acosh, "acosh"),
        "asin" => lower_instant_function(call, PromqlInstantFunctionKind::Asin, "asin"),
        "asinh" => lower_instant_function(call, PromqlInstantFunctionKind::Asinh, "asinh"),
        "atan" => lower_instant_function(call, PromqlInstantFunctionKind::Atan, "atan"),
        "atanh" => lower_instant_function(call, PromqlInstantFunctionKind::Atanh, "atanh"),
        "cos" => lower_instant_function(call, PromqlInstantFunctionKind::Cos, "cos"),
        "cosh" => lower_instant_function(call, PromqlInstantFunctionKind::Cosh, "cosh"),
        "sin" => lower_instant_function(call, PromqlInstantFunctionKind::Sin, "sin"),
        "sinh" => lower_instant_function(call, PromqlInstantFunctionKind::Sinh, "sinh"),
        "tan" => lower_instant_function(call, PromqlInstantFunctionKind::Tan, "tan"),
        "tanh" => lower_instant_function(call, PromqlInstantFunctionKind::Tanh, "tanh"),
        "deg" => lower_instant_function(call, PromqlInstantFunctionKind::Deg, "deg"),
        "rad" => lower_instant_function(call, PromqlInstantFunctionKind::Rad, "rad"),
        "minute" => lower_optional_time_extraction_function(
            call,
            PromqlInstantFunctionKind::Minute,
            "minute",
        ),
        "hour" => {
            lower_optional_time_extraction_function(call, PromqlInstantFunctionKind::Hour, "hour")
        }
        "day_of_month" => lower_optional_time_extraction_function(
            call,
            PromqlInstantFunctionKind::DayOfMonth,
            "day_of_month",
        ),
        "day_of_week" => lower_optional_time_extraction_function(
            call,
            PromqlInstantFunctionKind::DayOfWeek,
            "day_of_week",
        ),
        "day_of_year" => lower_optional_time_extraction_function(
            call,
            PromqlInstantFunctionKind::DayOfYear,
            "day_of_year",
        ),
        "days_in_month" => lower_optional_time_extraction_function(
            call,
            PromqlInstantFunctionKind::DaysInMonth,
            "days_in_month",
        ),
        "month" => {
            lower_optional_time_extraction_function(call, PromqlInstantFunctionKind::Month, "month")
        }
        "year" => {
            lower_optional_time_extraction_function(call, PromqlInstantFunctionKind::Year, "year")
        }
        "timestamp" => {
            lower_instant_function(call, PromqlInstantFunctionKind::Timestamp, "timestamp")
        }
        "histogram_count" => lower_histogram_scalar_function(
            call,
            PromqlHistogramScalarFunctionKind::Count,
            "histogram_count",
        ),
        "histogram_sum" => lower_histogram_scalar_function(
            call,
            PromqlHistogramScalarFunctionKind::Sum,
            "histogram_sum",
        ),
        "histogram_avg" => lower_histogram_scalar_function(
            call,
            PromqlHistogramScalarFunctionKind::Avg,
            "histogram_avg",
        ),
        other => Err(PromqlQueryError::Unsupported(format!(
            "unsupported PromQL function {other}"
        ))),
    }
}

fn lower_absent(call: &parser_promql::Call) -> Result<PromqlQuery, PromqlQueryError> {
    if call.args.len() != 1 {
        return Err(PromqlQueryError::Invalid(
            "absent expects one argument".to_string(),
        ));
    }
    let Some(arg) = call.args.args.first() else {
        return Err(PromqlQueryError::Invalid(
            "absent expects one argument".to_string(),
        ));
    };
    let labels = absent_result_labels(arg.as_ref());
    let input = lower_expr(arg.as_ref())?;
    Ok(PromqlQuery::Absent(PromqlAbsent {
        labels,
        input: Box::new(input),
    }))
}

fn lower_absent_over_time(call: &parser_promql::Call) -> Result<PromqlQuery, PromqlQueryError> {
    if call.args.len() != 1 {
        return Err(PromqlQueryError::Invalid(
            "absent_over_time expects one argument".to_string(),
        ));
    }
    let Some(arg) = call.args.args.first() else {
        return Err(PromqlQueryError::Invalid(
            "absent_over_time expects one argument".to_string(),
        ));
    };
    let parser_promql::Expr::MatrixSelector(matrix) = arg.as_ref() else {
        return Err(PromqlQueryError::Unsupported(
            "absent_over_time currently supports only selector range arguments".to_string(),
        ));
    };
    wrap_offset(
        PromqlQuery::AbsentOverTime(PromqlAbsentOverTime {
            labels: absent_result_labels(arg.as_ref()),
            selector: lower_vector_selector(&matrix.vs)?,
            range_ms: duration_ms(matrix.range)?,
        }),
        matrix.vs.offset.as_ref(),
    )
}

fn lower_time_function(call: &parser_promql::Call) -> Result<PromqlQuery, PromqlQueryError> {
    if !call.args.is_empty() {
        return Err(PromqlQueryError::Invalid(
            "time expects no arguments".to_string(),
        ));
    }
    Ok(PromqlQuery::Time)
}

fn lower_vector_function(call: &parser_promql::Call) -> Result<PromqlQuery, PromqlQueryError> {
    if call.args.len() != 1 {
        return Err(PromqlQueryError::Invalid(
            "vector expects one argument".to_string(),
        ));
    }
    let input = lower_expr(call.args.args[0].as_ref())?;
    if !scalar_query_syntax(&input) {
        return Err(PromqlQueryError::Invalid(
            "vector expects a scalar expression".to_string(),
        ));
    }
    Ok(PromqlQuery::VectorFunction(PromqlVectorFunction {
        input: Box::new(input),
    }))
}

fn lower_scalar_function(call: &parser_promql::Call) -> Result<PromqlQuery, PromqlQueryError> {
    if call.args.len() != 1 {
        return Err(PromqlQueryError::Invalid(
            "scalar expects one argument".to_string(),
        ));
    }
    Ok(PromqlQuery::ScalarFunction(PromqlScalarFunction {
        input: Box::new(lower_expr(call.args.args[0].as_ref())?),
    }))
}

fn lower_pi_function(call: &parser_promql::Call) -> Result<PromqlQuery, PromqlQueryError> {
    if !call.args.is_empty() {
        return Err(PromqlQueryError::Invalid(
            "pi expects no arguments".to_string(),
        ));
    }
    Ok(PromqlQuery::Scalar(std::f64::consts::PI))
}

fn lower_quantile_over_time(call: &parser_promql::Call) -> Result<PromqlQuery, PromqlQueryError> {
    if call.args.len() != 2 {
        return Err(PromqlQueryError::Invalid(
            "quantile_over_time expects two arguments".to_string(),
        ));
    }
    let quantile =
        lower_non_nan_scalar_expression(call.args.args[0].as_ref(), "quantile_over_time quantile")?;
    let matrix = lower_matrix_selector_argument(call.args.args[1].as_ref(), "quantile_over_time")?;
    wrap_offset(
        PromqlQuery::QuantileOverTime(PromqlQuantileOverTime {
            quantile,
            selector: lower_vector_selector(&matrix.vs)?,
            range_ms: duration_ms(matrix.range)?,
        }),
        matrix.vs.offset.as_ref(),
    )
}

fn lower_predict_linear(call: &parser_promql::Call) -> Result<PromqlQuery, PromqlQueryError> {
    if call.args.len() != 2 {
        return Err(PromqlQueryError::Invalid(
            "predict_linear expects two arguments".to_string(),
        ));
    }
    let matrix = lower_matrix_selector_argument(call.args.args[0].as_ref(), "predict_linear")?;
    let seconds =
        lower_finite_scalar_expression(call.args.args[1].as_ref(), "predict_linear seconds")?;
    wrap_offset(
        PromqlQuery::PredictLinear(PromqlPredictLinear {
            selector: lower_vector_selector(&matrix.vs)?,
            range_ms: duration_ms(matrix.range)?,
            seconds,
        }),
        matrix.vs.offset.as_ref(),
    )
}

fn lower_double_exponential_smoothing(
    call: &parser_promql::Call,
) -> Result<PromqlQuery, PromqlQueryError> {
    if call.args.len() != 3 {
        return Err(PromqlQueryError::Invalid(format!(
            "{} expects three arguments",
            call.func.name
        )));
    }
    let matrix = lower_matrix_selector_argument(call.args.args[0].as_ref(), call.func.name)?;
    let smoothing_factor = lower_smoothing_factor(
        call.args.args[1].as_ref(),
        &format!("{} smoothing factor", call.func.name),
    )?;
    let trend_factor = lower_smoothing_factor(
        call.args.args[2].as_ref(),
        &format!("{} trend factor", call.func.name),
    )?;
    wrap_offset(
        PromqlQuery::DoubleExponentialSmoothing(PromqlDoubleExponentialSmoothing {
            selector: lower_vector_selector(&matrix.vs)?,
            range_ms: duration_ms(matrix.range)?,
            smoothing_factor,
            trend_factor,
        }),
        matrix.vs.offset.as_ref(),
    )
}

fn lower_matrix_selector_argument<'a>(
    expr: &'a parser_promql::Expr,
    function_name: &str,
) -> Result<&'a parser_promql::MatrixSelector, PromqlQueryError> {
    let parser_promql::Expr::MatrixSelector(matrix) = expr else {
        return Err(PromqlQueryError::Unsupported(format!(
            "{function_name} currently supports only selector range arguments"
        )));
    };
    Ok(matrix)
}

fn lower_smoothing_factor(
    expr: &parser_promql::Expr,
    description: &str,
) -> Result<f64, PromqlQueryError> {
    let value = lower_finite_scalar_expression(expr, description)?;
    if !(0.0..=1.0).contains(&value) {
        return Err(PromqlQueryError::Invalid(format!(
            "{description} must be between 0 and 1"
        )));
    }
    Ok(value)
}

fn lower_round_function(call: &parser_promql::Call) -> Result<PromqlQuery, PromqlQueryError> {
    if call.args.is_empty() || call.args.len() > 2 {
        return Err(PromqlQueryError::Invalid(
            "round expects one or two arguments".to_string(),
        ));
    }
    let to_nearest = if call.args.len() == 2 {
        lower_finite_scalar_expression(call.args.args[1].as_ref(), "round to_nearest")?
    } else {
        1.0
    };
    Ok(PromqlQuery::InstantFunction(PromqlInstantFunction {
        kind: PromqlInstantFunctionKind::Round { to_nearest },
        input: Box::new(lower_expr(call.args.args[0].as_ref())?),
    }))
}

fn lower_clamp_function(
    call: &parser_promql::Call,
    function_name: &str,
    expects_min: bool,
    expects_max: bool,
) -> Result<PromqlQuery, PromqlQueryError> {
    let expected_args = 1 + expects_min as usize + expects_max as usize;
    if call.args.len() != expected_args {
        return Err(PromqlQueryError::Invalid(format!(
            "{function_name} expects {expected_args} arguments"
        )));
    }
    let mut next_arg = 1;
    let min = if expects_min {
        let value = lower_non_nan_scalar_expression(
            call.args.args[next_arg].as_ref(),
            &format!("{function_name} min"),
        )?;
        next_arg += 1;
        Some(value)
    } else {
        None
    };
    let max = if expects_max {
        Some(lower_non_nan_scalar_expression(
            call.args.args[next_arg].as_ref(),
            &format!("{function_name} max"),
        )?)
    } else {
        None
    };
    Ok(PromqlQuery::InstantFunction(PromqlInstantFunction {
        kind: PromqlInstantFunctionKind::Clamp { min, max },
        input: Box::new(lower_expr(call.args.args[0].as_ref())?),
    }))
}

fn lower_optional_time_extraction_function(
    call: &parser_promql::Call,
    kind: PromqlInstantFunctionKind,
    function_name: &str,
) -> Result<PromqlQuery, PromqlQueryError> {
    if call.args.len() > 1 {
        return Err(PromqlQueryError::Invalid(format!(
            "{function_name} expects zero or one arguments"
        )));
    }
    let input = if call.args.is_empty() {
        PromqlQuery::VectorFunction(PromqlVectorFunction {
            input: Box::new(PromqlQuery::Time),
        })
    } else {
        lower_expr(call.args.args[0].as_ref())?
    };
    Ok(PromqlQuery::InstantFunction(PromqlInstantFunction {
        kind,
        input: Box::new(input),
    }))
}

fn lower_label_replace_function(
    call: &parser_promql::Call,
) -> Result<PromqlQuery, PromqlQueryError> {
    if call.args.len() != 5 {
        return Err(PromqlQueryError::Invalid(
            "label_replace expects five arguments".to_string(),
        ));
    }
    let regex = lower_string_argument(call.args.args[4].as_ref(), "label_replace regex")?;
    regex::Regex::new(&regex).map_err(|err| {
        PromqlQueryError::Invalid(format!("label_replace regex is invalid: {err}"))
    })?;
    Ok(PromqlQuery::LabelReplace(PromqlLabelReplace {
        input: Box::new(lower_expr(call.args.args[0].as_ref())?),
        dst_label: lower_label_name_argument(
            call.args.args[1].as_ref(),
            "label_replace destination label",
        )?,
        replacement: lower_string_argument(
            call.args.args[2].as_ref(),
            "label_replace replacement",
        )?,
        src_label: lower_label_name_argument(
            call.args.args[3].as_ref(),
            "label_replace source label",
        )?,
        regex,
    }))
}

fn lower_label_join_function(call: &parser_promql::Call) -> Result<PromqlQuery, PromqlQueryError> {
    if call.args.len() < 4 {
        return Err(PromqlQueryError::Invalid(
            "label_join expects at least four arguments".to_string(),
        ));
    }
    let mut src_labels = Vec::with_capacity(call.args.len().saturating_sub(3));
    for arg in call.args.args.iter().skip(3) {
        src_labels.push(lower_label_name_argument(
            arg.as_ref(),
            "label_join source label",
        )?);
    }
    Ok(PromqlQuery::LabelJoin(PromqlLabelJoin {
        input: Box::new(lower_expr(call.args.args[0].as_ref())?),
        dst_label: lower_label_name_argument(
            call.args.args[1].as_ref(),
            "label_join destination label",
        )?,
        separator: lower_string_argument(call.args.args[2].as_ref(), "label_join separator")?,
        src_labels,
    }))
}

fn lower_instant_function(
    call: &parser_promql::Call,
    kind: PromqlInstantFunctionKind,
    function_name: &str,
) -> Result<PromqlQuery, PromqlQueryError> {
    if call.args.len() != 1 {
        return Err(PromqlQueryError::Invalid(format!(
            "{function_name} expects one argument"
        )));
    }
    let Some(arg) = call.args.args.first() else {
        return Err(PromqlQueryError::Invalid(format!(
            "{function_name} expects one argument"
        )));
    };
    Ok(PromqlQuery::InstantFunction(PromqlInstantFunction {
        kind,
        input: Box::new(lower_expr(arg.as_ref())?),
    }))
}

fn absent_result_labels(expr: &parser_promql::Expr) -> Vec<(String, String)> {
    let matchers = match expr {
        parser_promql::Expr::VectorSelector(selector) => &selector.matchers.matchers,
        parser_promql::Expr::MatrixSelector(matrix) => &matrix.vs.matchers.matchers,
        _ => return Vec::new(),
    };

    let mut labels = BTreeMap::new();
    let mut seen = BTreeSet::new();
    for matcher in matchers {
        if matcher.name == METRIC_NAME_LABEL {
            continue;
        }
        let label_name = normalize_label_name(&matcher.name);
        if matches!(&matcher.op, ParserMatchOp::Equal) && !seen.contains(&label_name) {
            labels.insert(label_name.clone(), matcher.value.clone());
            seen.insert(label_name);
        } else {
            labels.remove(&label_name);
            seen.insert(label_name);
        }
    }
    labels.into_iter().collect()
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

fn lower_histogram_fraction(call: &parser_promql::Call) -> Result<PromqlQuery, PromqlQueryError> {
    if call.args.len() != 3 {
        return Err(PromqlQueryError::Invalid(
            "histogram_fraction expects three arguments".to_string(),
        ));
    }
    let lower =
        lower_non_nan_scalar_expression(call.args.args[0].as_ref(), "histogram_fraction lower")?;
    let upper =
        lower_non_nan_scalar_expression(call.args.args[1].as_ref(), "histogram_fraction upper")?;
    let input = lower_expr(call.args.args[2].as_ref())?;
    Ok(PromqlQuery::HistogramFraction(PromqlHistogramFraction {
        lower,
        upper,
        input: Box::new(input),
    }))
}

fn lower_histogram_scalar_function(
    call: &parser_promql::Call,
    kind: PromqlHistogramScalarFunctionKind,
    function_name: &str,
) -> Result<PromqlQuery, PromqlQueryError> {
    if call.args.len() != 1 {
        return Err(PromqlQueryError::Invalid(format!(
            "{function_name} expects one argument"
        )));
    }
    let input = lower_expr(call.args.args[0].as_ref())?;
    Ok(PromqlQuery::HistogramScalarFunction(
        PromqlHistogramScalarFunction {
            kind,
            input: Box::new(input),
        },
    ))
}

fn lower_quantile_argument(expr: &parser_promql::Expr) -> Result<f64, PromqlQueryError> {
    lower_finite_scalar_expression(expr, "histogram_quantile quantile")
}

fn lower_finite_scalar_expression(
    expr: &parser_promql::Expr,
    description: &str,
) -> Result<f64, PromqlQueryError> {
    let Some(value) = constant_scalar_expression(expr, description)? else {
        return Err(PromqlQueryError::Invalid(format!(
            "{description} must be a finite scalar expression"
        )));
    };
    if value.is_finite() {
        Ok(value)
    } else {
        Err(PromqlQueryError::Invalid(format!(
            "{description} must be finite"
        )))
    }
}

fn lower_non_nan_scalar_expression(
    expr: &parser_promql::Expr,
    description: &str,
) -> Result<f64, PromqlQueryError> {
    let Some(value) = constant_scalar_expression(expr, description)? else {
        return Err(PromqlQueryError::Invalid(format!(
            "{description} must be a non-NaN scalar expression"
        )));
    };
    if !value.is_nan() {
        Ok(value)
    } else {
        Err(PromqlQueryError::Invalid(format!(
            "{description} must not be NaN"
        )))
    }
}

fn constant_scalar_expression(
    expr: &parser_promql::Expr,
    description: &str,
) -> Result<Option<f64>, PromqlQueryError> {
    lower_expr(expr)
        .map(|query| constant_scalar_value(&query))
        .map_err(|_| {
            PromqlQueryError::Invalid(format!("{description} must be a scalar expression"))
        })
}

fn constant_scalar_value(query: &PromqlQuery) -> Option<f64> {
    match query {
        PromqlQuery::Scalar(value) => Some(*value),
        PromqlQuery::BinaryExpression(binary)
            if !binary.return_bool && binary.vector_matching.is_none() =>
        {
            let left = constant_scalar_value(&binary.left)?;
            let right = constant_scalar_value(&binary.right)?;
            let value = match binary.op {
                PromqlBinaryOp::Add => left + right,
                PromqlBinaryOp::Sub => left - right,
                PromqlBinaryOp::Mul => left * right,
                PromqlBinaryOp::Div => left / right,
                PromqlBinaryOp::Mod => left % right,
                PromqlBinaryOp::Pow => left.powf(right),
                PromqlBinaryOp::Eq
                | PromqlBinaryOp::NotEq
                | PromqlBinaryOp::Gt
                | PromqlBinaryOp::Gte
                | PromqlBinaryOp::Lt
                | PromqlBinaryOp::Lte
                | PromqlBinaryOp::And
                | PromqlBinaryOp::Or
                | PromqlBinaryOp::Unless => return None,
            };
            Some(value)
        }
        _ => None,
    }
}

fn scalar_query_syntax(query: &PromqlQuery) -> bool {
    match query {
        PromqlQuery::Scalar(_) | PromqlQuery::Time | PromqlQuery::ScalarFunction(_) => true,
        PromqlQuery::BinaryExpression(binary)
            if !binary.return_bool && binary.vector_matching.is_none() =>
        {
            scalar_query_syntax(&binary.left) && scalar_query_syntax(&binary.right)
        }
        _ => false,
    }
}

fn lower_aggregation(
    aggregation: &parser_promql::AggregateExpr,
) -> Result<PromqlQuery, PromqlQueryError> {
    let op_name = aggregation.op.to_string();
    let op = match op_name.as_str() {
        "sum" => PromqlAggregationOp::Sum,
        "count" => PromqlAggregationOp::Count,
        "avg" => PromqlAggregationOp::Avg,
        "min" => PromqlAggregationOp::Min,
        "max" => PromqlAggregationOp::Max,
        "stddev" => PromqlAggregationOp::Stddev,
        "stdvar" => PromqlAggregationOp::Stdvar,
        "group" => PromqlAggregationOp::Group,
        "topk" => PromqlAggregationOp::TopK(lower_aggregation_limit_argument(
            aggregation.param.as_deref(),
            "topk",
        )?),
        "bottomk" => PromqlAggregationOp::BottomK(lower_aggregation_limit_argument(
            aggregation.param.as_deref(),
            "bottomk",
        )?),
        "quantile" => PromqlAggregationOp::Quantile(lower_aggregation_quantile_argument(
            aggregation.param.as_deref(),
        )?),
        "count_values" => PromqlAggregationOp::CountValues(lower_aggregation_label_argument(
            aggregation.param.as_deref(),
        )?),
        other => {
            return Err(PromqlQueryError::Unsupported(format!(
                "unsupported aggregation operator {other}"
            )));
        }
    };
    if aggregation.param.is_some()
        && !matches!(
            &op,
            PromqlAggregationOp::TopK(_)
                | PromqlAggregationOp::BottomK(_)
                | PromqlAggregationOp::Quantile(_)
                | PromqlAggregationOp::CountValues(_)
        )
    {
        return Err(PromqlQueryError::Unsupported(
            "aggregation parameters are not implemented".to_string(),
        ));
    }
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

fn lower_aggregation_limit_argument(
    expr: Option<&parser_promql::Expr>,
    op_name: &str,
) -> Result<usize, PromqlQueryError> {
    let Some(expr) = expr else {
        return Err(PromqlQueryError::Invalid(format!(
            "{op_name} expects a non-negative integer parameter"
        )));
    };
    let value = lower_finite_scalar_expression(expr, &format!("{op_name} parameter"))?;
    if value < 0.0 || value.fract() != 0.0 || value > usize::MAX as f64 {
        return Err(PromqlQueryError::Invalid(format!(
            "{op_name} parameter must be a non-negative integer"
        )));
    }
    Ok(value as usize)
}

fn lower_aggregation_quantile_argument(
    expr: Option<&parser_promql::Expr>,
) -> Result<f64, PromqlQueryError> {
    let Some(expr) = expr else {
        return Err(PromqlQueryError::Invalid(
            "quantile expects a scalar parameter".to_string(),
        ));
    };
    lower_finite_scalar_expression(expr, "quantile parameter")
}

fn lower_aggregation_label_argument(
    expr: Option<&parser_promql::Expr>,
) -> Result<String, PromqlQueryError> {
    let Some(expr) = expr else {
        return Err(PromqlQueryError::Invalid(
            "count_values expects a label-name parameter".to_string(),
        ));
    };
    let parser_promql::Expr::StringLiteral(label) = expr else {
        return Err(PromqlQueryError::Invalid(
            "count_values label parameter must be a string literal".to_string(),
        ));
    };
    if label.val.is_empty() {
        return Err(PromqlQueryError::Invalid(format!(
            "count_values label parameter must be a valid PromQL label name: {:?}",
            label.val
        )));
    }
    Ok(normalize_label_name(&label.val))
}

fn lower_string_argument(
    expr: &parser_promql::Expr,
    description: &str,
) -> Result<String, PromqlQueryError> {
    let parser_promql::Expr::StringLiteral(value) = expr else {
        return Err(PromqlQueryError::Invalid(format!(
            "{description} must be a string literal"
        )));
    };
    Ok(value.val.clone())
}

fn lower_label_name_argument(
    expr: &parser_promql::Expr,
    description: &str,
) -> Result<String, PromqlQueryError> {
    let value = lower_string_argument(expr, description)?;
    if value.is_empty() {
        return Err(PromqlQueryError::Invalid(format!(
            "{description} must not be empty"
        )));
    }
    Ok(normalize_label_name(&value))
}

fn lower_binary_expression(
    binary: &parser_promql::BinaryExpr,
) -> Result<PromqlQuery, PromqlQueryError> {
    let op = match binary.op.to_string().as_str() {
        "+" => PromqlBinaryOp::Add,
        "-" => PromqlBinaryOp::Sub,
        "*" => PromqlBinaryOp::Mul,
        "/" => PromqlBinaryOp::Div,
        "%" => PromqlBinaryOp::Mod,
        "^" => PromqlBinaryOp::Pow,
        "==" => PromqlBinaryOp::Eq,
        "!=" => PromqlBinaryOp::NotEq,
        ">" => PromqlBinaryOp::Gt,
        ">=" => PromqlBinaryOp::Gte,
        "<" => PromqlBinaryOp::Lt,
        "<=" => PromqlBinaryOp::Lte,
        "and" => PromqlBinaryOp::And,
        "or" => PromqlBinaryOp::Or,
        "unless" => PromqlBinaryOp::Unless,
        other => {
            return Err(PromqlQueryError::Unsupported(format!(
                "unsupported binary operator {other}"
            )));
        }
    };
    let mut return_bool = false;
    let mut vector_matching = None;
    if let Some(modifier) = &binary.modifier {
        if modifier.fill_values.lhs.is_some() || modifier.fill_values.rhs.is_some() {
            return Err(PromqlQueryError::Unsupported(
                "binary fill modifiers are not implemented".to_string(),
            ));
        }
        let set_operator = matches!(
            op,
            PromqlBinaryOp::And | PromqlBinaryOp::Or | PromqlBinaryOp::Unless
        );
        if set_operator {
            if modifier.return_bool {
                return Err(PromqlQueryError::Unsupported(
                    "bool modifier requires a comparison operator".to_string(),
                ));
            }
            if !matches!(
                &modifier.card,
                parser_promql::VectorMatchCardinality::ManyToMany
            ) {
                return Err(PromqlQueryError::Unsupported(
                    "set operators support only many-to-many matching".to_string(),
                ));
            }
            vector_matching = lower_binary_vector_matching(
                modifier.matching.as_ref(),
                PromqlVectorMatchingCardinality::ManyToMany,
                Vec::new(),
            );
        } else {
            if modifier.return_bool {
                if !matches!(
                    op,
                    PromqlBinaryOp::Eq
                        | PromqlBinaryOp::NotEq
                        | PromqlBinaryOp::Gt
                        | PromqlBinaryOp::Gte
                        | PromqlBinaryOp::Lt
                        | PromqlBinaryOp::Lte
                ) {
                    return Err(PromqlQueryError::Unsupported(
                        "bool modifier requires a comparison operator".to_string(),
                    ));
                }
                return_bool = true;
            }
            let (cardinality, include_labels) =
                lower_binary_vector_matching_cardinality(&modifier.card)?;
            vector_matching = lower_binary_vector_matching(
                modifier.matching.as_ref(),
                cardinality,
                include_labels,
            );
        }
    }
    Ok(PromqlQuery::BinaryExpression(PromqlBinaryExpression {
        op,
        return_bool,
        vector_matching,
        left: Box::new(lower_expr(&binary.lhs)?),
        right: Box::new(lower_expr(&binary.rhs)?),
    }))
}

fn lower_binary_vector_matching(
    matching: Option<&parser_promql::LabelModifier>,
    cardinality: PromqlVectorMatchingCardinality,
    include_labels: Vec<String>,
) -> Option<PromqlVectorMatching> {
    let (mode, labels) = match matching {
        Some(parser_promql::LabelModifier::Include(labels)) => (
            PromqlVectorMatchingMode::On,
            lower_label_names(&labels.labels),
        ),
        Some(parser_promql::LabelModifier::Exclude(labels)) => (
            PromqlVectorMatchingMode::Ignoring,
            lower_label_names(&labels.labels),
        ),
        None if matches!(
            cardinality,
            PromqlVectorMatchingCardinality::OneToOne | PromqlVectorMatchingCardinality::ManyToMany
        ) =>
        {
            return None;
        }
        None => (PromqlVectorMatchingMode::Ignoring, Vec::new()),
    };
    Some(PromqlVectorMatching {
        mode,
        labels,
        cardinality,
        include_labels,
    })
}

fn lower_binary_vector_matching_cardinality(
    cardinality: &parser_promql::VectorMatchCardinality,
) -> Result<(PromqlVectorMatchingCardinality, Vec<String>), PromqlQueryError> {
    match cardinality {
        parser_promql::VectorMatchCardinality::OneToOne => {
            Ok((PromqlVectorMatchingCardinality::OneToOne, Vec::new()))
        }
        parser_promql::VectorMatchCardinality::ManyToOne(labels) => Ok((
            PromqlVectorMatchingCardinality::ManyToOne,
            lower_label_names(&labels.labels),
        )),
        parser_promql::VectorMatchCardinality::OneToMany(labels) => Ok((
            PromqlVectorMatchingCardinality::OneToMany,
            lower_label_names(&labels.labels),
        )),
        parser_promql::VectorMatchCardinality::ManyToMany => Err(PromqlQueryError::Unsupported(
            "many-to-many vector matching is supported only for set operators".to_string(),
        )),
    }
}

fn lower_label_names(labels: &[String]) -> Vec<String> {
    labels
        .iter()
        .map(|label| {
            if label == METRIC_NAME_LABEL {
                METRIC_NAME_LABEL.to_string()
            } else {
                normalize_label_name(label)
            }
        })
        .collect()
}

fn lower_vector_selector(
    selector: &parser_promql::VectorSelector,
) -> Result<PromqlSelector, PromqlQueryError> {
    if selector.at.is_some() {
        return Err(PromqlQueryError::Unsupported(
            "@ modifiers are not implemented".to_string(),
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

fn lower_vector_selector_query(
    selector: &parser_promql::VectorSelector,
) -> Result<PromqlQuery, PromqlQueryError> {
    let query = PromqlQuery::Vector(lower_vector_selector(selector)?);
    wrap_offset(query, selector.offset.as_ref())
}

fn wrap_offset(
    input: PromqlQuery,
    offset: Option<&parser_promql::Offset>,
) -> Result<PromqlQuery, PromqlQueryError> {
    let Some(offset) = offset else {
        return Ok(input);
    };
    Ok(PromqlQuery::Offset(PromqlOffset {
        input: Box::new(input),
        offset_ms: offset_millis(offset)?,
    }))
}

fn offset_millis(offset: &parser_promql::Offset) -> Result<i128, PromqlQueryError> {
    let millis = match offset {
        parser_promql::Offset::Pos(duration) => duration_ms_i128(*duration)?,
        parser_promql::Offset::Neg(duration) => -duration_ms_i128(*duration)?,
    };
    Ok(millis)
}

fn duration_ms_i128(duration: Duration) -> Result<i128, PromqlQueryError> {
    i128::try_from(duration.as_millis())
        .map_err(|_| PromqlQueryError::Invalid("duration is too large".to_string()))
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
