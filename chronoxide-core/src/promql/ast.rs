use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalLabel {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalLabelSet {
    pub(super) labels: Vec<CanonicalLabel>,
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
