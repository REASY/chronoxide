use std::time::Duration;

pub(super) fn format_duration(duration: Duration) -> String {
    format!("{duration:?}")
}

pub(super) fn format_end_ms(end_ms: u64) -> String {
    if end_ms == u64::MAX {
        "max".to_string()
    } else {
        end_ms.to_string()
    }
}

pub(super) fn format_query_limit(limit: Option<u64>) -> String {
    limit
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unlimited".to_string())
}

pub(super) fn markdown_escape_inline(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('`', "\\`")
        .replace('|', "\\|")
        .replace(['\n', '\r'], " ")
}
