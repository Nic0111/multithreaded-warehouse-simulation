use crate::realtime::priority::Priority;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct RecentChangeBorrowed<'a> {
    #[serde(default, borrow)]
    pub meta: Option<MetaBorrowed<'a>>,
    #[serde(default, borrow)]
    pub user: &'a str,
    #[serde(default)]
    pub bot: bool,
    #[serde(default, borrow)]
    pub server_name: &'a str,
    #[serde(default, borrow)]
    pub wiki: &'a str,
    #[serde(default, borrow, rename = "type")]
    pub event_type: &'a str,
    #[serde(default, borrow)]
    pub title: &'a str,
    #[serde(default)]
    pub id: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct MetaBorrowed<'a> {
    #[serde(default, borrow)]
    pub domain: &'a str,
    #[serde(default, borrow)]
    pub dt: &'a str,
}

impl<'a> RecentChangeBorrowed<'a> {
    pub fn priority(&self) -> Priority {
        Priority::from_bot_flag(self.bot)
    }

    pub fn domain(&self) -> &'a str {
        if !self.server_name.is_empty() {
            self.server_name
        } else if let Some(meta) = &self.meta {
            if !meta.domain.is_empty() {
                meta.domain
            } else {
                self.wiki
            }
        } else {
            self.wiki
        }
    }
}

pub fn parse_recent_change(input: &str) -> serde_json::Result<RecentChangeBorrowed<'_>> {
    serde_json::from_str(input)
}

pub fn extract_sse_json_frames(buffer: &str) -> Vec<&str> {
    buffer
        .split("\n\n")
        .filter_map(extract_sse_json_frame)
        .collect()
}

pub fn extract_sse_json_frame(frame: &str) -> Option<&str> {
    frame
        .lines()
        .find_map(|line| line.strip_prefix("data: "))
        .map(str::trim)
        .filter(|data| !data.is_empty() && *data != "[DONE]")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evidence::allocator::{reset_thread_allocations, thread_allocation_snapshot};

    const SAMPLE: &str = r#"{"meta":{"domain":"en.wikipedia.org","dt":"2026-05-05T12:00:00Z"},"user":"Alice","bot":false,"server_name":"en.wikipedia.org","wiki":"enwiki","type":"edit","title":"Real-time system","id":1}"#;

    #[test]
    fn parses_borrowed_recent_change_fields() {
        let event = parse_recent_change(SAMPLE).unwrap();
        assert_eq!(event.user, "Alice");
        assert_eq!(event.domain(), "en.wikipedia.org");
        assert_eq!(event.priority(), Priority::Human);
    }

    #[test]
    fn extracts_sse_data_frames() {
        let payload = "event: message\ndata: {\"bot\":false}\n\n:event\ndata: {\"bot\":true}\n\n";
        let frames = extract_sse_json_frames(payload);
        assert_eq!(frames, vec!["{\"bot\":false}", "{\"bot\":true}"]);
    }

    #[test]
    fn borrowed_parse_hot_path_has_no_string_allocations() {
        let _ = parse_recent_change(SAMPLE).unwrap();
        reset_thread_allocations();
        let before = thread_allocation_snapshot();
        let event = parse_recent_change(SAMPLE).unwrap();
        let domain = event.domain();
        let priority = event.priority();
        let after = thread_allocation_snapshot();
        let delta = after.delta_since(before);

        assert_eq!(domain, "en.wikipedia.org");
        assert_eq!(priority, Priority::Human);
        assert_eq!(delta.allocations, 0);
    }
}
