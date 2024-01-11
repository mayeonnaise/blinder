use serde::Serialize;

#[derive(Debug, Default, PartialEq, Eq, Serialize)]
pub struct PresearcherMetrics {
    pub total_queries: usize,
    pub prospective_queries: usize,
    pub actual_matches: usize,
}
