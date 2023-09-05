use std::collections::HashMap;

use tantivy::query::Query;

pub struct MonitorQuery {
    id: String,
    query: Box<dyn Query>,
    query_string: String,
    metadata: HashMap<String, String>,
}
