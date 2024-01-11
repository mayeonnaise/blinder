use std::hash::Hash;

use tantivy::{
    query::Query,
    schema::{Schema, INDEXED, STORED},
};

pub const MONITOR_QUERY_ID_FIELD_NAME: &str = "__monitor_query_id__";
pub const ANYTERM_FIELD: &str = "__anytermfield__";

pub struct MonitorQuery {
    pub id: u64,
    pub query: Box<dyn Query>,
}

impl PartialEq for MonitorQuery {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for MonitorQuery {}

impl Hash for MonitorQuery {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl Clone for MonitorQuery {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            query: self.query.box_clone(),
        }
    }
}

pub struct MonitorQuerySchemaBuilder;

impl MonitorQuerySchemaBuilder {
    pub fn build(schema: Schema) -> Schema {
        let mut schema_builder = Schema::builder();
        for (_, field_entry) in schema.fields() {
            schema_builder.add_field(field_entry.clone());
        }
        schema_builder.add_u64_field(MONITOR_QUERY_ID_FIELD_NAME, INDEXED | STORED);
        schema_builder.add_bool_field(ANYTERM_FIELD, INDEXED);
        schema_builder.build()
    }
}
