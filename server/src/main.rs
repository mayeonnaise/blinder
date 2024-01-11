#[macro_use]
extern crate rocket;

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
};

use blinker::{
    monitor::{Monitor, MonitorMatcher, MonitorQuery},
    presearcher::{PresearcherMetrics, TermFilteredPresearcher, TfIdfScorer},
};
use once_cell::sync::Lazy;
use rocket::{serde::json::Json, State};
use serde::{Deserialize, Serialize};
use tantivy::{
    query::QueryParser,
    schema::{Schema, TEXT},
    TantivyDocument,
};

static MONITOR: Lazy<Monitor<TermFilteredPresearcher<TfIdfScorer>>> = Lazy::new(|| {
    let mut document_schema_builder = Schema::builder();
    document_schema_builder.add_text_field("body", TEXT);
    let document_schema = document_schema_builder.build();

    let presearcher = TermFilteredPresearcher::default();
    Monitor::<TermFilteredPresearcher<TfIdfScorer>>::new(document_schema, presearcher)
});

thread_local! {
    static MONITOR_MATCHER: RefCell<tantivy::Result<MonitorMatcher<'static, TermFilteredPresearcher<TfIdfScorer>, TantivyDocument>>> = RefCell::new(MONITOR.matcher());
}

#[derive(Deserialize)]
struct SimpleMonitorQuery {
    id: u64,
    query: String,
}

#[derive(Default, Serialize)]
struct MonitorQueryMatches {
    ids: HashSet<u64>,
    metrics: PresearcherMetrics,
}

#[get("/")]
fn index() -> &'static str {
    "Hello World!"
}

#[post("/register_query", format = "application/json", data = "<query>")]
fn register_query(
    query: Json<SimpleMonitorQuery>,
    monitor: &State<&Monitor<TermFilteredPresearcher<TfIdfScorer>>>,
    query_parser: &State<QueryParser>,
) {
    let (tantivy_query, _) = query_parser.parse_query_lenient(&query.query);
    monitor
        .register_query(MonitorQuery {
            id: query.id,
            query: tantivy_query,
        })
        .unwrap();
}

#[post("/match_document", format = "application/json", data = "<document>")]
fn match_document(
    document: Json<HashMap<String, String>>,
    monitor: &State<&Monitor<TermFilteredPresearcher<TfIdfScorer>>>,
) -> Json<MonitorQueryMatches> {
    let mut tantivy_document = TantivyDocument::default();
    let schema = monitor.schema();

    for (field_name, value) in document.into_inner() {
        if let Some((field, _)) = schema.find_field(&field_name) {
            tantivy_document.add_text(field, value);
        }
    }

    let (matches, metrics) = MONITOR_MATCHER
        .with_borrow_mut(|matcher| matcher.as_mut().unwrap().match_document(tantivy_document))
        .unwrap();

    Json(MonitorQueryMatches {
        ids: matches,
        metrics,
    })
}

#[launch]
fn rocket() -> _ {
    let monitor = Lazy::force(&MONITOR);
    let query_parser = QueryParser::new(monitor.schema(), Vec::new(), monitor.tokenizers().clone());

    rocket::build()
        .manage(monitor)
        .manage(query_parser)
        .mount("/", routes![index, match_document, register_query])
}
