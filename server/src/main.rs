#[macro_use]
extern crate rocket;

use std::collections::{HashMap, HashSet};

use blinker::{
    monitor::{Monitor, MonitorQuery},
    presearcher::{TermFilteredPresearcher, TfIdfScorer},
};
use rocket::{serde::json::Json, State};
use serde::{Deserialize, Serialize};
use tantivy::{
    query::QueryParser,
    schema::{Schema, TEXT},
    TantivyDocument,
};

#[derive(Deserialize)]
pub struct SimpleMonitorQuery {
    pub id: u64,
    pub query: String,
}

#[derive(Deserialize)]
pub struct SimpleDocument(HashMap<String, String>);

#[derive(Default, Serialize)]
pub struct MonitorQueryMatches {
    pub ids: HashSet<u64>,
}

#[get("/")]
fn index() -> &'static str {
    "Hello World!"
}

#[post("/register_query", format = "application/json", data = "<query>")]
fn register_query(
    query: Json<SimpleMonitorQuery>,
    monitor: &State<Monitor<TermFilteredPresearcher<TfIdfScorer>>>,
    query_parser: &State<QueryParser>,
) {
    let (tantivy_query, _) = query_parser.parse_query_lenient(&query.query);

    dbg!(&tantivy_query);
    let _ = monitor.register_query(MonitorQuery {
        id: query.id,
        query: tantivy_query,
    });
}

#[post("/match_document", format = "application/json", data = "<document>")]
fn match_document(
    document: Json<SimpleDocument>,
    monitor: &State<Monitor<TermFilteredPresearcher<TfIdfScorer>>>,
) -> Json<MonitorQueryMatches> {
    let mut tantivy_document = TantivyDocument::default();
    let tantivy_schema = monitor.schema();
    for (field_name, value) in document.0 .0 {
        if let Some((field, _)) = tantivy_schema.find_field(&field_name) {
            tantivy_document.add_text(field, value);
        }
    }

    let matches = monitor.match_document(tantivy_document);
    Json(MonitorQueryMatches {
        ids: matches.unwrap(),
    })
}

#[launch]
fn rocket() -> _ {
    let mut document_schema_builder = Schema::builder();
    let _ = document_schema_builder.add_text_field("body", TEXT);
    let document_schema = document_schema_builder.build();

    let presearcher = TermFilteredPresearcher {
        scorer: Box::<TfIdfScorer>::default(),
    };

    let monitor =
        Monitor::<TermFilteredPresearcher<TfIdfScorer>>::new(document_schema, presearcher);

    let query_parser = QueryParser::new(monitor.schema(), Vec::new(), monitor.tokenizers().clone());

    rocket::build()
        .manage(monitor)
        .manage(query_parser)
        .mount("/", routes![index, match_document, register_query])
}
