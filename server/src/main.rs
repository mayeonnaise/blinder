#[macro_use] extern crate rocket;

use std::collections::HashSet;

use blinker::{monitor::{Monitor, BasicStatisticsProvider, MonitorQuery}, presearcher::TermFilteredPresearcher};
use rocket::{serde::json::Json, State};
use serde::{Deserialize, Serialize};
use tantivy::{TantivyDocument, schema::{Schema, TEXT}, query::QueryParser};

#[derive(Deserialize)]
pub struct SimpleMonitorQuery {
    pub id: u64,
    pub query: String
}

#[derive(Default, Serialize)]
pub struct MonitorQueryMatches {
    pub ids: HashSet<u64>,
}


#[get("/")]
fn index() -> &'static str {
    "Hello, world!"
}

#[post("/register_query", format = "application/json", data = "<query>")]
fn register_query(query: Json<SimpleMonitorQuery>, monitor: &State<Monitor<TermFilteredPresearcher>>, query_parser: &State<QueryParser>) {
    let (tantivy_query, _) = query_parser.parse_query_lenient(&query.query); 
    let _ = monitor.register_query(MonitorQuery{
        id: query.id,
        query: tantivy_query
    });
}

#[post("/match_document", format = "application/json", data = "<document>")]
fn match_document(document: Json<TantivyDocument>, monitor: &State<Monitor<TermFilteredPresearcher>>) -> Json<MonitorQueryMatches> {
    let matches = monitor.match_document(&document.0);
    Json(MonitorQueryMatches{ ids: matches.unwrap() })
}

#[launch]
fn rocket() -> _ {
    let mut document_schema_builder = Schema::builder();
    let _ = document_schema_builder.add_text_field("body", TEXT);
    let document_schema = document_schema_builder.build();

    let presearcher = TermFilteredPresearcher {
        scorer: Box::new(BasicStatisticsProvider::default()),
    };

    let monitor = Monitor::<TermFilteredPresearcher>::new(document_schema, presearcher);

    let query_parser = QueryParser::new(monitor.schema(), Vec::new(), monitor.tokenizers().clone());
        
    rocket::build().manage(monitor).manage(query_parser).mount("/", routes![index, match_document, register_query])
}
