mod list;
mod monitor;
mod monitor_query;
mod query_decomposer;

pub use query_decomposer::QueryDecomposer;
use tantivy::{
    query::{BooleanQuery, Occur, Query, QueryClone, TermQuery},
    schema::{IndexRecordOption, Schema, TEXT},
    Term,
};

fn main() {
    let mut schema_builder = Schema::builder();
    let title = schema_builder.add_text_field("title", TEXT);
    let girl_term_query: Box<dyn Query> = Box::new(TermQuery::new(
        Term::from_field_text(title, "girl"),
        IndexRecordOption::Basic,
    ));
    let diary_term_query: Box<dyn Query> = Box::new(TermQuery::new(
        Term::from_field_text(title, "diary"),
        IndexRecordOption::Basic,
    ));
    let queries_with_occurs1 = vec![
        (Occur::Must, diary_term_query),
        (Occur::MustNot, girl_term_query),
    ];
    let diary_must_and_girl_mustnot = Box::new(BooleanQuery::new(queries_with_occurs1));

    let mut all_subqueries = Vec::<Box<dyn Query>>::new();

    for _n in 1..1000000 {
        let mut query_decomposer = QueryDecomposer::new(&mut all_subqueries);
        query_decomposer.decompose(diary_must_and_girl_mustnot.box_clone());
        all_subqueries.clear();
    }
}
