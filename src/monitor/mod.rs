pub(crate) mod query;

mod matcher;

pub use self::matcher::MonitorMatcher;
pub use self::query::MonitorQuery;

use dashmap::DashMap;

use std::collections::{HashMap, HashSet};

use tantivy::{
    query::Query,
    schema::{Field, OwnedValue, Schema},
    tokenizer::TokenizerManager,
    Document, Index, IndexWriter, TantivyError,
};

use crate::{
    presearcher::{Presearcher, PresearcherMetrics},
    query_decomposer::QueryDecomposer,
};

use self::query::{MonitorQuerySchemaBuilder, MONITOR_QUERY_ID_FIELD_NAME};

pub struct Monitor<P: Presearcher> {
    query_index: Index,
    query_store: DashMap<u64, MonitorQuery>,
    presearcher: P,
    document_schema: Schema,
}

impl<P: Presearcher> Monitor<P> {
    pub fn new(document_schema: Schema, presearcher: P) -> Monitor<P> {
        let schema = MonitorQuerySchemaBuilder::build(document_schema.clone());
        let query_index = Index::create_in_ram(schema);
        Monitor::<P> {
            query_index,
            query_store: DashMap::default(),
            presearcher,
            document_schema,
        }
    }

    pub fn tokenizers(&self) -> &TokenizerManager {
        self.query_index.tokenizers()
    }

    pub fn schema(&self) -> Schema {
        self.document_schema.clone()
    }

    pub fn matcher<D: Document>(&self) -> tantivy::Result<MonitorMatcher<'_, P, D>> {
        MonitorMatcher::new(self)
    }

    pub fn match_document(
        &self,
        document: impl Document,
    ) -> tantivy::Result<(HashSet<u64>, PresearcherMetrics)> {
        self.matcher()?.match_document(document)
    }

    pub fn register_query(&self, monitor_query: MonitorQuery) -> Result<u64, TantivyError> {
        let mut all_subqueries = Vec::<Box<dyn Query>>::new();
        let mut query_decomposer = QueryDecomposer::new(&mut all_subqueries);
        query_decomposer.decompose(monitor_query.query.box_clone());

        let mut index_writer: IndexWriter<HashMap<Field, OwnedValue>> =
            self.query_index.writer(100_000_000)?;

        for subquery in all_subqueries {
            let mut subquery_document = self
                .presearcher
                .convert_query_to_document(&subquery, self.query_index.schema())?;
            subquery_document.insert(
                self.query_index
                    .schema()
                    .get_field(MONITOR_QUERY_ID_FIELD_NAME)?,
                OwnedValue::U64(monitor_query.id),
            );

            index_writer.add_document(subquery_document)?;
        }

        self.query_store.insert(monitor_query.id, monitor_query);

        index_writer.commit()
    }
}

#[cfg(test)]
mod test {
    use tantivy::{
        doc,
        query::{BooleanQuery, TermQuery},
        query_grammar::Occur,
        schema::{IndexRecordOption, Schema, TEXT},
        Term,
    };

    use crate::presearcher::{TermFilteredPresearcher, TfIdfScorer};

    use super::{Monitor, *};

    #[test]
    fn test_monitor_basic() {
        let mut document_schema_builder = Schema::builder();
        let body = document_schema_builder.add_text_field("body", TEXT);
        let document_schema = document_schema_builder.build();

        let presearcher = TermFilteredPresearcher {
            scorer: Box::<TfIdfScorer>::default(),
        };

        let monitor =
            Monitor::<TermFilteredPresearcher<TfIdfScorer>>::new(document_schema, presearcher);

        let monitor_query = MonitorQuery {
            id: 0,
            query: Box::new(TermQuery::new(
                Term::from_field_text(body, "bloomberg"),
                IndexRecordOption::Basic,
            )),
        };

        let _ = monitor
            .register_query(monitor_query)
            .expect("Should not error registering query");

        let document = doc!(body => "Michael Bloomberg");

        let (matches, metrics) = monitor
            .match_document(document)
            .expect("Should not error matching document");

        assert_eq!(matches, HashSet::from_iter([0]));
        assert_eq!(
            metrics,
            PresearcherMetrics {
                total_queries: 1,
                prospective_queries: 1,
                actual_matches: 1,
            }
        );

        let document = doc!(body => "Michael Bay");

        let (matches, metrics) = monitor
            .match_document(document)
            .expect("Should not error matching document");

        assert!(matches.is_empty());
        assert_eq!(
            metrics,
            PresearcherMetrics {
                total_queries: 1,
                prospective_queries: 0,
                actual_matches: 0,
            }
        );
    }

    #[test]
    fn test_monitor_boolean_query() {
        let mut document_schema_builder = Schema::builder();
        let body = document_schema_builder.add_text_field("body", TEXT);
        let document_schema = document_schema_builder.build();

        let presearcher = TermFilteredPresearcher {
            scorer: Box::<TfIdfScorer>::default(),
        };

        let monitor =
            Monitor::<TermFilteredPresearcher<TfIdfScorer>>::new(document_schema, presearcher);

        let monitor_query = MonitorQuery {
            id: 0,
            query: Box::new(BooleanQuery::new(vec![
                (
                    Occur::Should,
                    Box::new(TermQuery::new(
                        Term::from_field_text(body, "trump"),
                        IndexRecordOption::Basic,
                    )),
                ),
                (
                    Occur::Should,
                    Box::new(TermQuery::new(
                        Term::from_field_text(body, "bloomberg"),
                        IndexRecordOption::Basic,
                    )),
                ),
            ])),
        };

        monitor
            .register_query(monitor_query)
            .expect("should not error registering query");

        let document = doc!(body => "Michael Bloomberg");

        let (matches, metrics) = monitor
            .match_document(document)
            .expect("should not error matching document");

        assert_eq!(matches, HashSet::from_iter([0]));
        assert_eq!(
            metrics,
            PresearcherMetrics {
                total_queries: 1,
                prospective_queries: 1,
                actual_matches: 1,
            }
        );

        let document = doc!(body => "Donald Trump");

        let (matches, metrics) = monitor
            .match_document(document)
            .expect("should not error matching document");

        assert_eq!(matches, HashSet::from_iter([0]));
        assert_eq!(
            metrics,
            PresearcherMetrics {
                total_queries: 1,
                prospective_queries: 1,
                actual_matches: 1,
            }
        );

        let document = doc!(body => "Bloomberg Trump");

        let (matches, metrics) = monitor
            .match_document(document)
            .expect("should not error matching document");

        assert_eq!(matches, HashSet::from_iter([0]));
        assert_eq!(
            metrics,
            PresearcherMetrics {
                total_queries: 1,
                prospective_queries: 1,
                actual_matches: 1,
            }
        );

        let document = doc!(body => "Rishi Sunak");

        let (matches, metrics) = monitor
            .match_document(document)
            .expect("should not error matching document");

        assert!(matches.is_empty());
        assert_eq!(
            metrics,
            PresearcherMetrics {
                total_queries: 1,
                prospective_queries: 0,
                actual_matches: 0,
            }
        );
    }

    #[test]
    fn test_monitor_multiple_queries() {
        let mut document_schema_builder = Schema::builder();
        let body = document_schema_builder.add_text_field("body", TEXT);
        let document_schema = document_schema_builder.build();

        let presearcher = TermFilteredPresearcher {
            scorer: Box::<TfIdfScorer>::default(),
        };

        let monitor =
            Monitor::<TermFilteredPresearcher<TfIdfScorer>>::new(document_schema, presearcher);

        let document = doc!(body => "Michael is a common name");

        let _ = monitor
            .match_document(document)
            .expect("Should not error matching document");

        let monitor_query = MonitorQuery {
            id: 0,
            query: Box::new(BooleanQuery::new(vec![
                (
                    Occur::Must,
                    Box::new(TermQuery::new(
                        Term::from_field_text(body, "michael"),
                        IndexRecordOption::Basic,
                    )),
                ),
                (
                    Occur::Must,
                    Box::new(TermQuery::new(
                        Term::from_field_text(body, "bloomberg"),
                        IndexRecordOption::Basic,
                    )),
                ),
            ])),
        };

        let _ = monitor
            .register_query(monitor_query)
            .expect("Should not error registering query");

        let document = doc!(body => "Michael is a common name");

        let (matches, metrics) = monitor
            .match_document(document)
            .expect("Should not error matching document");

        assert!(matches.is_empty());
        assert_eq!(
            metrics,
            PresearcherMetrics {
                total_queries: 1,
                prospective_queries: 0,
                actual_matches: 0,
            }
        );

        let monitor_query = MonitorQuery {
            id: 1,
            query: Box::new(BooleanQuery::new(vec![
                (
                    Occur::Must,
                    Box::new(TermQuery::new(
                        Term::from_field_text(body, "michael"),
                        IndexRecordOption::Basic,
                    )),
                ),
                (
                    Occur::Must,
                    Box::new(TermQuery::new(
                        Term::from_field_text(body, "bay"),
                        IndexRecordOption::Basic,
                    )),
                ),
            ])),
        };

        let _ = monitor
            .register_query(monitor_query)
            .expect("Should not error registering query");

        let monitor_query = MonitorQuery {
            id: 2,
            query: Box::new(BooleanQuery::new(vec![
                (
                    Occur::Must,
                    Box::new(TermQuery::new(
                        Term::from_field_text(body, "michael"),
                        IndexRecordOption::Basic,
                    )),
                ),
                (
                    Occur::Must,
                    Box::new(TermQuery::new(
                        Term::from_field_text(body, "jackson"),
                        IndexRecordOption::Basic,
                    )),
                ),
            ])),
        };

        let _ = monitor
            .register_query(monitor_query)
            .expect("Should not error registering query");

        let document = doc!(body => "Michael Bloomberg runs for mayor of New York");

        let (matches, metrics) = monitor
            .match_document(document)
            .expect("Should not error matching document");

        assert_eq!(matches, HashSet::from_iter([0]));
        assert_eq!(
            metrics,
            PresearcherMetrics {
                total_queries: 3,
                prospective_queries: 1,
                actual_matches: 1,
            }
        );
    }
}
