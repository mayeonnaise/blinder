pub(crate) mod query;

pub use self::query::MonitorQuery;

use dashmap::DashMap;

use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
};

use tantivy::{
    collector::{Collector, SegmentCollector},
    query::Query,
    schema::{Field, OwnedValue, Schema},
    tokenizer::TokenizerManager,
    DocAddress, DocId, Document, Index, IndexWriter, Searcher, TantivyDocument, TantivyError,
};

use crate::{presearcher::Presearcher, query_decomposer::QueryDecomposer};

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

    pub fn match_document<D: Clone + Debug + Document>(
        &self,
        document: D,
    ) -> Result<HashSet<u64>, TantivyError> {
        let query_reader = self.query_index.reader()?;
        let query_searcher = query_reader.searcher();

        let document_query = self.presearcher.convert_document_to_query(
            &document,
            self.query_index.schema(),
            self.query_index.tokenizers(),
        )?;

        let presearcher_query_matches = query_searcher.search(
            &*document_query,
            &PresearchQueryMatchCollector {
                query_searcher: &query_searcher,
                monitor_queries: &self.query_store,
                schema: self.query_index.schema(),
            },
        )?;

        let mut actual_query_matches: HashSet<u64> = HashSet::new();

        let index = Index::create_in_ram(self.document_schema.clone());

        let mut index_writer: IndexWriter<D> = index.writer(15_000_000)?;
        index_writer.add_document(document.clone())?;
        index_writer.commit()?;

        for monitor_query_id in presearcher_query_matches {
            if let Some(monitor_query) = self.query_store.get(&monitor_query_id) {
                let reader = index.reader()?;
                let searcher = reader.searcher();

                let query_matched =
                    searcher.search(&monitor_query.query, &QueryMatchCollector {})?;

                if query_matched {
                    actual_query_matches.insert(monitor_query_id);
                }
            }
        }

        Ok(actual_query_matches)
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
            dbg!("HERE 2");
            index_writer.add_document(subquery_document)?;
            dbg!("HERE 3");
        }

        self.query_store.insert(monitor_query.id, monitor_query);

        index_writer.commit()
    }
}

struct PresearchQueryMatchCollector<'a> {
    query_searcher: &'a Searcher,
    monitor_queries: &'a DashMap<u64, MonitorQuery>,
    schema: Schema,
}

impl Collector for PresearchQueryMatchCollector<'_> {
    type Fruit = HashSet<u64>;
    type Child = PresearchQueryMatchChildCollector;

    fn for_segment(
        &self,
        segment_local_id: tantivy::SegmentOrdinal,
        _segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(PresearchQueryMatchChildCollector {
            segment_local_id,
            subquery_document_ids: HashSet::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as tantivy::collector::SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        let mut matched_queries: HashSet<u64> = HashSet::new();
        for (segment_local_id, subquery_doc_ids) in segment_fruits {
            for subquery_doc_id in subquery_doc_ids {
                let document = self
                    .query_searcher
                    .doc::<TantivyDocument>(DocAddress::new(segment_local_id, subquery_doc_id))?;

                let parent_query_id_field = self.schema.get_field(MONITOR_QUERY_ID_FIELD_NAME)?;
                let parent_query_id = match document.get_first(parent_query_id_field).expect("") {
                    tantivy::schema::OwnedValue::U64(bytes) => Ok(bytes),
                    _ => Err(TantivyError::SchemaError("".to_string())),
                }?;

                match self.monitor_queries.get(parent_query_id) {
                    Some(monitor_query) => matched_queries.insert(monitor_query.id),
                    None => continue,
                };
            }
        }

        Ok(matched_queries)
    }
}

struct PresearchQueryMatchChildCollector {
    segment_local_id: u32,
    subquery_document_ids: HashSet<DocId>,
}

impl SegmentCollector for PresearchQueryMatchChildCollector {
    type Fruit = (u32, HashSet<DocId>);

    fn collect(&mut self, doc: tantivy::DocId, _score: tantivy::Score) {
        self.subquery_document_ids.insert(doc);
    }

    fn harvest(self) -> Self::Fruit {
        (self.segment_local_id, self.subquery_document_ids)
    }
}

struct QueryMatchCollector;

impl Collector for QueryMatchCollector {
    type Fruit = bool;
    type Child = QueryMatchChildCollector;

    fn for_segment(
        &self,
        _segment_local_id: tantivy::SegmentOrdinal,
        _segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(QueryMatchChildCollector { is_match: false })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as tantivy::collector::SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        let mut all_matched: bool = false;
        for matched in segment_fruits {
            all_matched |= matched;
        }

        Ok(all_matched)
    }
}

struct QueryMatchChildCollector {
    is_match: bool,
}

impl SegmentCollector for QueryMatchChildCollector {
    type Fruit = bool;

    fn collect(&mut self, _doc: tantivy::DocId, _score: tantivy::Score) {
        self.is_match = true;
    }

    fn harvest(self) -> Self::Fruit {
        self.is_match
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

        let id = 0;
        let monitor_query = MonitorQuery {
            id,
            query: Box::new(TermQuery::new(
                Term::from_field_text(body, "bloomberg"),
                IndexRecordOption::Basic,
            )),
        };

        let _ = monitor
            .register_query(monitor_query)
            .expect("Should not error registering query");

        let document = doc!(body => "Michael Bloomberg");

        let matches = monitor
            .match_document(document)
            .expect("Should not error matching document");

        assert!(matches.contains(&id));

        let document = doc!(body => "Michael Bay");

        let no_matches = monitor
            .match_document(document)
            .expect("Should not error matching document");

        assert!(no_matches.is_empty());
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

        let id = 0;
        let monitor_query = MonitorQuery {
            id,
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

        let matches = monitor
            .match_document(document)
            .expect("should not error matching document");

        assert!(matches.contains(&id));

        let document = doc!(body => "Donald Trump");

        let matches = monitor
            .match_document(document)
            .expect("should not error matching document");

        assert!(matches.contains(&id));

        let document = doc!(body => "Bloomberg Trump");

        let matches = monitor
            .match_document(document)
            .expect("should not error matching document");

        assert!(matches.contains(&id));

        let document = doc!(body => "Rishi Sunak");

        let matches = monitor
            .match_document(document)
            .expect("should not error matching document");

        assert!(matches.is_empty());
    }
}
