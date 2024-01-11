use std::collections::{HashMap, HashSet};

use dashmap::DashMap;
use tantivy::{
    collector::{Collector, SegmentCollector},
    schema::{Field, OwnedValue, Schema},
    DocAddress, DocId, Document, Index, IndexWriter, Searcher, TantivyError,
};

use crate::presearcher::{Presearcher, PresearcherMetrics};

use super::{query::MONITOR_QUERY_ID_FIELD_NAME, Monitor, MonitorQuery};

pub struct MonitorMatcher<'a, P: Presearcher, D: Document> {
    monitor: &'a Monitor<P>,
    document_index_writer: IndexWriter<D>,
}

impl<'a, P: Presearcher, D: Document> MonitorMatcher<'a, P, D> {
    pub(super) fn new(monitor: &'a Monitor<P>) -> tantivy::Result<Self> {
        let document_index_writer = Index::create_in_ram(monitor.document_schema.clone())
            .writer_with_num_threads(1, 15_000_000)?;

        Ok(Self {
            monitor,
            document_index_writer,
        })
    }

    pub fn match_document(
        &mut self,
        document: D,
    ) -> tantivy::Result<(HashSet<u64>, PresearcherMetrics)> {
        let mut presearcher_metrics = PresearcherMetrics {
            total_queries: self.monitor.query_store.len(),
            ..Default::default()
        };

        let query_reader = self.monitor.query_index.reader()?;
        let query_searcher = query_reader.searcher();

        let document_query = self.monitor.presearcher.convert_document_to_query(
            &document,
            self.monitor.query_index.schema(),
            self.monitor.query_index.tokenizers(),
        )?;

        let presearcher_query_matches = query_searcher.search(
            &*document_query,
            &PresearchQueryMatchCollector {
                query_searcher: &query_searcher,
                monitor_queries: &self.monitor.query_store,
                schema: self.monitor.query_index.schema(),
            },
        )?;
        presearcher_metrics.prospective_queries = presearcher_query_matches.len();

        let mut actual_query_matches: HashSet<u64> = HashSet::new();

        self.document_index_writer.delete_all_documents()?;
        self.document_index_writer.add_document(document)?;
        self.document_index_writer.commit()?;

        let reader = self.document_index_writer.index().reader()?;
        for monitor_query_id in presearcher_query_matches {
            if let Some(monitor_query) = self.monitor.query_store.get(&monitor_query_id) {
                let searcher = reader.searcher();

                let query_matched =
                    searcher.search(&monitor_query.query, &QueryMatchCollector {})?;

                if query_matched {
                    actual_query_matches.insert(monitor_query_id);
                }
            }
        }

        presearcher_metrics.actual_matches = actual_query_matches.len();
        Ok((actual_query_matches, presearcher_metrics))
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
                let document =
                    self.query_searcher
                        .doc::<HashMap<Field, OwnedValue>>(DocAddress::new(
                            segment_local_id,
                            subquery_doc_id,
                        ))?;

                let parent_query_id_field = self.schema.get_field(MONITOR_QUERY_ID_FIELD_NAME)?;
                let parent_query_id = match document.get(&parent_query_id_field).expect("") {
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
