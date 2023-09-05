use std::{collections::HashSet, error::Error};

use tantivy::{
    collector::{Collector, SegmentCollector, TopDocs},
    query::{BooleanQuery, Query},
    schema::Schema,
    DocId, Document, Index,
};

use crate::monitor_query::MonitorQuery;

pub struct Monitor {
    query_index: Index,
    document_schema: Schema,
    monitor_queries: HashSet<MonitorQuery>,
}

impl Monitor {
    pub fn match_query(&self, document: Document) -> Result<String, Box<dyn Error>> {
        let reader = self.query_index.reader()?;
        let searcher = reader.searcher();

        let document_query = self.document_to_query(&document)?;
        let documents = searcher.search(&*document_query, &TopDocs::with_limit(10));

        Ok("".to_string())
    }

    fn document_to_query(&self, document: &Document) -> Result<Box<dyn Query>, Box<dyn Error>> {
        todo!()
    }
}

struct QueryMatchCollector<'a> {
    indexed_document: Index,
    monitor_queries: &'a HashSet<MonitorQuery>,
}

impl Collector for QueryMatchCollector<'_> {
    type Fruit = Box<dyn Query>;
    type Child = QueryMatchChildCollector;

    fn for_segment(
        &self,
        segment_local_id: tantivy::SegmentOrdinal,
        _segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(QueryMatchChildCollector {
            segment_local_id: segment_local_id,
            query_document_id: Option::None,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as tantivy::collector::SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        for (segment_local_id, doc_id) in segment_fruits {
            let reader = self.indexed_document.reader()?;
            let searcher = reader.searcher();
        }

        Ok(Box::new(BooleanQuery::new(vec![])))
    }
}

struct QueryMatchChildCollector {
    segment_local_id: u32,
    query_document_id: Option<DocId>,
}

impl SegmentCollector for QueryMatchChildCollector {
    type Fruit = (u32, Option<DocId>);

    fn collect(&mut self, doc: tantivy::DocId, score: tantivy::Score) {
        todo!()
    }

    fn harvest(self) -> Self::Fruit {
        todo!()
    }
}
