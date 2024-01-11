pub(crate) mod metrics;
pub(crate) mod scorer;
pub(crate) mod term_filtered_presearcher;

pub use self::metrics::PresearcherMetrics;
pub use self::scorer::{PresearcherScorer, TfIdfScorer};
pub use self::term_filtered_presearcher::TermFilteredPresearcher;

use std::collections::HashMap;

use tantivy::{
    query::Query,
    schema::{Field, OwnedValue, Schema},
    tokenizer::TokenizerManager,
    Document, TantivyError,
};

pub trait Presearcher {
    fn convert_query_to_document(
        &self,
        query: &dyn Query,
        schema: Schema,
    ) -> Result<HashMap<Field, OwnedValue>, TantivyError>;

    fn convert_document_to_query(
        &self,
        document: &impl Document,
        schema: Schema,
        tokenizer_manager: &TokenizerManager,
    ) -> Result<Box<dyn Query>, TantivyError>;
}
