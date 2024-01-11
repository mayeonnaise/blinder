use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::{mapref::entry::Entry, DashMap};
use tantivy::{
    query::{Bm25StatisticsProvider, QueryDocumentTree},
    schema::Field,
    Score, Term,
};

fn idf(doc_freq: u64, doc_count: u64) -> Score {
    assert!(doc_count >= doc_freq);
    let x = ((doc_count - doc_freq) as Score + 0.5) / (doc_freq as Score + 0.5);
    (1.0 + x).ln()
}

pub trait PresearcherScorer {
    fn score(&self, query_document_tree: &QueryDocumentTree) -> f32;
    fn add_term(&self, term: Term);
    fn add_document_count(&self);
}

#[derive(Default)]
pub struct TfIdfScorer {
    token_count: AtomicU64,
    document_count: AtomicU64,
    term_frequencies: DashMap<Term, u64>,
}

impl PresearcherScorer for TfIdfScorer {
    fn add_document_count(&self) {
        self.document_count.fetch_add(1, Ordering::Relaxed);
    }

    fn add_term(&self, term: Term) {
        self.token_count.fetch_add(1, Ordering::Relaxed);

        match self.term_frequencies.entry(term) {
            Entry::Occupied(mut entry) => {
                let term_frequency = entry.get() + 1;
                entry.insert(term_frequency);
            }
            Entry::Vacant(entry) => {
                entry.insert(1);
            }
        }
    }

    fn score(&self, query_document_tree: &QueryDocumentTree) -> f32 {
        return match query_document_tree {
            QueryDocumentTree::Conjunction(trees) => trees.iter().fold(0_f32, |max_score, tree| {
                let tree_score = self.score(tree);
                if max_score < tree_score {
                    tree_score
                } else {
                    max_score
                }
            }),
            QueryDocumentTree::Disjunction(trees) => trees.iter().fold(1_f32, |min_score, tree| {
                let tree_score = self.score(tree);
                if min_score > tree_score {
                    tree_score
                } else {
                    min_score
                }
            }),
            QueryDocumentTree::Term(term) => {
                return match (self.doc_freq(term), self.total_num_docs()) {
                    (Ok(doc_freq), Ok(total_num_docs)) => idf(doc_freq, total_num_docs),
                    _ => 0_f32,
                }
            }
            QueryDocumentTree::AnyTerm => -1_f32,
        };
    }
}

impl Bm25StatisticsProvider for TfIdfScorer {
    fn total_num_tokens(&self, _: Field) -> tantivy::Result<u64> {
        Ok(self.token_count.load(Ordering::Relaxed))
    }

    fn total_num_docs(&self) -> tantivy::Result<u64> {
        Ok(self.document_count.load(Ordering::Relaxed))
    }

    fn doc_freq(&self, term: &Term) -> tantivy::Result<u64> {
        Ok(self.term_frequencies.get(term).map_or(0, |freq| *freq))
    }
}

#[cfg(test)]
mod test {
    use tantivy::schema::{Schema, TEXT};
    use tantivy::{schema::Field, Term};

    use super::*;

    fn add_document<P: PresearcherScorer>(field: &Field, value: &str, scorer: &P) {
        scorer.add_document_count();
        for text in value.split_whitespace() {
            scorer.add_term(Term::from_field_text(*field, text));
        }
    }

    #[test]
    fn test_term_get_score() {
        // Given
        let mut schema_builder = Schema::builder();
        let body = schema_builder.add_text_field("body", TEXT);

        let scorer = TfIdfScorer::default();
        add_document(&body, "This is the first document", &scorer);
        add_document(&body, "This is the second document", &scorer);
        add_document(&body, "This is the third document", &scorer);

        let document_term = Term::from_field_text(body, "document");
        let document_term_tree = QueryDocumentTree::Term(document_term);
        let first_term = Term::from_field_text(body, "first");
        let first_term_tree = QueryDocumentTree::Term(first_term);
        let non_existent_term = Term::from_field_text(body, "fourth");
        let non_existent_term_tree = QueryDocumentTree::Term(non_existent_term);

        // When
        let document_term_score = scorer.score(&document_term_tree);
        let first_term_score = scorer.score(&first_term_tree);
        let non_existent_term_score = scorer.score(&non_existent_term_tree);

        // Then
        assert_eq!(document_term_score, 0.13353144);
        assert_eq!(first_term_score, 0.9808292);
        assert_eq!(non_existent_term_score, 2.0794415);
    }

    #[test]
    fn test_disjunction_get_score() {
        // Given
        let mut schema_builder = Schema::builder();
        let body = schema_builder.add_text_field("body", TEXT);

        let scorer = TfIdfScorer::default();
        add_document(&body, "This is the first document", &scorer);
        add_document(&body, "This is the second document", &scorer);
        add_document(&body, "This is the third document", &scorer);

        let document_term = Term::from_field_text(body, "document");
        let document_term_tree = QueryDocumentTree::Term(document_term);
        let first_term = Term::from_field_text(body, "first");
        let first_term_tree = QueryDocumentTree::Term(first_term);
        let non_existent_term = Term::from_field_text(body, "fourth");
        let non_existent_term_tree = QueryDocumentTree::Term(non_existent_term);
        let disjunction = QueryDocumentTree::Disjunction(vec![
            document_term_tree,
            first_term_tree,
            non_existent_term_tree,
        ]);

        // When
        let disjunction_score = scorer.score(&disjunction);

        // Then
        assert_eq!(disjunction_score, 0.13353144);
    }

    #[test]
    fn test_conjunction_get_score() {
        // Given
        let mut schema_builder = Schema::builder();
        let body = schema_builder.add_text_field("body", TEXT);

        let scorer = TfIdfScorer::default();
        add_document(&body, "This is the first document", &scorer);
        add_document(&body, "This is the second document", &scorer);
        add_document(&body, "This is the third document", &scorer);

        let document_term = Term::from_field_text(body, "document");
        let document_term_tree = QueryDocumentTree::Term(document_term);
        let first_term = Term::from_field_text(body, "first");
        let first_term_tree = QueryDocumentTree::Term(first_term);
        let non_existent_term = Term::from_field_text(body, "fourth");
        let non_existent_term_tree = QueryDocumentTree::Term(non_existent_term);
        let conjunction = QueryDocumentTree::Conjunction(vec![
            document_term_tree,
            first_term_tree,
            non_existent_term_tree,
        ]);

        // When
        let conjunction_score = scorer.score(&conjunction);

        // Then
        assert_eq!(conjunction_score, 2.0794415);
    }
}
