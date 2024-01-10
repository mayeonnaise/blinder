use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::atomic::{AtomicU64, Ordering},
};

use dashmap::{mapref::entry::Entry, DashMap};
use tantivy::{
    query::{
        Bm25StatisticsProvider, BooleanQuery, Query, QueryDocumentTree, TermQuery, TermSetQuery,
    },
    query_grammar::Occur,
    schema::{Field, IndexRecordOption, OwnedValue, Schema, Value},
    tokenizer::{Token, TokenizerManager},
    Document, Score, TantivyError, Term,
};

use crate::monitor::query::ANYTERM_FIELD;

fn idf(doc_freq: u64, doc_count: u64) -> Score {
    assert!(doc_count >= doc_freq);
    let x = ((doc_count - doc_freq) as Score + 0.5) / (doc_freq as Score + 0.5);
    (1.0 + x).ln()
}

pub trait Presearcher {
    fn convert_query_to_document(
        &self,
        query: &dyn Query,
        schema: Schema,
    ) -> Result<HashMap<Field, OwnedValue>, TantivyError>;
    fn convert_document_to_query<D: Debug + Document>(
        &self,
        document: &D,
        schema: Schema,
        tokenizer_manager: &TokenizerManager,
    ) -> Result<Box<dyn Query>, TantivyError>;
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

pub struct TermFilteredPresearcher<S: PresearcherScorer> {
    pub scorer: Box<S>,
}

impl<S: PresearcherScorer> TermFilteredPresearcher<S> {
    fn to_field_terms(
        &self,
        query_document_tree: &QueryDocumentTree,
        field_terms: &mut HashMap<Field, HashSet<Term>>,
        schema: Schema,
    ) -> Result<(), TantivyError> {
        match query_document_tree {
            QueryDocumentTree::Conjunction(trees) => {
                let mut sorted_trees = trees
                    .iter()
                    .map(|tree| (self.scorer.score(tree), tree))
                    .collect::<Vec<(f32, &QueryDocumentTree)>>();

                sorted_trees.sort_by(|(score_a, _), (score_b, _)| score_b.total_cmp(score_a));

                if let Some((_, tree_with_highest_score)) = sorted_trees.first() {
                    self.to_field_terms(tree_with_highest_score, field_terms, schema)?;
                }
            }
            QueryDocumentTree::Disjunction(trees) => {
                for tree in trees {
                    self.to_field_terms(tree, field_terms, schema.clone())?;
                }
            }
            QueryDocumentTree::Term(term) => {
                let terms = field_terms.entry(term.field()).or_default();

                terms.insert(term.clone());
            }
            QueryDocumentTree::AnyTerm => {
                let terms = field_terms
                    .entry(schema.get_field(ANYTERM_FIELD)?)
                    .or_default();

                terms.insert(Term::from_field_bool(
                    schema.get_field(ANYTERM_FIELD)?,
                    true,
                ));
            }
        }

        Ok(())
    }
}

impl<S: PresearcherScorer> Presearcher for TermFilteredPresearcher<S> {
    fn convert_query_to_document(
        &self,
        query: &dyn Query,
        schema: Schema,
    ) -> Result<HashMap<Field, OwnedValue>, TantivyError> {
        let mut document = HashMap::<Field, OwnedValue>::new();
        let mut field_terms = HashMap::<Field, HashSet<Term>>::new();
        self.to_field_terms(&query.to_ast(), &mut field_terms, schema.clone())?;

        for (field, terms) in field_terms.into_iter() {
            let field_entry = schema.get_field_entry(field);
            let field_type = field_entry.field_type();
            match field_type {
                tantivy::schema::FieldType::Str(_) => {
                    let joined_terms = terms
                        .into_iter()
                        .filter_map(|term| term.value().as_str().map(|s| s.to_string()))
                        .collect::<Vec<String>>()
                        .join(" ");

                    document.insert(field, OwnedValue::Str(joined_terms));
                }
                tantivy::schema::FieldType::Bool(_) => match schema.get_field(ANYTERM_FIELD) {
                    Ok(anyterm_field) => {
                        if field == anyterm_field {
                            document.insert(anyterm_field, OwnedValue::Bool(true));
                        }
                    }
                    Err(_) => continue,
                },
                _ => continue,
            }
        }

        Ok(document)
    }

    fn convert_document_to_query<D: Debug + Document>(
        &self,
        document: &D,
        schema: Schema,
        tokenizer_manager: &TokenizerManager,
    ) -> Result<Box<dyn Query>, TantivyError> {
        self.scorer.add_document_count();

        let mut terms = Vec::<Term>::new();

        for (field, value) in document.iter_fields_and_values() {
            let field_entry = schema.get_field_entry(field);
            let field_type = field_entry.field_type();
            let indexing_options_opt = match field_type {
                tantivy::schema::FieldType::Str(options) => options.get_indexing_options(),
                tantivy::schema::FieldType::JsonObject(options) => {
                    options.get_text_indexing_options()
                }
                _ => {
                    continue;
                }
            };
            let indexing_options = indexing_options_opt.ok_or_else(|| {
                TantivyError::InvalidArgument(format!(
                    "No indexing options set for field {field_entry:?}"
                ))
            })?;

            let mut tokenizer = tokenizer_manager
                .get(indexing_options.tokenizer())
                .ok_or_else(|| {
                    TantivyError::InvalidArgument(format!(
                        "No Tokenizer found for field {field_entry:?}"
                    ))
                })?;

            let mut token_stream = tokenizer.token_stream(value.as_str().ok_or_else(|| {
                TantivyError::InvalidArgument(format!(
                    "{:?} is not a text field.",
                    field_entry.name()
                ))
            })?);

            let mut to_term = |token: &Token| {
                let term = Term::from_field_text(field, &token.text);
                self.scorer.add_term(term.clone());
                terms.push(term);
            };

            token_stream.process(&mut to_term);
        }

        let query = BooleanQuery::new(vec![
            (Occur::Should, Box::new(TermSetQuery::new(terms))),
            (
                Occur::Should,
                Box::new(TermQuery::new(
                    Term::from_field_bool(schema.get_field(ANYTERM_FIELD)?, true),
                    IndexRecordOption::Basic,
                )),
            ),
        ]);

        Ok(Box::new(query))
    }
}

#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};

    use tantivy::schema::{Schema, TEXT};
    use tantivy::Index;
    use tantivy::{schema::Field, Term};

    use super::*;

    fn add_document<P: PresearcherScorer>(field: &Field, value: &str, scorer: &P) {
        scorer.add_document_count();
        for text in value.split_whitespace() {
            scorer.add_term(Term::from_field_text(field.clone(), text));
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

    #[test]
    fn test_term_to_field_terms() {
        // Given
        let mut schema_builder = Schema::builder();
        let body = schema_builder.add_text_field("body", TEXT);
        let index = Index::create_in_ram(schema_builder.build());

        let mut field_terms = HashMap::<Field, HashSet<Term>>::new();

        let scorer = TfIdfScorer::default();
        add_document(&body, "This is the first document", &scorer);
        let presearcher: TermFilteredPresearcher<TfIdfScorer> = TermFilteredPresearcher {
            scorer: Box::new(scorer),
        };

        let document_term = Term::from_field_text(body, "document");
        let document_term_tree = QueryDocumentTree::Term(document_term.clone());

        // When
        let _ = presearcher.to_field_terms(&document_term_tree, &mut field_terms, index.schema());

        // Then
        let found_field_terms = field_terms.entry(body).or_default();
        assert!(found_field_terms.contains(&document_term));
    }

    #[test]
    fn test_disjunction_to_field_terms() {
        // Given
        let mut schema_builder = Schema::builder();
        let body = schema_builder.add_text_field("body", TEXT);
        let index = Index::create_in_ram(schema_builder.build());

        let mut field_terms = HashMap::<Field, HashSet<Term>>::new();

        let scorer = TfIdfScorer::default();
        add_document(&body, "This is the first document", &scorer);
        let presearcher: TermFilteredPresearcher<TfIdfScorer> = TermFilteredPresearcher {
            scorer: Box::new(scorer),
        };

        let document_term = Term::from_field_text(Field::from_field_id(0), "document");
        let document_term_tree = QueryDocumentTree::Term(document_term.clone());
        let first_term = Term::from_field_text(Field::from_field_id(0), "first");
        let first_term_tree = QueryDocumentTree::Term(first_term.clone());
        let non_existent_term = Term::from_field_text(Field::from_field_id(0), "fourth");
        let non_existent_term_tree = QueryDocumentTree::Term(non_existent_term.clone());
        let disjunction = QueryDocumentTree::Disjunction(vec![
            document_term_tree,
            first_term_tree,
            non_existent_term_tree,
        ]);

        // When
        let _ = presearcher.to_field_terms(&disjunction, &mut field_terms, index.schema());

        // Then
        let found_field_terms = field_terms.entry(Field::from_field_id(0)).or_default();
        assert!(found_field_terms.contains(&document_term));
        assert!(found_field_terms.contains(&first_term));
        assert!(found_field_terms.contains(&non_existent_term));
    }

    #[test]
    fn test_conjunction_to_field_terms() {
        // Given
        let mut schema_builder = Schema::builder();
        let body = schema_builder.add_text_field("body", TEXT);
        let index = Index::create_in_ram(schema_builder.build());

        let mut field_terms = HashMap::<Field, HashSet<Term>>::new();

        let scorer = TfIdfScorer::default();
        add_document(&body, "This is the first document", &scorer);
        let presearcher: TermFilteredPresearcher<TfIdfScorer> = TermFilteredPresearcher {
            scorer: Box::new(scorer),
        };

        let document_term = Term::from_field_text(body, "document");
        let document_term_tree = QueryDocumentTree::Term(document_term.clone());
        let first_term = Term::from_field_text(body, "first");
        let first_term_tree = QueryDocumentTree::Term(first_term.clone());
        let non_existent_term = Term::from_field_text(body, "fourth");
        let non_existent_term_tree = QueryDocumentTree::Term(non_existent_term.clone());
        let conjunction = QueryDocumentTree::Conjunction(vec![
            document_term_tree,
            first_term_tree,
            non_existent_term_tree,
        ]);

        // When
        let _ = presearcher.to_field_terms(&conjunction, &mut field_terms, index.schema());

        // Then
        let found_field_terms = field_terms.entry(body).or_default();
        assert!(!found_field_terms.contains(&document_term));
        assert!(!found_field_terms.contains(&first_term));
        assert!(found_field_terms.contains(&non_existent_term));
    }
}
