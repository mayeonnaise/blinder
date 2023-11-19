use std::collections::{HashMap, HashSet};

use tantivy::{
    query::{Bm25StatisticsProvider, Query, QueryDocumentTree},
    schema::Field,
    Score, TantivyDocument, Term,
};

fn idf(doc_freq: u64, doc_count: u64) -> Score {
    assert!(doc_count >= doc_freq, "{doc_count} >= {doc_freq}");
    let x = ((doc_count - doc_freq) as Score + 0.5) / (doc_freq as Score + 0.5);
    (1.0 + x).ln()
}

pub trait Presearcher {
    fn convert_query_to_document(&self, query: &dyn Query) -> TantivyDocument;
    fn convert_document_to_query(&self, document: &TantivyDocument) -> Box<dyn Query>;
}

pub struct TermFilteredPresearcher {
    scorer: Box<dyn Bm25StatisticsProvider>,
}

impl TermFilteredPresearcher {
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
                return match (self.scorer.doc_freq(term), self.scorer.total_num_docs()) {
                    (Ok(doc_freq), Ok(total_num_docs)) => idf(doc_freq, total_num_docs),
                    _ => 0_f32,
                }
            }
            QueryDocumentTree::AnyTerm => -1_f32,
        };
    }

    fn to_field_terms(
        &self,
        query_document_tree: &QueryDocumentTree,
        field_terms: &mut HashMap<Field, HashSet<Term>>,
    ) {
        match query_document_tree {
            QueryDocumentTree::Conjunction(trees) => {
                let mut sorted_trees = trees
                    .iter()
                    .map(|tree| (self.score(tree), tree))
                    .collect::<Vec<(f32, &QueryDocumentTree)>>();

                sorted_trees.sort_by(|(score_a, _), (score_b, _)| score_b.total_cmp(score_a));

                if let Some((_, tree_with_highest_score)) = sorted_trees.first() {
                    self.to_field_terms(tree_with_highest_score, field_terms);
                }
            }
            QueryDocumentTree::Disjunction(trees) => {
                for tree in trees {
                    self.to_field_terms(tree, field_terms);
                }
            }
            QueryDocumentTree::Term(term) => {
                let terms = field_terms.entry(term.field()).or_default();

                terms.insert(term.clone());
            }
            QueryDocumentTree::AnyTerm => todo!(),
        }
    }
}

impl Presearcher for TermFilteredPresearcher {
    fn convert_query_to_document(&self, query: &dyn Query) -> TantivyDocument {
        let mut document = TantivyDocument::new();
        let mut field_terms = HashMap::<Field, HashSet<Term>>::new();
        self.to_field_terms(&query.to_ast(), &mut field_terms);

        for (field, terms) in field_terms.into_iter() {
            let joined_terms = terms
                .into_iter()
                .map(|term| {
                    return match term.clone().value().as_str() {
                        Some(term_value) => term_value.to_string(),
                        None => "".to_string(),
                    };
                })
                .collect::<Vec<String>>()
                .join(" ");

            document.add_text(field, joined_terms);
        }

        document
    }

    fn convert_document_to_query(&self, _document: &TantivyDocument) -> Box<dyn Query> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};

    use tantivy::{query::Bm25StatisticsProvider, schema::Field, Result, Term};

    use super::QueryDocumentTree;
    use super::TermFilteredPresearcher;

    struct TestStatisticsProvider {
        document_count: u64,
        term_doc_freq: HashMap<Term, u64>,
    }

    impl Bm25StatisticsProvider for TestStatisticsProvider {
        fn total_num_tokens(&self, _: Field) -> Result<u64> {
            Ok(0)
        }

        fn total_num_docs(&self) -> Result<u64> {
            Ok(self.document_count)
        }

        fn doc_freq(&self, term: &Term) -> Result<u64> {
            Ok(self.term_doc_freq.get(term).map_or(0, |freq| freq.clone()))
        }
    }

    impl TestStatisticsProvider {
        fn add_document(&mut self, document: &str) {
            self.document_count += 1;

            for term in document.split_whitespace() {
                let freq = self
                    .term_doc_freq
                    .entry(Term::from_field_text(Field::from_field_id(0), term))
                    .or_default();
                *freq += 1;
            }
        }
    }

    #[test]
    fn test_term_get_score() {
        // Given
        let mut stats_provider = TestStatisticsProvider {
            document_count: 0,
            term_doc_freq: HashMap::<Term, u64>::new(),
        };
        stats_provider.add_document("This is the first document");
        stats_provider.add_document("This is the second document");
        stats_provider.add_document("This is the third document");

        let presearcher: TermFilteredPresearcher = TermFilteredPresearcher {
            scorer: Box::new(stats_provider),
        };

        let document_term = Term::from_field_text(Field::from_field_id(0), "document");
        let document_term_tree = QueryDocumentTree::Term(document_term);
        let first_term = Term::from_field_text(Field::from_field_id(0), "first");
        let first_term_tree = QueryDocumentTree::Term(first_term);
        let non_existent_term = Term::from_field_text(Field::from_field_id(0), "fourth");
        let non_existent_term_tree = QueryDocumentTree::Term(non_existent_term);

        // When
        let document_term_score = presearcher.score(&document_term_tree);
        let first_term_score = presearcher.score(&first_term_tree);
        let non_existent_term_score = presearcher.score(&non_existent_term_tree);

        // Then
        assert_eq!(document_term_score, 0.13353144);
        assert_eq!(first_term_score, 0.9808292);
        assert_eq!(non_existent_term_score, 2.0794415);
    }

    #[test]
    fn test_disjunction_get_score() {
        // Given
        let mut stats_provider = TestStatisticsProvider {
            document_count: 0,
            term_doc_freq: HashMap::<Term, u64>::new(),
        };
        stats_provider.add_document("This is the first document");
        stats_provider.add_document("This is the second document");
        stats_provider.add_document("This is the third document");
        let presearcher: TermFilteredPresearcher = TermFilteredPresearcher {
            scorer: Box::new(stats_provider),
        };

        let document_term = Term::from_field_text(Field::from_field_id(0), "document");
        let document_term_tree = QueryDocumentTree::Term(document_term);
        let first_term = Term::from_field_text(Field::from_field_id(0), "first");
        let first_term_tree = QueryDocumentTree::Term(first_term);
        let non_existent_term = Term::from_field_text(Field::from_field_id(0), "fourth");
        let non_existent_term_tree = QueryDocumentTree::Term(non_existent_term);
        let disjunction = QueryDocumentTree::Disjunction(vec![
            document_term_tree,
            first_term_tree,
            non_existent_term_tree,
        ]);

        // When
        let disjunction_score = presearcher.score(&disjunction);

        // Then
        assert_eq!(disjunction_score, 0.13353144);
    }

    #[test]
    fn test_conjunction_get_score() {
        // Given
        let mut stats_provider = TestStatisticsProvider {
            document_count: 0,
            term_doc_freq: HashMap::<Term, u64>::new(),
        };
        stats_provider.add_document("This is the first document");
        stats_provider.add_document("This is the second document");
        stats_provider.add_document("This is the third document");
        let presearcher: TermFilteredPresearcher = TermFilteredPresearcher {
            scorer: Box::new(stats_provider),
        };

        let document_term = Term::from_field_text(Field::from_field_id(0), "document");
        let document_term_tree = QueryDocumentTree::Term(document_term);
        let first_term = Term::from_field_text(Field::from_field_id(0), "first");
        let first_term_tree = QueryDocumentTree::Term(first_term);
        let non_existent_term = Term::from_field_text(Field::from_field_id(0), "fourth");
        let non_existent_term_tree = QueryDocumentTree::Term(non_existent_term);
        let conjunction = QueryDocumentTree::Conjunction(vec![
            document_term_tree,
            first_term_tree,
            non_existent_term_tree,
        ]);

        // When
        let conjunction_score = presearcher.score(&conjunction);

        // Then
        assert_eq!(conjunction_score, 2.0794415);
    }

    #[test]
    fn test_term_to_field_terms() {
        // Given
        let mut field_terms = HashMap::<Field, HashSet<Term>>::new();

        let mut stats_provider = TestStatisticsProvider {
            document_count: 0,
            term_doc_freq: HashMap::<Term, u64>::new(),
        };
        stats_provider.add_document("This is the first document");
        let presearcher: TermFilteredPresearcher = TermFilteredPresearcher {
            scorer: Box::new(stats_provider),
        };

        let document_term = Term::from_field_text(Field::from_field_id(0), "document");
        let document_term_tree = QueryDocumentTree::Term(document_term.clone());

        // When
        presearcher.to_field_terms(&document_term_tree, &mut field_terms);

        // Then
        let found_field_terms = field_terms.entry(Field::from_field_id(0)).or_default();
        assert!(found_field_terms.contains(&document_term));
    }

    #[test]
    fn test_disjunction_to_field_terms() {
        // Given
        let mut field_terms = HashMap::<Field, HashSet<Term>>::new();

        let mut stats_provider = TestStatisticsProvider {
            document_count: 0,
            term_doc_freq: HashMap::<Term, u64>::new(),
        };
        stats_provider.add_document("This is the first document");
        let presearcher: TermFilteredPresearcher = TermFilteredPresearcher {
            scorer: Box::new(stats_provider),
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
        presearcher.to_field_terms(&disjunction, &mut field_terms);

        // Then
        let found_field_terms = field_terms.entry(Field::from_field_id(0)).or_default();
        assert!(found_field_terms.contains(&document_term));
        assert!(found_field_terms.contains(&first_term));
        assert!(found_field_terms.contains(&non_existent_term));
    }

    #[test]
    fn test_conjunction_to_field_terms() {
        // Given
        let mut field_terms = HashMap::<Field, HashSet<Term>>::new();

        let mut stats_provider = TestStatisticsProvider {
            document_count: 0,
            term_doc_freq: HashMap::<Term, u64>::new(),
        };
        stats_provider.add_document("This is the first document");
        let presearcher: TermFilteredPresearcher = TermFilteredPresearcher {
            scorer: Box::new(stats_provider),
        };

        let document_term = Term::from_field_text(Field::from_field_id(0), "document");
        let document_term_tree = QueryDocumentTree::Term(document_term.clone());
        let first_term = Term::from_field_text(Field::from_field_id(0), "first");
        let first_term_tree = QueryDocumentTree::Term(first_term.clone());
        let non_existent_term = Term::from_field_text(Field::from_field_id(0), "fourth");
        let non_existent_term_tree = QueryDocumentTree::Term(non_existent_term.clone());
        let conjunction = QueryDocumentTree::Conjunction(vec![
            document_term_tree,
            first_term_tree,
            non_existent_term_tree,
        ]);

        // When
        presearcher.to_field_terms(&conjunction, &mut field_terms);

        // Then
        let found_field_terms = field_terms.entry(Field::from_field_id(0)).or_default();
        assert!(!found_field_terms.contains(&document_term));
        assert!(!found_field_terms.contains(&first_term));
        assert!(found_field_terms.contains(&non_existent_term));
    }
}
