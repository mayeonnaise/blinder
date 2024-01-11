use std::iter;

use crate::list::List;
use tantivy::query::{BooleanQuery, BoostQuery, DisjunctionMaxQuery, Occur, Query, QueryClone};

pub struct QueryDecomposer<'a> {
    all_subqueries: List<'a, Box<dyn Query>>,
}

impl<'a> QueryDecomposer<'a> {
    pub fn new(all_subqueries: &'a mut Vec<Box<dyn Query>>) -> Self {
        Self::from_list(List::new(all_subqueries))
    }

    fn from_list(all_subqueries: List<'a, Box<dyn Query>>) -> Self {
        Self { all_subqueries }
    }

    pub fn decompose(&mut self, query: Box<dyn Query>) {
        let mut decomposer = QueryDecomposer::from_list(self.all_subqueries.saved());

        let query = match query.downcast::<BooleanQuery>() {
            Ok(query) => return decomposer.decompose_boolean(query),
            Err(query) => query,
        };

        let query = match query.downcast::<BoostQuery>() {
            Ok(query) => return decomposer.decompose_boost(&query),
            Err(query) => query,
        };

        let query = match query.downcast::<DisjunctionMaxQuery>() {
            Ok(query) => return decomposer.decompose_disjunction_max(&query),
            Err(query) => query,
        };

        self.all_subqueries.push(query);
    }

    fn decompose_boolean(&mut self, query: Box<BooleanQuery>) {
        let mut mandatory_clauses = Vec::new();
        let mut exclusion_clauses = Vec::new();

        for (occur, query) in query.clauses() {
            match occur {
                Occur::Should => {
                    QueryDecomposer::from_list(self.all_subqueries.saved())
                        .decompose(query.box_clone());
                }
                Occur::Must => {
                    mandatory_clauses.push(query);
                }
                Occur::MustNot => {
                    exclusion_clauses.push(query);
                }
            }
        }

        if mandatory_clauses.len() > 1
            || (mandatory_clauses.len() == 1 && !self.all_subqueries.is_empty())
        {
            self.all_subqueries.push(query);
            return;
        }

        if let &[mandatory_clause] = &mandatory_clauses[..] {
            QueryDecomposer::from_list(self.all_subqueries.saved())
                .decompose(mandatory_clause.box_clone());
        }

        if exclusion_clauses.is_empty() {
            return;
        }

        for subquery in &mut self.all_subqueries {
            *subquery = Box::new(BooleanQuery::new(
                iter::once((Occur::Must, subquery.box_clone()))
                    .chain(
                        exclusion_clauses
                            .iter()
                            .map(|exclusion_clause| (Occur::MustNot, exclusion_clause.box_clone())),
                    )
                    .collect(),
            ));
        }
    }

    fn decompose_boost(&mut self, query: &BoostQuery) {
        if query.boost() == 1.0 {
            return self.decompose(query.query());
        }

        self.decompose(query.query());
        for subquery in &mut self.all_subqueries {
            *subquery = Box::new(BoostQuery::new(subquery.box_clone(), query.boost()));
        }
    }

    fn decompose_disjunction_max(&mut self, query: &DisjunctionMaxQuery) {
        for subquery in query.disjuncts() {
            self.decompose(subquery.box_clone());
        }
    }
}
