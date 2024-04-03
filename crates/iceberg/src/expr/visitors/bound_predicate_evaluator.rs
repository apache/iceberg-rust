use crate::expr::{BoundPredicate, BoundReference, PredicateOperator};
use crate::spec::Datum;
use crate::Result;
use fnv::FnvHashSet;

pub trait BoundPredicateEvaluator {
    fn visit(&mut self, node: &BoundPredicate) -> Result<bool> {
        match node {
            BoundPredicate::AlwaysTrue => self.always_true(),
            BoundPredicate::AlwaysFalse => self.always_false(),
            BoundPredicate::And(expr) => {
                let [left_pred, right_pred] = expr.inputs();

                let left_result = self.visit(left_pred)?;
                let right_result = self.visit(right_pred)?;

                Ok(left_result && right_result)
            }
            BoundPredicate::Or(expr) => {
                let [left_pred, right_pred] = expr.inputs();

                let left_result = self.visit(left_pred)?;
                let right_result = self.visit(right_pred)?;

                Ok(left_result || right_result)
            }
            BoundPredicate::Not(expr) => {
                let [inner_pred] = expr.inputs();

                let inner_result = self.visit(inner_pred)?;

                self.not(inner_result)
            }
            BoundPredicate::Unary(expr) => match expr.op() {
                PredicateOperator::IsNull => self.is_null(expr.term()),
                PredicateOperator::NotNull => self.not_null(expr.term()),
                PredicateOperator::IsNan => self.is_nan(expr.term()),
                PredicateOperator::NotNan => self.not_nan(expr.term()),
                op => {
                    panic!("Unexpected op for unary predicate: {}", &op)
                }
            },
            BoundPredicate::Binary(expr) => {
                let reference = expr.term();
                let literal = expr.literal();
                match expr.op() {
                    PredicateOperator::LessThan => self.less_than(reference, literal),
                    PredicateOperator::LessThanOrEq => self.less_than_or_eq(reference, literal),
                    PredicateOperator::GreaterThan => self.greater_than(reference, literal),
                    PredicateOperator::GreaterThanOrEq => {
                        self.greater_than_or_eq(reference, literal)
                    }
                    PredicateOperator::Eq => self.eq(reference, literal),
                    PredicateOperator::NotEq => self.not_eq(reference, literal),
                    PredicateOperator::StartsWith => self.starts_with(reference, literal),
                    PredicateOperator::NotStartsWith => self.not_starts_with(reference, literal),
                    op => {
                        panic!("Unexpected op for binary predicate: {}", &op)
                    }
                }
            }
            BoundPredicate::Set(expr) => {
                let reference = expr.term();
                let literals = expr.literals();
                match expr.op() {
                    PredicateOperator::In => self.r#in(reference, literals),
                    PredicateOperator::NotIn => self.not_in(reference, literals),
                    op => {
                        panic!("Unexpected op for set predicate: {}", &op)
                    }
                }
            }
        }
    }

    // default implementations for logical operators
    fn always_true(&mut self) -> Result<bool> {
        Ok(true)
    }
    fn always_false(&mut self) -> Result<bool> {
        Ok(false)
    }
    fn and(&mut self, lhs: bool, rhs: bool) -> Result<bool> {
        Ok(lhs && rhs)
    }
    fn or(&mut self, lhs: bool, rhs: bool) -> Result<bool> {
        Ok(lhs || rhs)
    }
    fn not(&mut self, inner: bool) -> Result<bool> {
        Ok(!inner)
    }

    // visitor methods for unary / binary / set operators must be implemented
    fn is_null(&mut self, reference: &BoundReference) -> Result<bool>;
    fn not_null(&mut self, reference: &BoundReference) -> Result<bool>;
    fn is_nan(&mut self, reference: &BoundReference) -> Result<bool>;
    fn not_nan(&mut self, reference: &BoundReference) -> Result<bool>;
    fn less_than(&mut self, reference: &BoundReference, literal: &Datum) -> Result<bool>;
    fn less_than_or_eq(&mut self, reference: &BoundReference, literal: &Datum) -> Result<bool>;
    fn greater_than(&mut self, reference: &BoundReference, literal: &Datum) -> Result<bool>;
    fn greater_than_or_eq(&mut self, reference: &BoundReference, literal: &Datum) -> Result<bool>;
    fn eq(&mut self, reference: &BoundReference, literal: &Datum) -> Result<bool>;
    fn not_eq(&mut self, reference: &BoundReference, literal: &Datum) -> Result<bool>;
    fn starts_with(&mut self, reference: &BoundReference, literal: &Datum) -> Result<bool>;
    fn not_starts_with(&mut self, reference: &BoundReference, literal: &Datum) -> Result<bool>;
    fn r#in(&mut self, reference: &BoundReference, literals: &FnvHashSet<Datum>) -> Result<bool>;
    fn not_in(&mut self, reference: &BoundReference, literals: &FnvHashSet<Datum>) -> Result<bool>;
}

#[cfg(test)]
mod tests {
    use crate::expr::visitors::bound_predicate_evaluator::BoundPredicateEvaluator;
    use crate::expr::Predicate::{AlwaysFalse, AlwaysTrue};
    use crate::expr::{Bind, BoundReference, Predicate};
    use crate::spec::{Datum, Schema, SchemaRef};
    use fnv::FnvHashSet;
    use std::ops::Not;
    use std::sync::Arc;

    struct TestEvaluator {}
    impl BoundPredicateEvaluator for TestEvaluator {
        fn is_null(&mut self, _reference: &BoundReference) -> crate::Result<bool> {
            Ok(true)
        }

        fn not_null(&mut self, _reference: &BoundReference) -> crate::Result<bool> {
            Ok(false)
        }

        fn is_nan(&mut self, _reference: &BoundReference) -> crate::Result<bool> {
            Ok(true)
        }

        fn not_nan(&mut self, _reference: &BoundReference) -> crate::Result<bool> {
            Ok(false)
        }

        fn less_than(
            &mut self,
            _reference: &BoundReference,
            _literal: &Datum,
        ) -> crate::Result<bool> {
            Ok(true)
        }

        fn less_than_or_eq(
            &mut self,
            _reference: &BoundReference,
            _literal: &Datum,
        ) -> crate::Result<bool> {
            Ok(false)
        }

        fn greater_than(
            &mut self,
            _reference: &BoundReference,
            _literal: &Datum,
        ) -> crate::Result<bool> {
            Ok(true)
        }

        fn greater_than_or_eq(
            &mut self,
            _reference: &BoundReference,
            _literal: &Datum,
        ) -> crate::Result<bool> {
            Ok(false)
        }

        fn eq(&mut self, _reference: &BoundReference, _literal: &Datum) -> crate::Result<bool> {
            Ok(true)
        }

        fn not_eq(&mut self, _reference: &BoundReference, _literal: &Datum) -> crate::Result<bool> {
            Ok(false)
        }

        fn starts_with(
            &mut self,
            _reference: &BoundReference,
            _literal: &Datum,
        ) -> crate::Result<bool> {
            Ok(true)
        }

        fn not_starts_with(
            &mut self,
            _reference: &BoundReference,
            _literal: &Datum,
        ) -> crate::Result<bool> {
            Ok(false)
        }

        fn r#in(
            &mut self,
            _reference: &BoundReference,
            _literals: &FnvHashSet<Datum>,
        ) -> crate::Result<bool> {
            Ok(true)
        }

        fn not_in(
            &mut self,
            _reference: &BoundReference,
            _literals: &FnvHashSet<Datum>,
        ) -> crate::Result<bool> {
            Ok(false)
        }
    }

    fn create_test_schema() -> SchemaRef {
        let schema = Schema::builder().build().unwrap();

        let schema_arc = Arc::new(schema);
        schema_arc.clone()
    }

    #[test]
    fn test_default_default_always_true() {
        let predicate = Predicate::AlwaysTrue;
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = test_evaluator.visit(&bound_predicate);

        assert_eq!(result.unwrap(), true);
    }

    #[test]
    fn test_default_default_always_false() {
        let predicate = Predicate::AlwaysFalse;
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = test_evaluator.visit(&bound_predicate);

        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_default_default_logical_and() {
        let predicate = AlwaysTrue.and(AlwaysFalse);
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = test_evaluator.visit(&bound_predicate);

        assert_eq!(result.unwrap(), false);

        let predicate = AlwaysFalse.and(AlwaysFalse);
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = test_evaluator.visit(&bound_predicate);

        assert_eq!(result.unwrap(), false);

        let predicate = AlwaysTrue.and(AlwaysTrue);
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = test_evaluator.visit(&bound_predicate);

        assert_eq!(result.unwrap(), true);
    }

    #[test]
    fn test_default_default_logical_or() {
        let predicate = AlwaysTrue.or(AlwaysFalse);
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = test_evaluator.visit(&bound_predicate);

        assert_eq!(result.unwrap(), true);

        let predicate = AlwaysFalse.or(AlwaysFalse);
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = test_evaluator.visit(&bound_predicate);

        assert_eq!(result.unwrap(), false);

        let predicate = AlwaysTrue.or(AlwaysTrue);
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = test_evaluator.visit(&bound_predicate);

        assert_eq!(result.unwrap(), true);
    }

    #[test]
    fn test_default_default_not() {
        let predicate = AlwaysFalse.not();
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = test_evaluator.visit(&bound_predicate);

        assert_eq!(result.unwrap(), true);

        let predicate = AlwaysTrue.not();
        let bound_predicate = predicate.bind(create_test_schema(), false).unwrap();

        let mut test_evaluator = TestEvaluator {};

        let result = test_evaluator.visit(&bound_predicate);

        assert_eq!(result.unwrap(), false);
    }
}
