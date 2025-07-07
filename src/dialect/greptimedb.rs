// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
    Value, ValueWithSpan,
};
use crate::dialect::Dialect;
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};
use crate::tokenizer::Token;
use regex::Regex;
use std::sync::LazyLock;

/// A dialect for [GreptimeDB](https://greptime.com/).
#[derive(Debug, Copy, Clone)]
pub struct GreptimeDbDialect;

impl GreptimeDbDialect {
    /// Parse "Range" expr, syntax:
    /// `RANGE [ duration literal | (INTERVAL [interval expr]) ] FILL [ NULL | PREV | LINEAR | x]`
    fn parse_range_expr(&self, parser: &mut Parser, expr: &Expr) -> Result<Expr, ParserError> {
        // consume "RANGE" keyword
        parser.advance_token();

        let range = if parser.consume_token(&Token::LParen) {
            let expr = parser.parse_expr()?;
            parser.expect_token(&Token::RParen)?;
            expr
        } else if let Ok(value) = parser.parse_value() {
            if !is_valid_duration_literal(&value) {
                return Err(ParserError::ParserError(format!(
                    r#"Expected valid duration literal, found: "{}""#,
                    value
                )));
            }
            Expr::Value(value)
        } else {
            return parser.expected_ref(
                "duration literal or interval expr",
                parser.get_current_token(),
            );
        };

        let fill = if parser.parse_keyword(Keyword::FILL) {
            Value::SingleQuotedString(parser.next_token().to_string())
        } else {
            Value::SingleQuotedString(String::new())
        };

        // TODO(LFC): rewrite it
        
        // Recursively rewrite function nested in expr to range function when RANGE keyword appear in Expr
        // Treat Function Argument as scalar function, not execute rewrite
        // follow the pattern of `range_fn(func, range, fill)`
        // if `fill` is `None`, the last parameter will be a empty single quoted string for placeholder
        // rate(metrics) RANGE '5m'            ->    range_fn(rate(metrics), '5m', '')
        // rate()        RANGE '5m' FILL MAX   ->    range_fn(rate(), '5m', 'MAX')
        let mut rewrite_count = 0;
        let expr = rewrite_calculation_expr(&expr, false, &mut |e: &Expr| {
            if matches!(e, Expr::Function(..)) {
                let args = vec![
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e.clone())),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(range.clone())),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(fill.clone().into()))),
                ];
                let range_func = Function {
                    name: vec![Ident::new("range_fn")].into(),
                    over: None,
                    filter: None,
                    null_treatment: None,
                    args: FunctionArguments::List(FunctionArgumentList {
                        duplicate_treatment: None,
                        args,
                        clauses: vec![],
                    }),
                    parameters: FunctionArguments::None,
                    within_group: vec![],
                    uses_odbc_syntax: false,
                };
                rewrite_count += 1;
                Ok(Some(Expr::Function(range_func)))
            } else {
                Ok(None)
            }
        })?;
        if rewrite_count == 0 {
            return Err(ParserError::ParserError(format!(
                "Can't use the RANGE keyword in Expr {} without function",
                expr
            )));
        }
        Ok(expr)
    }
}

/// Recursively rewrite a nested calculation `Expr`
///
/// The function's return type is `Result<Option<Expr>>>`, where:
///
/// * `Ok(Some(replacement_expr))`: A replacement `Expr` is provided, use replacement `Expr`.
/// * `Ok(None)`: A replacement `Expr` is not provided, use old `Expr`.
/// * `Err(err)`: Any error returned.
fn rewrite_calculation_expr<F>(
    expr: &Expr,
    rewrite_func_expr: bool,
    replacement_fn: &mut F,
) -> Result<Expr, ParserError>
where
    F: FnMut(&Expr) -> Result<Option<Expr>, ParserError>,
{
    match replacement_fn(expr)? {
        Some(replacement) => Ok(replacement),
        None => match expr {
            Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
                left: Box::new(rewrite_calculation_expr(
                    left,
                    rewrite_func_expr,
                    replacement_fn,
                )?),
                op: op.clone(),
                right: Box::new(rewrite_calculation_expr(
                    right,
                    rewrite_func_expr,
                    replacement_fn,
                )?),
            }),
            Expr::Nested(expr) => Ok(Expr::Nested(Box::new(rewrite_calculation_expr(
                expr,
                rewrite_func_expr,
                replacement_fn,
            )?))),
            Expr::Cast {
                kind,
                expr,
                data_type,
                format,
            } => Ok(Expr::Cast {
                kind: kind.clone(),
                expr: Box::new(rewrite_calculation_expr(
                    expr,
                    rewrite_func_expr,
                    replacement_fn,
                )?),
                data_type: data_type.clone(),
                format: format.clone(),
            }),
            // Scalar function `ceil(val)` will be parse as `Expr::Ceil` instead of `Expr::Function`
            Expr::Ceil { expr, field } => Ok(Expr::Ceil {
                expr: Box::new(rewrite_calculation_expr(
                    expr,
                    rewrite_func_expr,
                    replacement_fn,
                )?),
                field: field.clone(),
            }),
            // Scalar function `floor(val)` will be parse as `Expr::Floor` instead of `Expr::Function`
            Expr::Floor { expr, field } => Ok(Expr::Floor {
                expr: Box::new(rewrite_calculation_expr(
                    expr,
                    rewrite_func_expr,
                    replacement_fn,
                )?),
                field: field.clone(),
            }),
            Expr::Function(func) if rewrite_func_expr => {
                let mut func = func.clone();
                if let FunctionArguments::List(args) = &mut func.args {
                    for fn_arg in &mut args.args {
                        if let FunctionArg::Named {
                            arg: FunctionArgExpr::Expr(expr),
                            ..
                        }
                        | FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = fn_arg
                        {
                            *expr =
                                rewrite_calculation_expr(expr, rewrite_func_expr, replacement_fn)?;
                        }
                    }
                }
                Ok(Expr::Function(func))
            }
            expr => Ok(expr.clone()),
        },
    }
}

impl Dialect for GreptimeDbDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_' || ch == '#' || ch == '@'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphabetic()
            || ch.is_ascii_digit()
            || ch == '@'
            || ch == '$'
            || ch == '#'
            || ch == '_'
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn parse_infix(
        &self,
        parser: &mut Parser,
        expr: &Expr,
        _precedence: u8,
    ) -> Option<Result<Expr, ParserError>> {
        log::debug!("dialect parse_infix: expr {:?}", expr);
        if parser.peek_keyword(Keyword::RANGE) {
            Some(self.parse_range_expr(parser, expr))
        } else {
            None
        }
    }

    fn get_next_precedence(&self, parser: &Parser) -> Option<Result<u8, ParserError>> {
        let token = parser.peek_token();
        match token.token {
            Token::Word(w) if w.keyword == Keyword::RANGE => Some(Ok(u8::MAX)),
            _ => None,
        }
    }
}

static DURATION_LITERAL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?x)
^
((?P<y>[0-9]+)y)?
((?P<w>[0-9]+)w)?
((?P<d>[0-9]+)d)?
((?P<h>[0-9]+)h)?
((?P<m>[0-9]+)m)?
((?P<s>[0-9]+)s)?
((?P<ms>[0-9]+)ms)?
$",
    )
    .unwrap_or_else(|e| panic!("{e}"))
});

// Checks if Value is a valid Duration literal.
// Regular Expression Reference: https://github.com/GreptimeTeam/promql-parser/blob/main/src/util/duration.rs
fn is_valid_duration_literal(v: &ValueWithSpan) -> bool {
    match &v.value {
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            !s.is_empty() && DURATION_LITERAL.is_match(s)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_duration_literal() {
        // valid
        vec![
            "1y", "2w", "3d", "4h", "5m", "6s", "7ms", "1y2w3d", "4h30m", "3600ms",
        ]
        .iter()
        .for_each(|x| {
            assert!(
                is_valid_duration_literal(&Value::SingleQuotedString(x.to_string()).into()),
                "{x}"
            )
        });

        // invalid
        vec!["1", "1y1m1d", "-1w", "1.5d", "d", "", "0"]
            .iter()
            .for_each(|x| {
                assert!(
                    !is_valid_duration_literal(&Value::SingleQuotedString(x.to_string()).into()),
                    "{x}"
                )
            });
    }
}
