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

//! Error handlers for REST catalog operations.
//!
//! This module provides error handlers that convert HTTP error responses into
//! semantic Iceberg errors. Each handler interprets HTTP status codes and error
//! response types in the context of specific catalog operations (namespace, table,
//! view, commit, OAuth).
//!
//! Error handlers follow a strategy pattern with delegation to provide operation-specific
//! error semantics while reusing common error handling logic.

use iceberg::{Error, ErrorKind};
use reqwest::StatusCode;

use crate::types::ErrorResponse;

/// Trait for handling REST API error responses and converting to semantic errors.
pub trait ErrorHandler: Send + Sync {
    /// Process an error response and convert to iceberg::Error.
    fn handle(&self, code: StatusCode, response: &ErrorResponse) -> Error;
}

/// Default error handler for common HTTP error responses.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultErrorHandler;

impl ErrorHandler for DefaultErrorHandler {
    fn handle(&self, code: StatusCode, response: &ErrorResponse) -> Error {
        let error_type = &response.error.r#type;
        let message = &response.error.message;

        match code {
            StatusCode::BAD_REQUEST => {
                // Check error.type for special cases
                if error_type.contains("IllegalArgumentException") {
                    Error::new(ErrorKind::DataInvalid, message)
                        .with_context("type", error_type.clone())
                        .with_context("code", code.as_u16().to_string())
                } else {
                    Error::new(ErrorKind::BadRequest, message)
                        .with_context("type", error_type.clone())
                        .with_context("code", code.as_u16().to_string())
                }
            }
            StatusCode::UNAUTHORIZED => Error::new(ErrorKind::NotAuthorized, message)
                .with_context("type", error_type.clone())
                .with_context("code", code.as_u16().to_string()),
            StatusCode::FORBIDDEN => Error::new(ErrorKind::Forbidden, message)
                .with_context("type", error_type.clone())
                .with_context("code", code.as_u16().to_string()),
            StatusCode::INTERNAL_SERVER_ERROR => Error::new(ErrorKind::Unexpected, message)
                .with_context("type", error_type.clone())
                .with_context("code", code.as_u16().to_string()),
            StatusCode::NOT_IMPLEMENTED => Error::new(ErrorKind::FeatureUnsupported, message)
                .with_context("type", error_type.clone())
                .with_context("code", code.as_u16().to_string()),
            StatusCode::SERVICE_UNAVAILABLE => Error::new(ErrorKind::ServiceUnavailable, message)
                .with_context("type", error_type.clone())
                .with_context("code", code.as_u16().to_string()),
            _ => {
                // Generic REST error for unhandled status codes
                Error::new(ErrorKind::Unexpected, message)
                    .with_context("type", error_type.clone())
                    .with_context("code", code.as_u16().to_string())
            }
        }
    }
}

/// Error handler for namespace operations (list, create, get, update).
#[derive(Debug, Default, Clone, Copy)]
pub struct NamespaceErrorHandler;

impl ErrorHandler for NamespaceErrorHandler {
    fn handle(&self, code: StatusCode, response: &ErrorResponse) -> Error {
        let error_type = &response.error.r#type;
        let message = &response.error.message;

        match code {
            StatusCode::BAD_REQUEST => {
                // Check for NamespaceNotEmptyException
                if error_type.contains("NamespaceNotEmptyException") {
                    Error::new(ErrorKind::NamespaceNotEmpty, message)
                        .with_context("type", error_type.clone())
                        .with_context("code", code.as_u16().to_string())
                } else {
                    // Delegate to default handler
                    DefaultErrorHandler.handle(code, response)
                }
            }
            StatusCode::NOT_FOUND => Error::new(ErrorKind::NamespaceNotFound, message)
                .with_context("type", error_type.clone())
                .with_context("code", code.as_u16().to_string()),
            StatusCode::CONFLICT => Error::new(ErrorKind::NamespaceAlreadyExists, message)
                .with_context("type", error_type.clone())
                .with_context("code", code.as_u16().to_string()),
            StatusCode::UNPROCESSABLE_ENTITY => Error::new(ErrorKind::Unexpected, message)
                .with_context("type", error_type.clone())
                .with_context("code", code.as_u16().to_string()),
            _ => DefaultErrorHandler.handle(code, response),
        }
    }
}

/// Error handler for drop namespace operations.
#[derive(Debug, Default, Clone, Copy)]
pub struct DropNamespaceErrorHandler;

impl ErrorHandler for DropNamespaceErrorHandler {
    fn handle(&self, code: StatusCode, response: &ErrorResponse) -> Error {
        match code {
            StatusCode::CONFLICT => {
                // For drop operations, 409 specifically means namespace not empty
                Error::new(ErrorKind::NamespaceNotEmpty, &response.error.message)
                    .with_context("type", response.error.r#type.clone())
                    .with_context("code", code.as_u16().to_string())
            }
            _ => NamespaceErrorHandler.handle(code, response),
        }
    }
}

/// Error handler for table operations (list, create, load, rename).
#[derive(Debug, Default, Clone, Copy)]
pub struct TableErrorHandler;

impl ErrorHandler for TableErrorHandler {
    fn handle(&self, code: StatusCode, response: &ErrorResponse) -> Error {
        let error_type = &response.error.r#type;
        let message = &response.error.message;

        match code {
            StatusCode::NOT_FOUND => {
                // Disambiguate based on error type
                if error_type.contains("NoSuchNamespaceException") {
                    Error::new(ErrorKind::NamespaceNotFound, message)
                        .with_context("type", error_type.clone())
                        .with_context("code", code.as_u16().to_string())
                } else {
                    Error::new(ErrorKind::TableNotFound, message)
                        .with_context("type", error_type.clone())
                        .with_context("code", code.as_u16().to_string())
                }
            }
            StatusCode::CONFLICT => Error::new(ErrorKind::TableAlreadyExists, message)
                .with_context("type", error_type.clone())
                .with_context("code", code.as_u16().to_string()),
            _ => DefaultErrorHandler.handle(code, response),
        }
    }
}

/// Error handler for table commit operations (update_table, commit_table).
#[derive(Debug, Default, Clone, Copy)]
pub struct TableCommitHandler;

impl ErrorHandler for TableCommitHandler {
    fn handle(&self, code: StatusCode, response: &ErrorResponse) -> Error {
        let error_type = &response.error.r#type;
        let message = &response.error.message;

        match code {
            StatusCode::NOT_FOUND => Error::new(ErrorKind::TableNotFound, message)
                .with_context("type", error_type.clone())
                .with_context("code", code.as_u16().to_string()),
            StatusCode::CONFLICT => {
                // Optimistic locking failure - can retry
                Error::new(ErrorKind::CatalogCommitConflicts, message)
                    .with_context("type", error_type.clone())
                    .with_context("code", code.as_u16().to_string())
                    .with_retryable(true)
            }
            // Special handling for commit operations - 5xx means unknown state
            StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT => Error::new(
                ErrorKind::CommitStateUnknown,
                format!(
                    "Service failed during commit ({}): {}. \
                    Commit state is unknown. Manual verification may be needed.",
                    code.as_u16(),
                    message
                ),
            )
            .with_context("type", error_type.clone())
            .with_context("code", code.as_u16().to_string()),
            // Note: NOT marked as retryable - requires manual verification
            _ => DefaultErrorHandler.handle(code, response),
        }
    }
}

/// Error handler for OAuth token exchange operations.
#[derive(Debug, Default, Clone, Copy)]
pub struct OAuthErrorHandler;

impl ErrorHandler for OAuthErrorHandler {
    fn handle(&self, code: StatusCode, response: &ErrorResponse) -> Error {
        let error_type = &response.error.r#type;
        let message = &response.error.message;

        // OAuth errors use the error.type field for error classification
        match error_type.as_str() {
            "invalid_client" => Error::new(ErrorKind::NotAuthorized, message)
                .with_context("type", error_type.clone())
                .with_context("code", code.as_u16().to_string()),
            "invalid_request"
            | "invalid_grant"
            | "unauthorized_client"
            | "unsupported_grant_type"
            | "invalid_scope" => Error::new(ErrorKind::BadRequest, message)
                .with_context("type", error_type.clone())
                .with_context("code", code.as_u16().to_string()),
            _ => DefaultErrorHandler.handle(code, response),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ErrorModel;

    fn create_error_response(code: u16, error_type: &str, message: &str) -> ErrorResponse {
        ErrorResponse {
            error: ErrorModel {
                message: message.to_string(),
                r#type: error_type.to_string(),
                code,
                stack: None,
            },
        }
    }

    #[test]
    fn test_default_handler_bad_request() {
        let handler = DefaultErrorHandler;
        let response = create_error_response(400, "BadRequestException", "Invalid request");
        let error = handler.handle(StatusCode::BAD_REQUEST, &response);

        assert_eq!(error.kind(), ErrorKind::BadRequest);
        assert_eq!(error.message(), "Invalid request");
    }

    #[test]
    fn test_default_handler_illegal_argument() {
        let handler = DefaultErrorHandler;
        let response = create_error_response(400, "IllegalArgumentException", "Illegal argument");
        let error = handler.handle(StatusCode::BAD_REQUEST, &response);

        assert_eq!(error.kind(), ErrorKind::DataInvalid);
        assert_eq!(error.message(), "Illegal argument");
    }

    #[test]
    fn test_default_handler_unauthorized() {
        let handler = DefaultErrorHandler;
        let response = create_error_response(401, "NotAuthorizedException", "Not authorized");
        let error = handler.handle(StatusCode::UNAUTHORIZED, &response);

        assert_eq!(error.kind(), ErrorKind::NotAuthorized);
    }

    #[test]
    fn test_default_handler_forbidden() {
        let handler = DefaultErrorHandler;
        let response = create_error_response(403, "ForbiddenException", "Forbidden");
        let error = handler.handle(StatusCode::FORBIDDEN, &response);

        assert_eq!(error.kind(), ErrorKind::Forbidden);
    }

    #[test]
    fn test_default_handler_service_unavailable() {
        let handler = DefaultErrorHandler;
        let response = create_error_response(503, "ServiceUnavailableException", "Unavailable");
        let error = handler.handle(StatusCode::SERVICE_UNAVAILABLE, &response);

        assert_eq!(error.kind(), ErrorKind::ServiceUnavailable);
    }

    #[test]
    fn test_namespace_handler_not_found() {
        let handler = NamespaceErrorHandler;
        let response =
            create_error_response(404, "NoSuchNamespaceException", "Namespace not found");
        let error = handler.handle(StatusCode::NOT_FOUND, &response);

        assert_eq!(error.kind(), ErrorKind::NamespaceNotFound);
    }

    #[test]
    fn test_namespace_handler_conflict() {
        let handler = NamespaceErrorHandler;
        let response = create_error_response(409, "AlreadyExistsException", "Namespace exists");
        let error = handler.handle(StatusCode::CONFLICT, &response);

        assert_eq!(error.kind(), ErrorKind::NamespaceAlreadyExists);
    }

    #[test]
    fn test_drop_namespace_handler_conflict() {
        let handler = DropNamespaceErrorHandler;
        let response =
            create_error_response(409, "NamespaceNotEmptyException", "Namespace not empty");
        let error = handler.handle(StatusCode::CONFLICT, &response);

        assert_eq!(error.kind(), ErrorKind::NamespaceNotEmpty);
    }

    #[test]
    fn test_table_handler_not_found_table() {
        let handler = TableErrorHandler;
        let response = create_error_response(404, "NoSuchTableException", "Table not found");
        let error = handler.handle(StatusCode::NOT_FOUND, &response);

        assert_eq!(error.kind(), ErrorKind::TableNotFound);
    }

    #[test]
    fn test_table_handler_not_found_namespace() {
        let handler = TableErrorHandler;
        let response =
            create_error_response(404, "NoSuchNamespaceException", "Namespace not found");
        let error = handler.handle(StatusCode::NOT_FOUND, &response);

        assert_eq!(error.kind(), ErrorKind::NamespaceNotFound);
    }

    #[test]
    fn test_table_handler_conflict() {
        let handler = TableErrorHandler;
        let response = create_error_response(409, "AlreadyExistsException", "Table exists");
        let error = handler.handle(StatusCode::CONFLICT, &response);

        assert_eq!(error.kind(), ErrorKind::TableAlreadyExists);
    }

    #[test]
    fn test_table_commit_handler_conflict() {
        let handler = TableCommitHandler;
        let response = create_error_response(409, "CommitFailedException", "Commit conflict");
        let error = handler.handle(StatusCode::CONFLICT, &response);

        assert_eq!(error.kind(), ErrorKind::CatalogCommitConflicts);
        assert!(error.retryable());
    }

    #[test]
    fn test_table_commit_handler_500() {
        let handler = TableCommitHandler;
        let response = create_error_response(500, "InternalServerError", "Server error");
        let error = handler.handle(StatusCode::INTERNAL_SERVER_ERROR, &response);

        assert_eq!(error.kind(), ErrorKind::CommitStateUnknown);
        assert!(!error.retryable());
    }

    #[test]
    fn test_table_commit_handler_503() {
        let handler = TableCommitHandler;
        let response =
            create_error_response(503, "ServiceUnavailableException", "Service unavailable");
        let error = handler.handle(StatusCode::SERVICE_UNAVAILABLE, &response);

        assert_eq!(error.kind(), ErrorKind::CommitStateUnknown);
    }

    #[test]
    fn test_oauth_handler_invalid_client() {
        let handler = OAuthErrorHandler;
        let response = create_error_response(401, "invalid_client", "Invalid client");
        let error = handler.handle(StatusCode::UNAUTHORIZED, &response);

        assert_eq!(error.kind(), ErrorKind::NotAuthorized);
    }

    #[test]
    fn test_oauth_handler_invalid_request() {
        let handler = OAuthErrorHandler;
        let response = create_error_response(400, "invalid_request", "Invalid request");
        let error = handler.handle(StatusCode::BAD_REQUEST, &response);

        assert_eq!(error.kind(), ErrorKind::BadRequest);
    }
}
