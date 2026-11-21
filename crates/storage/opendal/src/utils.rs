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

pub(crate) fn is_truthy(value: &str) -> bool {
    ["true", "t", "1", "on"].contains(&value.to_lowercase().as_str())
}

/// Convert an opendal error into an iceberg error.
pub(crate) fn from_opendal_error(e: opendal::Error) -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::Unexpected,
        "Failure in doing io operation",
    )
    .with_source(e)
}

/// `s` with its userinfo masked as `***` and its query and fragment dropped, for error messages.
///
/// No URL parsing is done, so a raw `/` or `://` in a password cannot move the mask. When an
/// `@` follows the first `?` or `#`, only the scheme is kept.
pub(crate) fn redact_path(s: &str) -> String {
    // Only a real scheme is kept; anything else before `://` may be userinfo.
    let rest = match s.split_once("://") {
        Some((scheme, rest))
            if scheme
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "+-.".contains(c)) =>
        {
            rest
        }
        _ => s,
    };
    let scheme = &s[..s.len() - rest.len()];
    let end = rest.find(['?', '#']).unwrap_or(rest.len());
    if rest[end..].contains('@') {
        return format!("{scheme}***");
    }
    match rest[..end].rsplit_once('@') {
        Some((_, after)) => format!("{scheme}***@{after}"),
        None => format!("{scheme}{}", &rest[..end]),
    }
}

#[cfg(test)]
mod tests {
    use super::redact_path;

    #[test]
    fn test_redact_path() {
        let cases = [
            (
                "s3://alice:SECRET@bucket/t/f.parquet",
                "s3://***@bucket/t/f.parquet",
            ),
            (
                "s3://bucket/t/f.parquet?X-Amz-Signature=abc",
                "s3://bucket/t/f.parquet",
            ),
            ("s3://bucket/t/f.parquet#frag", "s3://bucket/t/f.parquet"),
            ("s3://alice:p?w#d@bucket/f?sig=x#frag", "s3://***"),
            ("s3://alice:p@w@bucket/f", "s3://***@bucket/f"),
            ("s3://bucket/f?user=a@b", "s3://***"),
            (
                "gs://bucket/o?GoogleAccessId=sa@p.iam.gserviceaccount.com&Expires=1&Signature=SIG",
                "gs://***",
            ),
            (
                "s3://bucket/f?X-Amz-Credential=a@b&X-Amz-Signature=SECRET",
                "s3://***",
            ),
            (
                "abfss://@acct.dfs.core.windows.net/p?sv=1&sig=x",
                "abfss://***@acct.dfs.core.windows.net/p",
            ),
            ("hf://datasets/u/r@main/f", "hf://***@main/f"),
            ("s3://bucket/t/f.parquet", "s3://bucket/t/f.parquet"),
            ("s3://", "s3://"),
            ("not a url", "not a url"),
            ("", ""),
        ];

        let actual: Vec<String> = cases.iter().map(|(input, _)| redact_path(input)).collect();
        let expected: Vec<&str> = cases.iter().map(|(_, expected)| *expected).collect();
        assert_eq!(actual, expected);
    }
}
