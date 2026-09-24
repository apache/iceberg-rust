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

pub(super) fn contains_name(names: &[String], name: &str) -> bool {
    names.iter().any(|candidate| candidate == name)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_contains_name_matches_aliases_without_case_folding() {
        let names = vec!["current_name".to_string(), "old_name".to_string()];

        assert!(super::contains_name(&names, "current_name"));
        assert!(super::contains_name(&names, "old_name"));
        assert!(!super::contains_name(&names, "OLD_NAME"));
        assert!(!super::contains_name(&names, "missing"));
    }
}
