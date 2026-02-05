// This test verifies that `cleanup_expired_files` cannot be used with
// higher-ranked trait bounds (HRTB) where Fn (not FnOnce) is required.
// This is a known limitation that should cause CI to fail.

use std::future::Future;
use std::sync::Arc;

use iceberg::spec::TableMetadata;
use iceberg::table::Table;

// Using Fn instead of FnOnce - the closure must be callable multiple times
fn with_hrtb_fn<F, Fut>(f: F)
where
    F: for<'a> Fn(&'a Table, &'a Arc<TableMetadata>) -> Fut,
    Fut: Future<Output = ()>,
{
    let _ = f;
}

#[tokio::test]
async fn cleanup_expired_files_hrtb() {
    with_hrtb_fn(|table, before_metadata| async move {
        let _ = table.cleanup_expired_files(before_metadata).await;
    });
}
