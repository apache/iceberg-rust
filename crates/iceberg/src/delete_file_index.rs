use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

use futures::channel::mpsc::Receiver;
use futures::{StreamExt, TryStreamExt};

use crate::runtime::spawn;
use crate::scan::FileScanTaskDeleteFile;
use crate::spec::DataFile;
use crate::{Error, Result};

type DeleteFileIndexResult = Result<Option<Arc<Vec<FileScanTaskDeleteFile>>>>;

/// Safely shareable on-demand-populated delete file index.
///
/// Constructed during the file plan phase of a table scan from a channel that will
/// receive the details of all delete files that can possibly apply to a scan.
/// Asynchronously retrieves and lazily processes of all the delete files from FileIO
/// concurrently whilst the plan proceeds with collating all of the applicable data files.
/// Awaited on when constructing the resulting FileScanTask for each selected data file
/// in order to populate the list of delete files to include in each `FileScanTask`.
#[derive(Debug, Clone)]
pub(crate) struct DeleteFileIndex {
    files: Arc<RwLock<Option<DeleteFileIndexResult>>>,
}

#[derive(Debug, Clone)]
pub(crate) struct DeleteFileIndexFuture {
    files: Arc<RwLock<Option<DeleteFileIndexResult>>>,
}

impl Future for DeleteFileIndexFuture {
    type Output = DeleteFileIndexResult;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Ok(guard) = self.files.try_read() else {
            return Poll::Pending;
        };

        if let Some(value) = guard.as_ref() {
            Poll::Ready(match value.as_ref() {
                Ok(deletes) => Ok(deletes.clone()),
                Err(err) => Err(Error::new(err.kind(), err.message())),
            })
        } else {
            Poll::Pending
        }
    }
}

impl DeleteFileIndex {
    pub(crate) fn from_receiver(receiver: Receiver<Result<FileScanTaskDeleteFile>>) -> Self {
        let delete_file_stream = receiver.boxed();
        let files = Arc::new(RwLock::new(None));

        // spawn a task to handle accumulating all the DeleteFiles that are streamed into the
        // index through the receiver channel. Update the `None` inside the `RwLock` to a `Some`
        // once the stream has been exhausted so that any consumers awaiting on the Future returned
        // by DeleteFileIndex::get_deletes_for_data_file can proceed
        spawn({
            let files = files.clone();
            async move {
                let _ = spawn(async move {
                    let result = delete_file_stream.try_collect::<Vec<_>>().await;
                    let result = result.map(|files| {
                        if files.is_empty() {
                            None
                        } else {
                            Some(Arc::new(files))
                        }
                    });

                    // Unwrap is ok here since this is the only place where a write lock
                    // can be acquired, so the lock can't already have been poisoned
                    let mut guard = files.write().unwrap();
                    *guard = Some(result);
                })
                .await;
            }
        });

        DeleteFileIndex { files }
    }

    /// Asynchronously determines all the delete files that apply to the provided `DataFile`.
    pub(crate) fn get_deletes_for_data_file(&self, _data_file: &DataFile) -> DeleteFileIndexFuture {
        // TODO: filtering

        DeleteFileIndexFuture {
            files: self.files.clone(),
        }
    }
}
