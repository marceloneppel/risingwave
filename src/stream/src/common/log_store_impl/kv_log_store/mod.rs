// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_connector::sink::log_store::LogStoreFactory;
use risingwave_pb::catalog::Table;
use risingwave_storage::store::NewLocalOptions;
use risingwave_storage::StateStore;

use crate::common::log_store_impl::kv_log_store::buffer::new_log_store_buffer;
use crate::common::log_store_impl::kv_log_store::reader::KvLogStoreReader;
use crate::common::log_store_impl::kv_log_store::serde::LogStoreRowSerde;
use crate::common::log_store_impl::kv_log_store::writer::KvLogStoreWriter;

mod buffer;
mod reader;
mod serde;
#[cfg(test)]
mod test_utils;
mod writer;

type SeqIdType = i32;
type RowOpCodeType = i16;

const FIRST_SEQ_ID: SeqIdType = 0;

/// Readers truncate the offset at the granularity of seq id.
/// None `SeqIdType` means that the whole epoch is truncated.
type ReaderTruncationOffsetType = (u64, Option<SeqIdType>);

pub struct KvLogStoreFactory<S: StateStore> {
    state_store: S,

    table_catalog: Table,

    vnodes: Option<Arc<Bitmap>>,

    max_stream_chunk_count: usize,
}

impl<S: StateStore> KvLogStoreFactory<S> {
    pub fn new(
        state_store: S,
        table_catalog: Table,
        vnodes: Option<Arc<Bitmap>>,
        max_stream_chunk_count: usize,
    ) -> Self {
        Self {
            state_store,
            table_catalog,
            vnodes,
            max_stream_chunk_count,
        }
    }
}

impl<S: StateStore> LogStoreFactory for KvLogStoreFactory<S> {
    type Reader = KvLogStoreReader<S>;
    type Writer = KvLogStoreWriter<S::Local>;

    async fn build(self) -> (Self::Reader, Self::Writer) {
        let table_id = TableId::new(self.table_catalog.id);
        let serde = LogStoreRowSerde::new(&self.table_catalog, self.vnodes);
        let local_state_store = self
            .state_store
            .new_local(NewLocalOptions {
                table_id: TableId {
                    table_id: self.table_catalog.id,
                },
                is_consistent_op: false,
                table_option: TableOption {
                    retention_seconds: None,
                },
                is_replicated: false,
            })
            .await;

        let (tx, rx) = new_log_store_buffer(self.max_stream_chunk_count);

        let reader = KvLogStoreReader::new(table_id, self.state_store, serde.clone(), rx);

        let writer = KvLogStoreWriter::new(table_id, local_state_store, serde, tx);

        (reader, writer)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::buffer::{Bitmap, BitmapBuilder};
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::EpochPair;
    use risingwave_connector::sink::log_store::{
        LogReader, LogStoreFactory, LogStoreReadItem, LogWriter, TruncateOffset,
    };
    use risingwave_hummock_sdk::HummockReadEpoch;
    use risingwave_hummock_test::test_utils::prepare_hummock_test_env;
    use risingwave_storage::store::SyncResult;
    use risingwave_storage::StateStore;

    use crate::common::log_store_impl::kv_log_store::test_utils::{
        calculate_vnode_bitmap, check_rows_eq, check_stream_chunk_eq,
        gen_multi_vnode_stream_chunks, gen_stream_chunk, gen_test_log_store_table,
    };
    use crate::common::log_store_impl::kv_log_store::KvLogStoreFactory;

    #[tokio::test]
    async fn test_basic() {
        for count in 0..20 {
            test_basic_inner(count).await
        }
    }

    async fn test_basic_inner(max_stream_chunk_count: usize) {
        let test_env = prepare_hummock_test_env().await;

        let table = gen_test_log_store_table();

        test_env.register_table(table.clone()).await;

        let stream_chunk1 = gen_stream_chunk(0);
        let stream_chunk2 = gen_stream_chunk(10);
        let bitmap = calculate_vnode_bitmap(stream_chunk1.rows().chain(stream_chunk2.rows()));

        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(Arc::new(bitmap)),
            max_stream_chunk_count,
        );
        let (mut reader, mut writer) = factory.build().await;

        let epoch1 = test_env
            .storage
            .get_pinned_version()
            .version()
            .max_committed_epoch
            + 1;
        writer
            .init(EpochPair::new_test_epoch(epoch1))
            .await
            .unwrap();
        writer.write_chunk(stream_chunk1.clone()).await.unwrap();
        let epoch2 = epoch1 + 1;
        writer.flush_current_epoch(epoch2, false).await.unwrap();
        writer.write_chunk(stream_chunk2.clone()).await.unwrap();
        let epoch3 = epoch2 + 1;
        writer.flush_current_epoch(epoch3, true).await.unwrap();

        test_env.storage.seal_epoch(epoch1, false);
        test_env.storage.seal_epoch(epoch2, true);
        let sync_result: SyncResult = test_env.storage.sync(epoch2).await.unwrap();
        assert!(!sync_result.uncommitted_ssts.is_empty());

        reader.init().await.unwrap();
        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch1);
                assert!(check_stream_chunk_eq(&stream_chunk1, &read_stream_chunk));
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch1);
                assert!(!is_checkpoint)
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch2);
                assert!(check_stream_chunk_eq(&stream_chunk2, &read_stream_chunk));
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch2);
                assert!(is_checkpoint)
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_recovery() {
        for count in 0..20 {
            test_recovery_inner(count).await
        }
    }

    async fn test_recovery_inner(max_stream_chunk_count: usize) {
        let test_env = prepare_hummock_test_env().await;

        let table = gen_test_log_store_table();

        test_env.register_table(table.clone()).await;

        let stream_chunk1 = gen_stream_chunk(0);
        let stream_chunk2 = gen_stream_chunk(10);
        let bitmap = calculate_vnode_bitmap(stream_chunk1.rows().chain(stream_chunk2.rows()));
        let bitmap = Arc::new(bitmap);

        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(bitmap.clone()),
            max_stream_chunk_count,
        );
        let (mut reader, mut writer) = factory.build().await;

        let epoch1 = test_env
            .storage
            .get_pinned_version()
            .version()
            .max_committed_epoch
            + 1;
        writer
            .init(EpochPair::new_test_epoch(epoch1))
            .await
            .unwrap();
        writer.write_chunk(stream_chunk1.clone()).await.unwrap();
        let epoch2 = epoch1 + 1;
        writer.flush_current_epoch(epoch2, false).await.unwrap();
        writer.write_chunk(stream_chunk2.clone()).await.unwrap();
        let epoch3 = epoch2 + 1;
        writer.flush_current_epoch(epoch3, true).await.unwrap();

        test_env.storage.seal_epoch(epoch1, false);

        reader.init().await.unwrap();
        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch1);
                assert!(check_stream_chunk_eq(&stream_chunk1, &read_stream_chunk));
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch1);
                assert!(!is_checkpoint)
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch2);
                assert!(check_stream_chunk_eq(&stream_chunk2, &read_stream_chunk));
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch2);
                assert!(is_checkpoint)
            }
            _ => unreachable!(),
        }

        test_env.commit_epoch(epoch2).await;
        // The truncate does not work because it is after the sync
        reader
            .truncate(TruncateOffset::Barrier { epoch: epoch2 })
            .await
            .unwrap();
        test_env
            .storage
            .try_wait_epoch(HummockReadEpoch::Committed(epoch2))
            .await
            .unwrap();

        // Recovery
        test_env.storage.clear_shared_buffer().await.unwrap();

        // Rebuild log reader and writer in recovery
        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(bitmap),
            max_stream_chunk_count,
        );
        let (mut reader, mut writer) = factory.build().await;
        writer
            .init(EpochPair::new_test_epoch(epoch3))
            .await
            .unwrap();
        reader.init().await.unwrap();
        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch1);
                assert!(check_stream_chunk_eq(&stream_chunk1, &read_stream_chunk));
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch1);
                assert!(!is_checkpoint)
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch2);
                assert!(check_stream_chunk_eq(&stream_chunk2, &read_stream_chunk));
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch2);
                assert!(is_checkpoint)
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_truncate() {
        for count in 2..10 {
            test_truncate_inner(count).await
        }
    }

    async fn test_truncate_inner(max_stream_chunk_count: usize) {
        let test_env = prepare_hummock_test_env().await;

        let table = gen_test_log_store_table();

        test_env.register_table(table.clone()).await;

        let stream_chunk1_1 = gen_stream_chunk(0);
        let stream_chunk1_2 = gen_stream_chunk(10);
        let stream_chunk2 = gen_stream_chunk(20);
        let stream_chunk3 = gen_stream_chunk(20);
        let bitmap = calculate_vnode_bitmap(
            stream_chunk1_1
                .rows()
                .chain(stream_chunk1_2.rows())
                .chain(stream_chunk2.rows())
                .chain(stream_chunk3.rows()),
        );
        let bitmap = Arc::new(bitmap);

        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(bitmap.clone()),
            max_stream_chunk_count,
        );
        let (mut reader, mut writer) = factory.build().await;

        let epoch1 = test_env
            .storage
            .get_pinned_version()
            .version()
            .max_committed_epoch
            + 1;
        writer
            .init(EpochPair::new_test_epoch(epoch1))
            .await
            .unwrap();
        writer.write_chunk(stream_chunk1_1.clone()).await.unwrap();
        writer.write_chunk(stream_chunk1_2.clone()).await.unwrap();
        let epoch2 = epoch1 + 1;
        writer.flush_current_epoch(epoch2, true).await.unwrap();
        writer.write_chunk(stream_chunk2.clone()).await.unwrap();

        test_env.commit_epoch(epoch1).await;

        reader.init().await.unwrap();
        let chunk_id1 = match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    chunk_id,
                },
            ) => {
                assert_eq!(epoch, epoch1);
                assert!(check_stream_chunk_eq(&stream_chunk1_1, &read_stream_chunk));
                chunk_id
            }
            _ => unreachable!(),
        };
        let chunk_id2 = match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    chunk_id,
                },
            ) => {
                assert_eq!(epoch, epoch1);
                assert!(check_stream_chunk_eq(&stream_chunk1_2, &read_stream_chunk));
                chunk_id
            }
            _ => unreachable!(),
        };
        assert!(chunk_id2 > chunk_id1);
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch1);
                assert!(is_checkpoint)
            }
            _ => unreachable!(),
        }

        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch2);
                assert!(check_stream_chunk_eq(&stream_chunk2, &read_stream_chunk));
            }
            _ => unreachable!(),
        }

        // The truncate should work because it is before the flush
        reader
            .truncate(TruncateOffset::Chunk {
                epoch: epoch1,
                chunk_id: chunk_id1,
            })
            .await
            .unwrap();
        let epoch3 = epoch2 + 1;
        writer.flush_current_epoch(epoch3, true).await.unwrap();

        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch2);
                assert!(is_checkpoint)
            }
            _ => unreachable!(),
        }

        // Truncation on epoch1 should work because it is before this sync
        test_env.commit_epoch(epoch2).await;
        test_env
            .storage
            .try_wait_epoch(HummockReadEpoch::Committed(epoch2))
            .await
            .unwrap();

        // Recovery
        test_env.storage.clear_shared_buffer().await.unwrap();

        // Rebuild log reader and writer in recovery
        let factory = KvLogStoreFactory::new(
            test_env.storage.clone(),
            table.clone(),
            Some(bitmap),
            max_stream_chunk_count,
        );
        let (mut reader, mut writer) = factory.build().await;

        writer
            .init(EpochPair::new_test_epoch(epoch3))
            .await
            .unwrap();

        writer.write_chunk(stream_chunk3.clone()).await.unwrap();

        reader.init().await.unwrap();
        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch1);
                assert!(check_stream_chunk_eq(&stream_chunk1_2, &read_stream_chunk));
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch1);
                assert!(is_checkpoint)
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch2);
                assert!(check_stream_chunk_eq(&stream_chunk2, &read_stream_chunk));
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch2);
                assert!(is_checkpoint)
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (
                epoch,
                LogStoreReadItem::StreamChunk {
                    chunk: read_stream_chunk,
                    ..
                },
            ) => {
                assert_eq!(epoch, epoch3);
                assert!(check_stream_chunk_eq(&stream_chunk3, &read_stream_chunk));
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_update_vnode_recover() {
        let test_env = prepare_hummock_test_env().await;

        let table = gen_test_log_store_table();

        test_env.register_table(table.clone()).await;

        fn build_bitmap(indexes: impl Iterator<Item = usize>) -> Arc<Bitmap> {
            let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT);
            for i in indexes {
                builder.set(i, true);
            }
            Arc::new(builder.finish())
        }

        let vnodes1 = build_bitmap((0..VirtualNode::COUNT).filter(|i| i % 2 == 0));
        let vnodes2 = build_bitmap((0..VirtualNode::COUNT).filter(|i| i % 2 == 1));

        let factory1 =
            KvLogStoreFactory::new(test_env.storage.clone(), table.clone(), Some(vnodes1), 10);
        let factory2 =
            KvLogStoreFactory::new(test_env.storage.clone(), table.clone(), Some(vnodes2), 10);
        let (mut reader1, mut writer1) = factory1.build().await;
        let (mut reader2, mut writer2) = factory2.build().await;

        let epoch1 = test_env
            .storage
            .get_pinned_version()
            .version()
            .max_committed_epoch
            + 1;
        writer1
            .init(EpochPair::new_test_epoch(epoch1))
            .await
            .unwrap();
        writer2
            .init(EpochPair::new_test_epoch(epoch1))
            .await
            .unwrap();
        reader1.init().await.unwrap();
        reader2.init().await.unwrap();
        let [chunk1_1, chunk1_2] = gen_multi_vnode_stream_chunks::<2>(0, 100);
        writer1.write_chunk(chunk1_1.clone()).await.unwrap();
        writer2.write_chunk(chunk1_2.clone()).await.unwrap();
        let epoch2 = epoch1 + 1;
        writer1.flush_current_epoch(epoch2, false).await.unwrap();
        writer2.flush_current_epoch(epoch2, false).await.unwrap();
        let [chunk2_1, chunk2_2] = gen_multi_vnode_stream_chunks::<2>(200, 100);
        writer1.write_chunk(chunk2_1.clone()).await.unwrap();
        writer2.write_chunk(chunk2_2.clone()).await.unwrap();

        match reader1.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk { chunk, .. }) => {
                assert_eq!(epoch, epoch1);
                assert!(check_stream_chunk_eq(&chunk1_1, &chunk));
            }
            _ => unreachable!(),
        };
        match reader1.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch1);
                assert!(!is_checkpoint);
            }
            _ => unreachable!(),
        }

        match reader2.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk { chunk, .. }) => {
                assert_eq!(epoch, epoch1);
                assert!(check_stream_chunk_eq(&chunk1_2, &chunk));
            }
            _ => unreachable!(),
        }
        match reader2.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch1);
                assert!(!is_checkpoint);
            }
            _ => unreachable!(),
        }

        // Only reader1 will truncate
        reader1
            .truncate(TruncateOffset::Barrier { epoch: epoch1 })
            .await
            .unwrap();

        match reader1.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk { chunk, .. }) => {
                assert_eq!(epoch, epoch2);
                assert!(check_stream_chunk_eq(&chunk2_1, &chunk));
            }
            _ => unreachable!(),
        }
        match reader2.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk { chunk, .. }) => {
                assert_eq!(epoch, epoch2);
                assert!(check_stream_chunk_eq(&chunk2_2, &chunk));
            }
            _ => unreachable!(),
        }

        let epoch3 = epoch2 + 1;
        writer1.flush_current_epoch(epoch3, true).await.unwrap();
        writer2.flush_current_epoch(epoch3, true).await.unwrap();

        match reader1.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch2);
                assert!(is_checkpoint);
            }
            _ => unreachable!(),
        }
        match reader2.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch2);
                assert!(is_checkpoint);
            }
            _ => unreachable!(),
        }

        // Truncation of reader1 on epoch1 should work because it is before this sync
        test_env.storage.seal_epoch(epoch1, false);
        test_env.commit_epoch(epoch2).await;
        test_env
            .storage
            .try_wait_epoch(HummockReadEpoch::Committed(epoch2))
            .await
            .unwrap();

        // Recovery
        test_env.storage.clear_shared_buffer().await.unwrap();

        let vnodes = build_bitmap(0..VirtualNode::COUNT);
        let factory =
            KvLogStoreFactory::new(test_env.storage.clone(), table.clone(), Some(vnodes), 10);
        let (mut reader, mut writer) = factory.build().await;
        writer.init(EpochPair::new(epoch3, epoch2)).await.unwrap();
        reader.init().await.unwrap();
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk { chunk, .. }) => {
                assert_eq!(epoch, epoch1);
                assert!(check_stream_chunk_eq(&chunk1_2, &chunk));
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch1);
                assert!(!is_checkpoint);
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk { chunk, .. }) => {
                assert_eq!(epoch, epoch2);
                assert!(check_rows_eq(
                    chunk2_1.rows().chain(chunk2_2.rows()),
                    chunk.rows()
                ));
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch2);
                assert!(is_checkpoint);
            }
            _ => unreachable!(),
        }
    }
}
