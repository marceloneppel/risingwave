// Copyright 2024 RisingWave Labs
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

use std::future::Future;

use futures::{pin_mut, Stream};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::row::OwnedRow;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_connector::source::cdc::external::{CdcOffset, ExternalTableReader};

use super::external::ExternalStorageTable;
use crate::executor::backfill::utils::{get_new_pos, iter_chunks};
use crate::executor::{StreamExecutorError, StreamExecutorResult, INVALID_EPOCH};

pub trait UpstreamTableRead {
    fn snapshot_read(
        &self,
        args: SnapshotReadArgs,
        limit: u32,
    ) -> impl Stream<Item = StreamExecutorResult<Option<StreamChunk>>> + Send + '_;

    fn current_binlog_offset(
        &self,
    ) -> impl Future<Output = StreamExecutorResult<Option<CdcOffset>>> + Send + '_;
}

#[derive(Debug, Clone, Default)]
pub struct SnapshotReadArgs {
    pub epoch: u64,
    pub current_pos: Option<OwnedRow>,
    pub ordered: bool,
    pub chunk_size: usize,
    pub pk_in_output_indices: Vec<usize>,
}

impl SnapshotReadArgs {
    pub fn new(
        current_pos: Option<OwnedRow>,
        chunk_size: usize,
        pk_in_output_indices: Vec<usize>,
    ) -> Self {
        Self {
            epoch: INVALID_EPOCH,
            current_pos,
            ordered: false,
            chunk_size,
            pk_in_output_indices,
        }
    }
}

/// A wrapper of upstream table for snapshot read
/// because we need to customize the snapshot read for managed upstream table (e.g. mv, index)
/// and external upstream table.
pub struct UpstreamTableReader<T> {
    inner: T,
}

impl<T> UpstreamTableReader<T> {
    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn new(table: T) -> Self {
        Self { inner: table }
    }
}

impl UpstreamTableReader<ExternalStorageTable> {
    #[try_stream(ok = Option<StreamChunk>, error = StreamExecutorError)]
    pub async fn snapshot_read_full_table(&self, args: SnapshotReadArgs, limit: u32) {
        let mut read_args = args;

        'read_loop: loop {
            let mut read_count: usize = 0;
            let chunk_stream = self.snapshot_read(read_args.clone(), limit);
            let mut current_pk_pos = read_args.current_pos.clone().unwrap_or(OwnedRow::default());
            #[for_await]
            for chunk in chunk_stream {
                let chunk = chunk?;

                match chunk {
                    Some(chunk) => {
                        read_count += chunk.cardinality();
                        current_pk_pos = get_new_pos(&chunk, &read_args.pk_in_output_indices);
                        yield Some(chunk);
                    }
                    None => {
                        // reach the end of the table
                        if read_count < limit as _ {
                            break 'read_loop;
                        } else {
                            // update PK position
                            read_args.current_pos = Some(current_pk_pos);
                            break;
                        }
                    }
                }
            }
        }
    }
}

impl UpstreamTableRead for UpstreamTableReader<ExternalStorageTable> {
    #[try_stream(ok = Option<StreamChunk>, error = StreamExecutorError)]
    async fn snapshot_read(&self, args: SnapshotReadArgs, limit: u32) {
        let primary_keys = self
            .inner
            .pk_indices()
            .iter()
            .map(|idx| {
                let f = &self.inner.schema().fields[*idx];
                f.name.clone()
            })
            .collect_vec();

        tracing::debug!(
            "snapshot_read primary keys: {:?}, current_pos: {:?}",
            primary_keys,
            args.current_pos
        );

        let row_stream = self.inner.table_reader().snapshot_read(
            self.inner.schema_table_name(),
            args.current_pos,
            primary_keys,
            limit,
        );

        pin_mut!(row_stream);

        let mut builder = DataChunkBuilder::new(self.inner.schema().data_types(), args.chunk_size);
        let chunk_stream = iter_chunks(row_stream, &mut builder);
        #[for_await]
        for chunk in chunk_stream {
            yield Some(chunk?);
        }
        yield None;
    }

    async fn current_binlog_offset(&self) -> StreamExecutorResult<Option<CdcOffset>> {
        let binlog = self.inner.table_reader().current_cdc_offset();
        let binlog = binlog.await?;
        Ok(Some(binlog))
    }
}
