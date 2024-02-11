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

use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::Duration;

use arrow_schema::{Field, Fields, Schema};
use await_tree::InstrumentAwait;
use cfg_or_panic::cfg_or_panic;
use risingwave_common::array::{ArrayError, ArrayRef, DataChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use risingwave_pb::expr::ExprNode;
use risingwave_udf::ArrowFlightUdfClient;
use thiserror_ext::AsReport;

use super::{BoxedExpression, Build};
use crate::expr::Expression;
use crate::{bail, Result};

#[derive(Debug)]
pub struct UdfExpression {
    children: Vec<BoxedExpression>,
    arg_types: Vec<DataType>,
    return_type: DataType,
    #[allow(dead_code)]
    arg_schema: Arc<Schema>,
    client: Arc<ArrowFlightUdfClient>,
    identifier: String,
    span: await_tree::Span,
    /// Number of remaining successful calls until retry is enabled.
    /// If non-zero, we will not retry on connection errors to prevent blocking the stream.
    /// On each connection error, the count will be reset to `INITIAL_RETRY_COUNT`.
    /// On each successful call, the count will be decreased by 1.
    /// See <https://github.com/risingwavelabs/risingwave/issues/13791>.
    disable_retry_count: AtomicU8,
    timeout: Option<Duration>,
}

const INITIAL_RETRY_COUNT: u8 = 16;

#[async_trait::async_trait]
impl Expression for UdfExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    #[cfg_or_panic(not(madsim))]
    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        if input.cardinality() == 0 {
            // early return for empty input
            let mut builder = self.return_type.create_array_builder(input.capacity());
            builder.append_n_null(input.capacity());
            return Ok(builder.finish().into_ref());
        }
        let mut columns = Vec::with_capacity(self.children.len());
        for child in &self.children {
            let array = child.eval(input).await?;
            columns.push(array);
        }
        let chunk = DataChunk::new(columns, input.visibility().clone());
        self.eval_inner(&chunk).await
    }

    #[cfg_or_panic(not(madsim))]
    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let mut columns = Vec::with_capacity(self.children.len());
        for child in &self.children {
            let datum = child.eval_row(input).await?;
            columns.push(datum);
        }
        let arg_row = OwnedRow::new(columns);
        let chunk = DataChunk::from_rows(std::slice::from_ref(&arg_row), &self.arg_types);
        let output_array = self.eval_inner(&chunk).await?;
        Ok(output_array.to_datum())
    }
}

impl UdfExpression {
    async fn eval_inner(&self, input: &DataChunk) -> Result<ArrayRef> {
        // this will drop invisible rows
        let arrow_input = arrow_array::RecordBatch::try_from(input)?;

        let disable_retry_count = self.disable_retry_count.load(Ordering::Relaxed);
        let result = self
            .client
            .call_opt(
                &self.identifier,
                arrow_input,
                self.timeout,
                disable_retry_count == 0,
            )
            .instrument_await(self.span.clone())
            .await;
        let disable_retry_count = self.disable_retry_count.load(Ordering::Relaxed);
        let connection_error = matches!(&result, Err(e) if e.is_connection_error());
        if connection_error && disable_retry_count != INITIAL_RETRY_COUNT {
            // reset count on connection error
            self.disable_retry_count
                .store(INITIAL_RETRY_COUNT, Ordering::Relaxed);
        } else if !connection_error && disable_retry_count != 0 {
            // decrease count on success, ignore if exchange failed
            _ = self.disable_retry_count.compare_exchange(
                disable_retry_count,
                disable_retry_count - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
        let arrow_output = result?;

        if arrow_output.num_rows() != input.cardinality() {
            bail!(
                "UDF returned {} rows, but expected {}",
                arrow_output.num_rows(),
                input.cardinality(),
            );
        }

        let output = DataChunk::try_from(&arrow_output)?;
        let output = output.uncompact(input.visibility().clone());

        let Some(array) = output.columns().first() else {
            bail!("UDF returned no columns");
        };
        if !array.data_type().equals_datatype(&self.return_type) {
            bail!(
                "UDF returned {:?}, but expected {:?}",
                array.data_type(),
                self.return_type,
            );
        }

        Ok(array.clone())
    }
}

#[cfg_or_panic(not(madsim))]
impl Build for UdfExpression {
    fn build(
        prost: &ExprNode,
        build_child: impl Fn(&ExprNode) -> Result<BoxedExpression>,
    ) -> Result<Self> {
        let return_type = DataType::from(prost.get_return_type().unwrap());
        let udf = prost.get_rex_node().unwrap().as_udf().unwrap();

        // connect to UDF service
        let client = get_or_create_client(&udf.link)?;

        let arg_schema = Arc::new(Schema::new(
            udf.arg_types
                .iter()
                .map::<Result<_>, _>(|t| {
                    Ok(Field::new(
                        "",
                        DataType::from(t).try_into().map_err(|e: ArrayError| {
                            risingwave_udf::Error::unsupported(e.to_report_string())
                        })?,
                        true,
                    ))
                })
                .try_collect::<Fields>()?,
        ));

        Ok(Self {
            children: udf.children.iter().map(build_child).try_collect()?,
            arg_types: udf.arg_types.iter().map(|t| t.into()).collect(),
            return_type,
            arg_schema,
            client,
            identifier: udf.identifier.clone(),
            span: format!("expr_udf_call ({})", udf.identifier).into(),
            disable_retry_count: AtomicU8::new(0),
            timeout: match udf.timeout_ms {
                0 => Some(Duration::from_millis(10000_u64)),
                ms => Some(Duration::from_millis(ms as u64)),
            },
        })
    }
}

#[cfg(not(madsim))]
/// Get or create a client for the given UDF service.
///
/// There is a global cache for clients, so that we can reuse the same client for the same service.
pub(crate) fn get_or_create_client(link: &str) -> Result<Arc<ArrowFlightUdfClient>> {
    static CLIENTS: LazyLock<Mutex<HashMap<String, Weak<ArrowFlightUdfClient>>>> =
        LazyLock::new(Default::default);
    let mut clients = CLIENTS.lock().unwrap();
    if let Some(client) = clients.get(link).and_then(|c| c.upgrade()) {
        // reuse existing client
        Ok(client)
    } else {
        // create new client
        let client = Arc::new(ArrowFlightUdfClient::connect_lazy(link)?);
        clients.insert(link.into(), Arc::downgrade(&client));
        Ok(client)
    }
}
