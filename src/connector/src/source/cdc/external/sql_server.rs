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

use std::cmp::Ordering;
use std::collections::HashMap;

use anyhow::Context;
use futures::stream::BoxStream;
use futures_async_stream::try_stream;
use risingwave_common::catalog::Schema;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::DatumRef;
use serde_derive::{Deserialize, Serialize};
use thiserror_ext::AsReport;

use crate::error::{ConnectorError, ConnectorResult};
#[cfg(not(madsim))]
use crate::source::cdc::external::{
    CdcOffset, CdcOffsetParseFunc, DebeziumOffset, ExternalTableConfig, ExternalTableReader,
    SchemaTableName,
};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SqlServerOffset {
    pub txid: i64,
    // In SqlServer, an LSN is a 64-bit integer, repres enting a byte position in the write-ahead log stream.
    // It is printed as two hexadecimal numbers of up to 8 digits each, separated by a slash; for example, 16/B374D848
    pub lsn: u64,
}

// only compare the lsn field
impl PartialOrd for SqlServerOffset {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.lsn.partial_cmp(&other.lsn)
    }
}

impl SqlServerOffset {
    pub fn parse_debezium_offset(offset: &str) -> ConnectorResult<Self> {
        let dbz_offset: DebeziumOffset = serde_json::from_str(offset)
            .with_context(|| format!("invalid upstream offset: {}", offset))?;

        Ok(Self {
            txid: dbz_offset
                .source_offset
                .txid
                .context("invalid postgres txid")?,
            lsn: dbz_offset
                .source_offset
                .lsn
                .context("invalid postgres lsn")?,
        })
    }
}

#[derive(Debug)]
pub struct SqlServerExternalTableReader {
    config: ExternalTableConfig,
    rw_schema: Schema,
    field_names: String,
    // client: tokio::sync::Mutex<tokio_postgres::Client>,
}

impl ExternalTableReader for SqlServerExternalTableReader {
    fn get_normalized_table_name(&self, table_name: &SchemaTableName) -> String {
        format!(
            "\"{}\".\"{}\"",
            table_name.schema_name, table_name.table_name
        )
    }

    async fn current_cdc_offset(&self) -> ConnectorResult<CdcOffset> {
        todo!("WKXTODO: implement current_cdc_offset");
    }

    fn snapshot_read(
        &self,
        table_name: SchemaTableName,
        start_pk: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) -> BoxStream<'_, ConnectorResult<OwnedRow>> {
        todo!("WKXTODO: implement snapshot_read_inner");
    }
}

impl SqlServerExternalTableReader {
    pub async fn new(
        properties: HashMap<String, String>,
        rw_schema: Schema,
    ) -> ConnectorResult<Self> {
        tracing::debug!(?rw_schema, "create postgres external table reader");

        let config = serde_json::from_value::<ExternalTableConfig>(
            serde_json::to_value(properties).unwrap(),
        )
        .context("failed to extract postgres connector properties")?;

        let database_url = format!(
            "sqlserver://{}:{}@{}:{}/{}?sslmode={}",
            config.username,
            config.password,
            config.host,
            config.port,
            config.database,
            config.sslmode
        );

        // #[cfg(not(madsim))]
        // let connector = match config.sslmode {
        //     SslMode::Disable => MaybeMakeTlsConnector::NoTls(NoTls),
        //     SslMode::Prefer => match SslConnector::builder(SslMethod::tls()) {
        //         Ok(mut builder) => {
        //             // disable certificate verification for `prefer`
        //             builder.set_verify(SslVerifyMode::NONE);
        //             MaybeMakeTlsConnector::Tls(MakeTlsConnector::new(builder.build()))
        //         }
        //         Err(e) => {
        //             tracing::warn!(error = %e.as_report(), "SSL connector error");
        //             MaybeMakeTlsConnector::NoTls(NoTls)
        //         }
        //     },
        //     SslMode::Require => {
        //         let mut builder = SslConnector::builder(SslMethod::tls())?;
        //         // disable certificate verification for `require`
        //         builder.set_verify(SslVerifyMode::NONE);
        //         MaybeMakeTlsConnector::Tls(MakeTlsConnector::new(builder.build()))
        //     }
        // };
        // #[cfg(madsim)]
        // let connector = NoTls;

        // let (client, connection) = tokio_postgres::connect(&database_url, connector).await?;

        // tokio::spawn(async move {
        //     if let Err(e) = connection.await {
        //         tracing::error!(error = %e.as_report(), "postgres connection error");
        //     }
        // });

        // let field_names = rw_schema
        //     .fields
        //     .iter()
        //     .map(|f| Self::quote_column(&f.name))
        //     .join(",");

        // Ok(Self {
        //     config,
        //     rw_schema,
        //     field_names,
        //     client: tokio::sync::Mutex::new(client),
        // })
        todo!("WKXTODO: implement SqlServerExternalTableReader::new");
    }

    pub fn get_cdc_offset_parser() -> CdcOffsetParseFunc {
        todo!("WKXTODO: implement get_cdc_offset_parser");
    }

    #[try_stream(boxed, ok = OwnedRow, error = ConnectorError)]
    async fn snapshot_read_inner(
        &self,
        table_name: SchemaTableName,
        start_pk_row: Option<OwnedRow>,
        primary_keys: Vec<String>,
        limit: u32,
    ) {
        todo!("WKXTODO: implement snapshot_read_inner");
    }

    // row filter expression: (v1, v2, v3) > ($1, $2, $3)
    fn filter_expression(columns: &[String]) -> String {
        todo!("WKXTODO: implement filter_expression");
    }

    fn quote_column(column: &str) -> String {
        format!("\"{}\"", column)
    }
}
