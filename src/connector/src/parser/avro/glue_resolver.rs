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

use std::sync::Arc;

use anyhow::Context;
use apache_avro::Schema;
use aws_sdk_glue::Client;
use moka::future::Cache;

use crate::error::ConnectorResult;

#[derive(Debug)]
pub struct GlueSchemaResolver {
    writer_schemas: Cache<uuid::Uuid, Arc<Schema>>,
    glue_client: Client,
}

impl GlueSchemaResolver {
    /// Create a new `GlueSchemaResolver`
    pub fn new(client: Client) -> Self {
        Self {
            writer_schemas: Cache::new(u64::MAX),
            glue_client: client,
        }
    }

    // get the writer schema by id
    pub async fn get(&self, schema_version_id: uuid::Uuid) -> ConnectorResult<Arc<Schema>> {
        if let Some(schema) = self.writer_schemas.get(&schema_version_id).await {
            Ok(schema)
        } else {
            let r = self
                .glue_client
                .get_schema_version()
                .schema_version_id(schema_version_id)
                .send()
                .await
                .context("glue sdk error")?;
            self.parse_and_cache_schema(schema_version_id, r.schema_definition().unwrap())
                .await
        }
    }

    async fn parse_and_cache_schema(
        &self,
        schema_version_id: uuid::Uuid,
        content: &str,
    ) -> ConnectorResult<Arc<Schema>> {
        let schema = Schema::parse_str(content).context("failed to parse avro schema")?;
        let schema = Arc::new(schema);
        self.writer_schemas
            .insert(schema_version_id, Arc::clone(&schema))
            .await;
        Ok(schema)
    }
}
