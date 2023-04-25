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

package com.risingwave.sourcenode.core;

import com.risingwave.sourcenode.common.DbzConnectorConfig;
import com.risingwave.sourcenode.types.CdcChunk;
import io.grpc.Context;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** handler for starting a debezium source connectors api for rust */
public class DbzSourceHandlerIpc {
    static final Logger LOG = LoggerFactory.getLogger(DbzSourceHandlerIpc.class);

    private final DbzConnectorConfig config;
    private final DbzCdcEngineRunnerIpc runner;

    public DbzSourceHandlerIpc(DbzConnectorConfig config) throws Exception {
        this.config = config;
        this.runner = DbzCdcEngineRunnerIpc.newCdcEngineRunnerIpc(config);
    }

    public void startSource() {
        if (runner == null) {
            return;
        }
        try {
            // Start the engine
            runner.start();
            LOG.info("Start consuming events of table {}", config.getSourceId());
        } catch (Throwable t) {
            LOG.error("Cdc engine failed.", t);
        }
    }

    public CdcChunk getChunk() {
        if (!runner.isRunning()) {
            return null;
        }
        try {
            if (Context.current().isCancelled()) {
                LOG.info(
                        "Engine#{}: Connection broken detected, stop the engine",
                        config.getSourceId());
                runner.stop();
                return null;
            }
            return runner.getEngine().getOutputChannel().poll(500, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOG.error("Poll engine output channel fail. ", e);
            return null;
        }
    }
}
