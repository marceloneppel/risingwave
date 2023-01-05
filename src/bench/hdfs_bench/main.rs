use std::sync::Arc;

use anyhow::Result;
use opendal::services::hdfs;
use opendal::Accessor;
use opendal::Object;
use opendal::Operator;

#[tokio::main]
async fn main() -> Result<()> {
    // Create fs backend builder.
    let mut builder = hdfs::Builder::default();
    // Set the name node for hdfs.
    builder.name_node("hdfs://127.0.0.1:9000");
    // Set the root for hdfs, all operations will happen under this root.
    //
    // NOTE: the root must be absolute path.
    builder.root("/tmp");

    // `Accessor` provides the low level APIs, we will use `Operator` normally.
    let op: Operator = Operator::new(builder.build()?);

    // Create an object handle to start operation on object.
    let _: Object = op.object("test_file");

    Ok(())
}