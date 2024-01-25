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

use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::types::DataType;
use risingwave_pb::expr::ExprNode;

use super::{BoxedExpression, Build};
use crate::expr::Expression;
use crate::Result;

/// A reference to a column in input relation.
#[derive(Debug, Clone)]
pub struct InputRefExpression {
    return_type: DataType,
    idx: usize,
}

#[async_trait::async_trait]
impl Expression for InputRefExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        Ok(input.column_at(self.idx).clone())
    }

    fn input_ref_index(&self) -> Option<usize> {
        Some(self.idx)
    }
}

impl InputRefExpression {
    pub fn new(return_type: DataType, idx: usize) -> Self {
        InputRefExpression { return_type, idx }
    }

    /// Create an [`InputRefExpression`] from a protobuf expression.
    ///
    /// Panics if the protobuf expression is not an input reference.
    pub fn from_prost(prost: &ExprNode) -> Self {
        let ret_type = DataType::from(prost.get_return_type().unwrap());
        let input_col_idx = prost.get_rex_node().unwrap().as_input_ref().unwrap();

        Self {
            return_type: ret_type,
            idx: *input_col_idx as _,
        }
    }

    pub fn index(&self) -> usize {
        self.idx
    }

    pub fn eval_immut(&self, input: &DataChunk) -> Result<ArrayRef> {
        Ok(input.column_at(self.idx).clone())
    }
}

impl Build for InputRefExpression {
    fn build(
        prost: &ExprNode,
        _build_child: impl Fn(&ExprNode) -> Result<BoxedExpression>,
    ) -> Result<Self> {
        Ok(Self::from_prost(prost))
    }
}
