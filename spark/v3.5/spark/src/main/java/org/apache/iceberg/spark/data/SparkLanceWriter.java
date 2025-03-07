/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.spark.data;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.iceberg.arrow.vectorized.ColumnarBatch;
import org.apache.spark.sql.catalyst.InternalRow;

import java.util.Iterator;
import org.apache.spark.sql.types.StructType;

// The code in here converts from Spark to Arrow.  However, it uses a
// version of ArrowUtils and ArrowWriter that I ported from spark-sql's
// .scala implementation.
//
// The main reason I had to resort to this is that I could not figure out
// how to use the implementation from spark-sql.  Either methods would be
// mysteriously missing at runtime (e.g. no such method root()) or they
// would be using / not using the shaded arrow imports correctly (e.g. this
// method expects org.apache.arrow and not org.apache.iceberg.relocated.arrow)
//
// I'm sure someone with better gradle skills than me could figure this out
public class SparkLanceWriter {

    public static BufferAllocator rootAllocator = new RootAllocator();

    public static VectorSchemaRoot schemaToArrow(Object schema) {
        StructType sparkSchema = (StructType) schema;
        Schema arrowSchema = ArrowUtils.toArrowSchema(sparkSchema);
        return VectorSchemaRoot.create(arrowSchema, rootAllocator);
    }

    public static VectorSchemaRoot valueToArrow(VectorSchemaRoot root, Object value) {
        InternalRow row = (InternalRow) value;

        ArrowWriter writer = new ArrowWriter(root);
        writer.write(row);
        writer.finish();
        return root;
    }

    public static VectorSchemaRoot valuesToArrow(VectorSchemaRoot root, Iterator<Object> values) {
        ArrowWriter writer = new ArrowWriter(root);
        while (values.hasNext()) {
            InternalRow row = (InternalRow) values.next();
            writer.write(row);
        }
        writer.finish();
        return root;
    }

}
