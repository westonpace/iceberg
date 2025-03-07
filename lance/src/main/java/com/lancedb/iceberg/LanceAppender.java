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

package com.lancedb.iceberg;

import com.lancedb.lance.file.LanceFileWriter;
import com.lancedb.lance.file.LanceOutput;
import java.util.HashMap;
import java.util.function.BiFunction;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.PositionOutputStream;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LanceAppender<T> implements FileAppender<T> {

    private static final Logger LOG = LoggerFactory.getLogger(LanceAppender.class);

    // Function to convert from Spark value to Arrow value
    private final BiFunction<VectorSchemaRoot, Object, VectorSchemaRoot> valueToArrow;
    // Function to convert from Spark values to Arrow values
    private final BiFunction<VectorSchemaRoot, Iterator<Object>, VectorSchemaRoot> valuesToArrow;
    // Writer schema
    private final VectorSchemaRoot schema;
    // Output location
    private final PositionOutputStream outputStream;
    // Lance writer
    private final LanceFileWriter writer;

    private long recordCount;

    // Adapter to map what Iceberg provides (PoistionOutputStream) to what
    // Lance expects (LanceOutput)
    public static class LanceOutputAdapter extends LanceOutput {
        private final PositionOutputStream outputStream;

        @Override
        public long tell() throws IOException {
            return this.outputStream.getPos();
        }

        @Override
        public void write(int b) throws IOException {
            this.outputStream.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            this.outputStream.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            this.outputStream.write(b, off, len);
        }

        public LanceOutputAdapter(PositionOutputStream outputStream) {
            this.outputStream = outputStream;
        }

        @Override
        public void close() throws IOException {
            this.outputStream.close();
        }

        @Override
        public void flush() throws IOException {
            this.outputStream.flush();
        }
    }

    public LanceAppender(PositionOutputStream outputStream, VectorSchemaRoot schema, BiFunction<VectorSchemaRoot, Object, VectorSchemaRoot> valueToArrow, BiFunction<VectorSchemaRoot, Iterator<Object>, VectorSchemaRoot> valuesToArrow, BufferAllocator allocator) {
        this.valueToArrow = valueToArrow;
        this.valuesToArrow = valuesToArrow;
        this.schema = schema;
        this.outputStream = outputStream;
        try {
            this.writer = LanceFileWriter.openWithJavaIo(new LanceOutputAdapter(outputStream), allocator, null);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        this.recordCount = 0;
    }

    @Override
    public void add(T datum) {
        // TODO(Iceberg-Lance)
        // This is horribly inefficient.  In practice a writer should be
        // accumulating rows until a reasonably sized batch is accumulated
        // and not just calling writer.write with 1-row sized batches as
        // we are doing here.
        //
        // Ideally "reasonably sized batch" would be defined in "size in
        // bytes" and not "size in rows"
        VectorSchemaRoot batch = valueToArrow.apply(schema, datum);
        try {
            writer.write(batch);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        recordCount += 1;
    }

    // I'm sure there is a better way to convert Iterator<T> to Iterator<Object>
    // but I'm really bad at Java
    class ObjIter<T> implements Iterator<Object> {
        private final Iterator<T> inner;

        public ObjIter(Iterator<T> inner) {
            this.inner = inner;
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public Object next() {
            recordCount += 1;
            return inner.next();
        }
    }

    @Override
    public void addAll(Iterator<T> values) {
        // This is nicer than add but I couldn't figure out how to get this method to
        // be called
        VectorSchemaRoot batch = valuesToArrow.apply(schema, new ObjIter<T>(values));
        try {
            writer.write(batch);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Metrics metrics() {
        // TODO(Lance): Wire up Lance Metrics <-> Iceberg Metrics
        //
        // This will always be a little complicated since Arrow doesn't
        // have a spec for scalars :'(
        return new Metrics(recordCount, new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>());
    }

    @Override
    public long length() {
        // TODO(Lance): This will not report data that's been accumulated but not
        // yet flushed.  It appears the other formats have a better way of
        // reporting this.  Need to add feature to Lance writer.
        try {
            return outputStream.getPos();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            writer.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
