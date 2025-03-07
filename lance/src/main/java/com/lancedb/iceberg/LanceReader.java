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

import com.lancedb.lance.file.LanceFileReader;
import com.lancedb.lance.file.LanceInput;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Function;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LanceReader implements CloseableIterable<Object> {

    // Underlying Lance reader
    private final LanceFileReader fileReader;
    // Batch size
    private final int recordsPerBatch;
    // Function to convert from Arrow to Spark
    private final Function<VectorSchemaRoot, Object> arrowToSpark;

    // Convert from what Iceberg provides for I/O (SeekableInputStream)
    // to what Lance expects (LanceInput)
    private static class LanceInputAdapter implements LanceInput {
        private final InputFile inputFile;
        private final SeekableInputStream seekableInputStream;

        public LanceInputAdapter(InputFile inputFile) {
            this.inputFile = inputFile;
            this.seekableInputStream = inputFile.newStream();
        }

        @Override
        public long fileSize() throws IOException {
            long fileSize = inputFile.getLength();
            return fileSize;
        }

        @Override
        // TODO(lance): Ideally this function should not have to be synchronized.  Need
        // to fix Lance reader to use scheduler even when using custom (e.g. Java)
        // I/O sources.
        public synchronized byte[] readAt(long offset, long length) throws IOException {
            // As far as I can tell there is no way, with RandomAccessFile or
            // SeekableInputStream to perform a pread64.  This is unfortunate for
            // random access as it means we will need to perform two system calls for
            // every single read
            seekableInputStream.seek(offset);
            byte[] bytes = seekableInputStream.readNBytes((int)length);
            return bytes;
        }
    }

    private static class ArrowToSpark implements CloseableIterator<Object> {

        private final ArrowReader arrowReader;
        private final Function<VectorSchemaRoot, Object> arrowToSpark;
        private boolean initialized;
        private boolean exhausted;

        public ArrowToSpark(ArrowReader arrowReader, Function<VectorSchemaRoot, Object> arrowToSpark) {
            this.arrowReader = arrowReader;
            this.arrowToSpark = arrowToSpark;
            this.initialized = false;
            this.exhausted = false;
        }

        @Override
        public boolean hasNext() {
            if (!initialized) {
                try {
                    exhausted = !arrowReader.loadNextBatch();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                initialized = true;
            }
            return !exhausted;
        }

        @Override
        public Object next() {
            try {
                Object batch = arrowToSpark.apply(arrowReader.getVectorSchemaRoot());
                exhausted = !arrowReader.loadNextBatch();
                return batch;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void close() throws IOException {
            this.exhausted = true;
        }
    }


    public LanceReader(InputFile inputFile, BufferAllocator allocator, int recordsPerBatch, Function<VectorSchemaRoot, Object> arrowToSpark) {
        this.recordsPerBatch = recordsPerBatch;
        this.arrowToSpark = arrowToSpark;
        try {
            this.fileReader = LanceFileReader.openWithJavaIo(new LanceInputAdapter(inputFile), inputFile.location(), allocator);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public CloseableIterator<Object> iterator() {
        try {
            ArrowReader arrowReader = fileReader.readAll(recordsPerBatch);
            return new ArrowToSpark(arrowReader, arrowToSpark);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws IOException {

    }
}
