package com.lancedb.iceberg;

import java.util.Map;
import java.util.function.BiFunction;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.NativeEncryptionOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.datafile.AppenderBuilder;
import org.apache.iceberg.io.datafile.ReadBuilder;
import org.apache.iceberg.mapping.NameMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.function.Function;

public class Lance {

    private static final Logger LOG = LoggerFactory.getLogger(Lance.class);

    public static class LanceWriterBuilder implements AppenderBuilder<LanceWriterBuilder, Object> {
        private final OutputFile outputFile;
        private boolean shouldOverwrite;
        private Object schema;
        private BufferAllocator allocator;
        private Function<Object, VectorSchemaRoot> schemaToArrow;
        private BiFunction<VectorSchemaRoot, Object, VectorSchemaRoot> valueToArrow;
        private BiFunction<VectorSchemaRoot, Iterator<Object>, VectorSchemaRoot> valuesToArrow;

        public LanceWriterBuilder(OutputFile outputFile) {
            this.outputFile = outputFile;
            this.shouldOverwrite = false;
        }

        @Override
        public LanceWriterBuilder forTable(Table table) {
            // Don't need anything here, Lance has no writer configuration
            return this;
        }

        @Override
        public LanceWriterBuilder schema(Schema schema) {
            // Don't need this, we just look at first batch
            //
            // (Actually, we may need to examine this to get field ids right)
            return this;
        }

        @Override
        public LanceWriterBuilder named(String name) {
            // Don't need, Lance doesn't need table name
            return this;
        }

        @Override
        public LanceWriterBuilder set(String property, String value) {
            // Don't need, Lance has no significant 
            // writer configuration at the moment
            return this;
        }

        @Override
        public LanceWriterBuilder meta(String property, String value) {
            // Don't need, Lance has no writer configuration
            return this;
        }

        @Override
        public LanceWriterBuilder metricsConfig(MetricsConfig newMetricsConfig) {
            // Don't need, Lance only has one metrics mode (truncate)
            //
            // (What happens if user requests full?)
            return this;
        }

        @Override
        public LanceWriterBuilder overwrite(boolean enabled) {
            this.shouldOverwrite = enabled;
            return this;
        }

        @Override
        public Object engineSchema(Object newEngineSchema) {
            this.schema = newEngineSchema;
            return this;
        }

        public LanceWriterBuilder schemaToArrow(Function<Object, VectorSchemaRoot> schemaToArrow) {
            this.schemaToArrow = schemaToArrow;
            return this;
        }

        public LanceWriterBuilder valueToArrow(BiFunction<VectorSchemaRoot, Object, VectorSchemaRoot> valueToArrow) {
            this.valueToArrow = valueToArrow;
            return this;
        }

        public LanceWriterBuilder valuesToArrow(BiFunction<VectorSchemaRoot, Iterator<Object>, VectorSchemaRoot> valuesToArrow) {
            this.valuesToArrow = valuesToArrow;
            return this;
        }

        public LanceWriterBuilder allocator(BufferAllocator allocator) {
            this.allocator = allocator;
            return this;
        }

        @Override
        public <D> FileAppender<D> build(WriteMode mode) throws IOException {
            LOG.info("Creating Lance writer for file {} (overwrite = {} mode={})", outputFile, shouldOverwrite, mode);
            Preconditions.checkNotNull(valueToArrow, "valueToArrow is required");
            Preconditions.checkNotNull(valuesToArrow, "valueToArrow is required");

            if (mode != WriteMode.APPENDER && mode != WriteMode.DATA_WRITER) {
                throw new UnsupportedOperationException("Only appender mode is supported");
            }

            PositionOutputStream outputStream = shouldOverwrite ? outputFile.createOrOverwrite() : outputFile.create();

            VectorSchemaRoot schema = this.schemaToArrow.apply(this.schema);
            return new LanceAppender<>(outputStream, schema, valueToArrow, valuesToArrow, allocator);
        }

    }

    public static LanceWriterBuilder write(EncryptedOutputFile file) {
        if (file instanceof NativeEncryptionOutputFile) {
            throw new RuntimeException("Native encryption is not supported yet");
        } else {
          return new LanceWriterBuilder(file.encryptingOutputFile());
        }
    }

    public static class LanceReaderBuilder implements ReadBuilder<LanceReaderBuilder, Object> {
        private final InputFile file;
        // Splits are not currently integrated but should be.
        // In the future.  To use splits in lance we can convert
        // byte split to row split...
        //
        // normalizedStart = (splitStart / fileSizeBytes)
        // normalizedLength = (splitLength / fileSizeBytes)
        // rowStart = round(normalizedStart * numRows)
        // rowLength = round(normalizedLength * numRows)
        public long splitStart;
        public long splitLength;
        // Projection isn't currently implemented but would be a matter
        // of mapping field ids in projection to field ids in file schema
        // to determine column offsets.
        public Schema projection;
        // Not currently implemented, unclear why this is a concern of the
        // file reader.
        public Map<Integer, ?> idToConstant;
        // Not currently implemented, unclear why this is a concern of the
        // file reader.
        public NameMapping nameMapping;
        // Function to convert from Arrow batches to Spark batches
        private Function<VectorSchemaRoot, Object> arrowToSpark;
        // Allocator for Arrow
        private BufferAllocator allocator;
        // Batch size
        private int recordsPerBatch = 1024 * 8;

        public LanceReaderBuilder(InputFile file) {
            this.file = file;
        }

        @Override
        public LanceReaderBuilder split(long newStart, long newLength) {
            this.splitStart = newStart;
            this.splitLength = newLength;
            return this;
        }

        @Override
        public LanceReaderBuilder project(Schema newSchema) {
            this.projection = newSchema;
            return this;
        }

        @Override
        public LanceReaderBuilder set(String key, String value) {
            return this;
        }

        @Override
        public LanceReaderBuilder reuseContainers(boolean newReuseContainers) {
            return this;
        }

        @Override
        public LanceReaderBuilder idToConstant(Map<Integer, ?> newIdConstant) {
            this.idToConstant = newIdConstant;
            return this;
        }

        @Override
        public LanceReaderBuilder withNameMapping(NameMapping newNameMapping) {
            this.nameMapping = newNameMapping;
            return this;
        }

        @Override
        public LanceReaderBuilder recordsPerBatch(int newRecordsPerBatch) {
            this.recordsPerBatch = newRecordsPerBatch;
            return this;
        }

        @Override
        public <D> CloseableIterable<D> build() {
            return (CloseableIterable<D>) new LanceReader(file, allocator, recordsPerBatch, arrowToSpark);
        }

        public LanceReaderBuilder arrowToSpark(Function<VectorSchemaRoot, Object> arrowToSpark) {
            this.arrowToSpark = arrowToSpark;
            return this;
        }

        public LanceReaderBuilder allocator(BufferAllocator allocator) {
            this.allocator = allocator;
            return this;
        }
    }

    public static LanceReaderBuilder read(InputFile inputFile) {
        return new LanceReaderBuilder(inputFile);
    }

}
