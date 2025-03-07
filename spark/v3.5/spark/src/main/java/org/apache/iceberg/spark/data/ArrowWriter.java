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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

public class ArrowWriter {

    private final VectorSchemaRoot root;
    private final ArrowFieldWriter[] fieldWriters;
    private int count;

    public ArrowWriter(VectorSchemaRoot root) {
        this.root = root;
        fieldWriters = new ArrowFieldWriter[root.getFieldVectors().size()];
        List<FieldVector> fieldVectors = root.getFieldVectors();
        for (int i = 0; i < fieldVectors.size(); i += 1) {
            fieldWriters[i] = createFieldWriter(fieldVectors.get(i));
        }
        count = 0;
    }

    public void write(InternalRow row) {
        for (int i = 0; i < row.numFields(); i += 1) {
            fieldWriters[i].write(row, i);
        }
        count += 1;
    }

    public int sizeInBytes() {
        int bytesSum = 0;
        for (ArrowFieldWriter fieldWriter : fieldWriters) {
            bytesSum += fieldWriter.getSizeInBytes();
        }
        return bytesSum;
    }

    public void finish() {
        root.setRowCount(count);
        for (ArrowFieldWriter fieldWriter : fieldWriters) {
            fieldWriter.finish();
        }
    }

    public void reset() {
        root.setRowCount(0);
        count = 0;
        for (ArrowFieldWriter fieldWriter : fieldWriters) {
            fieldWriter.reset();
        }
    }

    public VectorSchemaRoot getRoot() {
        return root;
    }

    private static ArrowFieldWriter createFieldWriter(ValueVector vector) {
        if (vector instanceof BitVector) {
            return new ArrowWriter.BooleanWriter(vector);
        } else if (vector instanceof TinyIntVector) {
            return new ArrowWriter.ByteWriter(vector);
        } else if (vector instanceof SmallIntVector) {
            return new ArrowWriter.ShortWriter(vector);
        } else if (vector instanceof IntVector) {
            return new ArrowWriter.IntegerWriter(vector);
        } else if (vector instanceof BigIntVector) {
            return new ArrowWriter.LongWriter(vector);
        } else if (vector instanceof Float4Vector) {
            return new ArrowWriter.FloatWriter(vector);
        } else if (vector instanceof Float8Vector) {
            return new ArrowWriter.DoubleWriter(vector);
        } else if (vector instanceof DecimalVector) {
            return new ArrowWriter.DecimalWriter((DecimalVector) vector);
        } else if (vector instanceof VarCharVector) {
            return new ArrowWriter.StringWriter(vector);
        } else if (vector instanceof VarBinaryVector) {
            return new ArrowWriter.BinaryWriter(vector);
        } else if (vector instanceof DateDayVector) {
            return new ArrowWriter.DateWriter(vector);
        } else if (vector instanceof TimeStampMicroTZVector) {
            return new ArrowWriter.TimestampWriter(vector);
        } else if (vector instanceof TimeStampMicroVector) {
            return new ArrowWriter.TimestampNTZWriter(vector);
        } else if (vector instanceof ListVector) {
            ListVector listVector = (ListVector) vector;
            return new ArrowWriter.ListWriter(vector, createFieldWriter(listVector.getDataVector()));
        } else if (vector instanceof StructVector) {
            ArrayList<ArrowWriter.ArrowFieldWriter> childWriters = new ArrayList<>();
            for (ValueVector childVector : ((StructVector) vector).getChildrenFromFields()) {
                childWriters.add(createFieldWriter(childVector));
            }
            return new ArrowWriter.StructWriter(vector, childWriters.toArray(new ArrowWriter.ArrowFieldWriter[0]));
        } else if (vector instanceof NullVector) {
            return new ArrowWriter.NullWriter(vector);
        } else if (vector instanceof IntervalYearVector) {
            return new ArrowWriter.IntervalYearWriter(vector);
        } else if (vector instanceof DurationVector) {
            return new ArrowWriter.DurationWriter(vector);
        } else if (vector instanceof IntervalMonthDayNanoVector) {
            return new ArrowWriter.IntervalMonthDayNanoWriter(vector);
        } else {
            throw new UnsupportedOperationException("Unsupported data type: " + vector.getField().getType());
        }
    }

    protected static abstract class ArrowFieldWriter {
        private final ValueVector valueVector;
        protected int count;

        protected abstract void setNull();
        protected abstract void setValue(SpecializedGetters input, int ordinal);

        protected ArrowFieldWriter(ValueVector valueVector) {
            this.valueVector = valueVector;
            this.count = 0;
        }

        public void write(SpecializedGetters input, int ordinal) {
            if (input.isNullAt(ordinal)) {
                setNull();
            } else {
                setValue(input, ordinal);
            }
            count += 1;
        }

        public void finish() {
            valueVector.setValueCount(count);
        }

        public int getSizeInBytes() {
            valueVector.setValueCount(count);
            return valueVector.getBufferSizeFor(count);
        }

        public void reset() {
            valueVector.reset();
            count = 0;
        }
    }


    private static class BooleanWriter extends ArrowWriter.ArrowFieldWriter {
        private final BitVector bitVector;

        public BooleanWriter(ValueVector valueVector) {
            super(valueVector);
            this.bitVector = (BitVector) valueVector;
        }

        @Override
        protected void setNull() {
            bitVector.setNull(count);
        }

        @Override
        protected void setValue(SpecializedGetters input, int ordinal) {
            bitVector.setSafe(count, input.getBoolean(ordinal) ? 1 : 0);
        }
    }

    private static class ByteWriter extends ArrowWriter.ArrowFieldWriter {

        private final TinyIntVector tinyIntVector;

        public ByteWriter(ValueVector valueVector) {
            super(valueVector);
            this.tinyIntVector = (TinyIntVector) valueVector;
        }

        @Override
        protected void setNull() {
            tinyIntVector.setNull(count);
        }

        @Override
        protected void setValue(SpecializedGetters input, int ordinal) {
            tinyIntVector.setSafe(count, input.getByte(ordinal));
        }
    }

    private static class ShortWriter extends ArrowFieldWriter {
        private final SmallIntVector smallIntVector;

        public ShortWriter(ValueVector valueVector) {
            super(valueVector);
            this.smallIntVector = (SmallIntVector) valueVector;
        }

        @Override
        protected void setNull() {
            smallIntVector.setNull(count);
        }

        @Override
        protected void setValue(SpecializedGetters input, int ordinal) {
            smallIntVector.setSafe(count, input.getShort(ordinal));
        }
    }

    private static class IntegerWriter extends ArrowWriter.ArrowFieldWriter {

        private final IntVector intVector;

        public IntegerWriter(ValueVector valueVector) {
            super(valueVector);
            this.intVector = (IntVector) valueVector;
        }

        @Override
        protected void setNull() {
            intVector.setNull(count);
        }

        @Override
        protected void setValue(SpecializedGetters input, int ordinal) {
            intVector.setSafe(count, input.getInt(ordinal));
        }
    }

    private static class LongWriter extends ArrowWriter.ArrowFieldWriter {
        private final BigIntVector bigIntVector;

        public LongWriter(ValueVector valueVector) {
            super(valueVector);
            this.bigIntVector = (BigIntVector) valueVector;
        }

        @Override
        protected void setNull() {
            bigIntVector.setNull(count);
        }

        @Override
        protected void setValue(SpecializedGetters input, int ordinal) {
            bigIntVector.setSafe(count, input.getLong(ordinal));
        }
    }

    private static class FloatWriter extends ArrowWriter.ArrowFieldWriter {

        private final Float4Vector float4Vector;

        public FloatWriter(ValueVector valueVector) {
            super(valueVector);
            this.float4Vector = (Float4Vector) valueVector;
        }

        @Override
        protected void setNull() {
            float4Vector.setNull(count);
        }

        @Override
        protected void setValue(SpecializedGetters input, int ordinal) {
            float4Vector.setSafe(count, input.getFloat(ordinal));
        }

    }

    private static class DoubleWriter extends ArrowWriter.ArrowFieldWriter {
        private final Float8Vector float8Vector;

        public DoubleWriter(ValueVector valueVector) {
            super(valueVector);
            this.float8Vector = (Float8Vector) valueVector;
        }

        @Override
        protected void setNull() {
            float8Vector.setNull(count);
        }

        @Override
        protected void setValue(SpecializedGetters input, int ordinal) {
            float8Vector.setSafe(count, input.getDouble(ordinal));
        }
    }

    private static class DecimalWriter extends ArrowWriter.ArrowFieldWriter {
        private final DecimalVector decimalVector;
        private final int precision;
        private final int scale;

        public DecimalWriter(ValueVector valueVector) {
            super(valueVector);
            this.decimalVector = (DecimalVector) valueVector;
            this.precision = decimalVector.getPrecision();
            this.scale = decimalVector.getScale();
        }

        @Override
        protected void setNull() {
            decimalVector.setNull(count);
        }

        @Override
        protected void setValue(SpecializedGetters input, int ordinal) {
            Decimal decimal = input.getDecimal(ordinal, precision, scale);
            if (decimal.changePrecision(precision, scale)) {
                decimalVector.setSafe(count, decimal.toJavaBigDecimal());
            } else {
                setNull();
            }
        }
    }

    private static class StringWriter extends ArrowWriter.ArrowFieldWriter {
        private final VarCharVector varCharVector;

        public StringWriter(ValueVector valueVector) {
            super(valueVector);
            this.varCharVector = (VarCharVector) valueVector;
        }

        @Override
        protected void setNull() {
            varCharVector.setNull(count);
        }

        @Override
        protected void setValue(SpecializedGetters input, int ordinal) {
            UTF8String utf8 = input.getUTF8String(ordinal);
            ByteBuffer byteBuffer = utf8.getByteBuffer();
            varCharVector.setSafe(count, byteBuffer, byteBuffer.position(), utf8.numBytes());
        }
    }

    private static class BinaryWriter extends ArrowWriter.ArrowFieldWriter {
        private final VarBinaryVector varBinaryVector;

        public BinaryWriter(ValueVector valueVector) {
            super(valueVector);
            this.varBinaryVector = (VarBinaryVector) valueVector;
        }

        @Override
        protected void setNull() {
            varBinaryVector.setNull(count);
        }

        @Override
        protected void setValue(SpecializedGetters input, int ordinal) {
            byte[] bytes = input.getBinary(ordinal);
            varBinaryVector.setSafe(count, bytes, 0, bytes.length);
        }
    }

    private static class DateWriter extends ArrowWriter.ArrowFieldWriter {
        private final DateDayVector dateDayVector;

        public DateWriter(ValueVector valueVector) {
            super(valueVector);
            this.dateDayVector = (DateDayVector) valueVector;
        }

        @Override
        protected void setNull() {
            dateDayVector.setNull(count);
        }

        @Override
        protected void setValue(SpecializedGetters input, int ordinal) {
            dateDayVector.setSafe(count, input.getInt(ordinal));
        }
    }

    private static class TimestampWriter extends ArrowWriter.ArrowFieldWriter {
        private final TimeStampMicroTZVector timeStampMicroTZVector;

        public TimestampWriter(ValueVector valueVector) {
            super(valueVector);
            this.timeStampMicroTZVector = (TimeStampMicroTZVector) valueVector;
        }

        @Override
        protected void setNull() {
            timeStampMicroTZVector.setNull(count);
        }

        @Override
        protected void setValue(SpecializedGetters input, int ordinal) {
            timeStampMicroTZVector.setSafe(count, input.getLong(ordinal));
        }
    }

    private static class TimestampNTZWriter extends ArrowWriter.ArrowFieldWriter {
        private final TimeStampMicroVector timeStampMicroVector;

        public TimestampNTZWriter(ValueVector valueVector) {
            super(valueVector);
            this.timeStampMicroVector = (TimeStampMicroVector) valueVector;
        }

        @Override
        protected void setNull() {
            timeStampMicroVector.setNull(count);
        }

        @Override
        protected void setValue(SpecializedGetters input, int ordinal) {
            timeStampMicroVector.setSafe(count, input.getLong(ordinal));
        }
    }

    private static class ListWriter extends ArrowWriter.ArrowFieldWriter {
        private final ListVector listVector;
        private final ArrowWriter.ArrowFieldWriter elementWriter;

        public ListWriter(ValueVector valueVector, ArrowWriter.ArrowFieldWriter elementWriter) {
            super(valueVector);
            this.listVector = (ListVector) valueVector;
            this.elementWriter = elementWriter;
        }

        @Override
        protected void setNull() {
            listVector.setNull(count);
        }

        @Override
        protected void setValue(SpecializedGetters input, int ordinal) {
            ArrayData array = input.getArray(ordinal);
            listVector.startNewValue(count);
            for (int i = 0; i < array.numElements(); i++) {
                elementWriter.write(array, i);
            }
            listVector.endValue(count, array.numElements());
        }

        @Override
        public void finish() {
            super.finish();
            elementWriter.finish();
        }

        @Override
        public void reset() {
            super.reset();
            elementWriter.reset();
        }
    }

    private static class StructWriter extends ArrowWriter.ArrowFieldWriter {
        private final StructVector structVector;
        private final ArrowWriter.ArrowFieldWriter[] children;

        public StructWriter(ValueVector valueVector, ArrowWriter.ArrowFieldWriter[] children) {
            super(valueVector);
            this.structVector = (StructVector) valueVector;
            this.children = children;
        }

        @Override
        protected void setNull() {
            for(ArrowWriter.ArrowFieldWriter child : children) {
                child.setNull();
            }
            structVector.setNull(count);
        }

        @Override
        protected void setValue(SpecializedGetters input, int ordinal) {
            InternalRow row = input.getStruct(ordinal, children.length);
            structVector.setIndexDefined(count);
            for(int i = 0; i < children.length; i++) {
                children[i].write(row, i);
            }
        }

        @Override
        public void finish() {
            super.finish();
            for (ArrowWriter.ArrowFieldWriter child : children) {
                child.finish();
            }
        }

        @Override
        public void reset() {
            super.reset();
            for (ArrowWriter.ArrowFieldWriter child : children) {
                child.reset();
            }
        }

    }

    private static class NullWriter extends ArrowWriter.ArrowFieldWriter {
        private final NullVector nullVector;

        public NullWriter(ValueVector valueVector) {
            super(valueVector);
            this.nullVector = (NullVector) valueVector;
        }

        @Override
        protected void setNull() {
            nullVector.setNull(count);
        }

        @Override
        protected void setValue(SpecializedGetters input, int ordinal) {
            throw new UnsupportedOperationException("NullWriter does not support setValue");
        }
    }

    private static class IntervalYearWriter extends ArrowWriter.ArrowFieldWriter {
        private final IntervalYearVector intervalYearVector;

        public IntervalYearWriter(ValueVector valueVector) {
            super(valueVector);
            this.intervalYearVector = (IntervalYearVector) valueVector;
        }

        @Override
        protected void setNull() {
            intervalYearVector.setNull(count);
        }

        @Override
        protected void setValue(SpecializedGetters input, int ordinal) {
            intervalYearVector.setSafe(count, input.getInt(ordinal));
        }
    }

    private static class DurationWriter extends ArrowWriter.ArrowFieldWriter {
        private final DurationVector durationVector;

        public DurationWriter(ValueVector valueVector) {
            super(valueVector);
            this.durationVector = (DurationVector) valueVector;
        }

        @Override
        protected void setNull() {
            durationVector.setNull(count);
        }

        @Override
        protected void setValue(SpecializedGetters input, int ordinal) {
            durationVector.setSafe(count, input.getLong(ordinal));
        }
    }

    private static class IntervalMonthDayNanoWriter extends ArrowWriter.ArrowFieldWriter {
        private final IntervalMonthDayNanoVector intervalMonthDayNanoVector;

        public IntervalMonthDayNanoWriter(ValueVector valueVector) {
            super(valueVector);
            this.intervalMonthDayNanoVector = (IntervalMonthDayNanoVector) valueVector;
        }

        @Override
        protected void setNull() {
            intervalMonthDayNanoVector.setNull(count);
        }

        @Override
        protected void setValue(SpecializedGetters input, int ordinal) {
            CalendarInterval ci = input.getInterval(ordinal);
            intervalMonthDayNanoVector.setSafe(count, ci.months, ci.days, Math.multiplyExact(ci.microseconds, 1000L));
        }
    }


}
