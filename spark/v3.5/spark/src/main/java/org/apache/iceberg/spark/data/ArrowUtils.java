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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang.NotImplementedException;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.CalendarIntervalType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DayTimeIntervalType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.NullType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.UserDefinedType;
import org.apache.spark.sql.types.YearMonthIntervalType;

public class ArrowUtils {

    public static ArrowType toArrowType(DataType dt) {
        if (dt instanceof BooleanType) {
            return ArrowType.Bool.INSTANCE;
        } else if (dt instanceof ByteType) {
            return new ArrowType.Int(8, true);
        } else if (dt instanceof ShortType) {
            return new ArrowType.Int(8 * 2, true);
        } else if (dt instanceof IntegerType) {
            return new ArrowType.Int(8 * 4, true);
        } else if (dt instanceof LongType) {
            return new ArrowType.Int(8 * 8, true);
        } else if (dt instanceof FloatType) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        } else if (dt instanceof DoubleType) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        } else if (dt instanceof StringType) {
            return ArrowType.Utf8.INSTANCE;
        } else if (dt instanceof BinaryType) {
            return ArrowType.Binary.INSTANCE;
        } else if (dt instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) dt;
            return new ArrowType.Decimal(decimalType.precision(), decimalType.scale(), 128);
        } else if (dt instanceof TimestampType) {
            return new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
        } else if (dt instanceof TimestampNTZType) {
            return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
        } else if (dt instanceof NullType) {
            return ArrowType.Null.INSTANCE;
        } else if (dt instanceof YearMonthIntervalType) {
            return new ArrowType.Interval(IntervalUnit.YEAR_MONTH);
        } else if (dt instanceof DayTimeIntervalType) {
            return new ArrowType.Interval(IntervalUnit.DAY_TIME);
        } else if (dt instanceof CalendarIntervalType) {
            return new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO);
        } else {
            throw new RuntimeException("Unsupported data type: " + dt);
        }
    }

    public static Field toArrowField(
            String name,
            DataType dt,
            boolean nullable) {

        if (dt instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) dt;
            boolean listIsNullable = arrayType.containsNull();
            Field itemField = toArrowField("element", arrayType.elementType(), listIsNullable);
            FieldType listType = new FieldType(nullable, ArrowType.List.INSTANCE, null);
            return new Field(name, listType, List.of(itemField));
        } else if (dt instanceof StructType) {
            StructType structType = (StructType) dt;
            ArrayList<Field> children = new ArrayList<>();
            for (StructField child : structType.fields()) {
                Field childField = toArrowField(child.name(), child.dataType(), child.nullable());
                children.add(childField);
            }
            FieldType structTypeField = new FieldType(nullable, ArrowType.Struct.INSTANCE, null);
            return new Field(name, structTypeField, children);
        } else if (dt instanceof MapType) {
            MapType mapType = (MapType) dt;
            StructType structType = new StructType().add(MapVector.KEY_NAME, mapType.keyType(), false).add(MapVector.VALUE_NAME, mapType.valueType(), mapType.valueContainsNull());
            FieldType mapTypeField = new FieldType(nullable, new ArrowType.Map(false), null);
            return new Field(name, mapTypeField, List.of(toArrowField(MapVector.DATA_VECTOR_NAME, structType, false)));
        } else if (dt instanceof UserDefinedType) {
            throw new NotImplementedException("User defined types");
        } else {
            FieldType fieldType = new FieldType(nullable, toArrowType(dt), null);
            return new Field(name, fieldType, List.of());
        }
    }

    public static Schema toArrowSchema(
            StructType schema) {
        ArrayList<Field> fields = new ArrayList<>();
        for (StructField field : schema.fields()) {
            fields.add(toArrowField(field.name(), field.dataType(), field.nullable()));
        }
        return new Schema(fields);
    }

}
