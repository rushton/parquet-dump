// TODO: THESE WERE COPIED FROM PARQUET-MR, ONCE PARQUET-1408 IS RELEASED REMOVE THESE.
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package parquetdump;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;


public class SimpleRecordConverter extends GroupConverter {
  private static final int JULIAN_EPOCH_OFFSET_DAYS = 2_440_588;
  private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
  private static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);

  private final Converter converters[];
  private final String name;
  private final SimpleRecordConverter parent;
  protected SimpleRecord record;
  private GroupType schema;

  public SimpleRecordConverter(GroupType schema) {
    this(schema, null, null);
  }

  public SimpleRecordConverter(GroupType schema, String name, SimpleRecordConverter parent) {
    this.converters = new Converter[schema.getFieldCount()];
    this.parent = parent;
    this.name = name;
    this.schema = schema;

    int i = 0;
    for (Type field: schema.getFields()) {
      converters[i++] = createConverter(field);
    }
  }

  private Converter createConverter(Type field) {
    OriginalType otype = field.getOriginalType();

    
    if (field.isPrimitive() && field.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT96) {
        return new Int96Converter(field.getName());
    }
    
    if (field.isPrimitive()) {
      if (otype != null) {
        switch (otype) {
          case MAP: break;
          case LIST: break;
          case UTF8: return new StringConverter(field.getName());
          case MAP_KEY_VALUE: break;
          case ENUM: break;
          case DECIMAL:
            int scale = field.asPrimitiveType().getDecimalMetadata().getScale();
            return new DecimalConverter(field.getName(), scale);
        }
      }

      return new SimplePrimitiveConverter(field.getName());
    }

    GroupType groupType = field.asGroupType();
    if (otype != null) {
      switch (otype) {
        case MAP: return new SimpleMapRecordConverter(groupType, field.getName(), this);
        case LIST: return new SimpleListRecordConverter(groupType, field.getName(), this);
      }
    }
    return new SimpleRecordConverter(groupType, field.getName(), this);
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  SimpleRecord getCurrentRecord() {
    return record;
  }

  @Override
  public void start() {
    record = new SimpleRecord();
  }

  @Override
  public void end() {
    if (parent != null) {
      parent.getCurrentRecord().add(name, record);
    }

    // find any fields missing on the record
    // and fill them with null
    Set<String> recordKeys = record
        .getValues().stream()
        .map(nv -> nv.getName())
        .collect(Collectors.toSet());

    schema.getFields().stream()
        .map(field -> field.getName())
        .filter(name -> !recordKeys.contains(name))
        .forEach(name -> record.add(name, null));
  }

  private class SimplePrimitiveConverter extends PrimitiveConverter {
    protected final String name;

    public SimplePrimitiveConverter(String name) {
      this.name = name;
    }

    @Override
    public void addBinary(Binary value) {
      record.add(name, value.getBytes());
    }

    @Override
    public void addBoolean(boolean value) {
      record.add(name, value);
    }

    @Override
    public void addDouble(double value) {
      record.add(name, value);
    }

    @Override
    public void addFloat(float value) {
      record.add(name, value);
    }

    @Override
    public void addInt(int value) {
      record.add(name, value);
    }

    @Override
    public void addLong(long value) {
      record.add(name, value);
    }
  }

  private class StringConverter extends SimplePrimitiveConverter {
    public StringConverter(String name) {
      super(name);
    }

    @Override
    public void addBinary(Binary value) {
      record.add(name, value.toStringUsingUTF8());
    }
  }

  private class Int96Converter extends SimplePrimitiveConverter {

    public Int96Converter(String name) {
      super(name);
    }

    @Override
    public void addBinary(Binary value) {
      byte[] bytes = value.getBytes();
      long timeOfDayNanos = ByteBuffer.wrap(Arrays.copyOfRange(bytes, 0, 8)).order(ByteOrder.LITTLE_ENDIAN).getLong();
      int julianDay = ByteBuffer.wrap(Arrays.copyOfRange(bytes, 8, 12)).order(ByteOrder.LITTLE_ENDIAN).getInt();

      long epochMillis = ((julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY ) + (timeOfDayNanos / NANOS_PER_MILLISECOND);
      record.add(name, epochMillis);
    }
  }

  private class DecimalConverter extends SimplePrimitiveConverter {
    private final int scale;

    public DecimalConverter(String name, int scale) {
      super(name);
      this.scale = scale;
    }

    @Override
    public void addBinary(Binary value) {
      record.add(name, new BigDecimal(new BigInteger(value.getBytes()), scale));
    }
  }
}

