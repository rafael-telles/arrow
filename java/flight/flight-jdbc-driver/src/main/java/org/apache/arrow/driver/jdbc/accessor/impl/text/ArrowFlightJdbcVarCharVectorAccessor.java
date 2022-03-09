/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.driver.jdbc.accessor.impl.text;

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.function.IntSupplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.driver.jdbc.utils.DateTimeUtils;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;

/**
 * Accessor for the Arrow types: {@link VarCharVector} and {@link LargeVarCharVector}.
 */
public class ArrowFlightJdbcVarCharVectorAccessor extends ArrowFlightJdbcAccessor {

  private final Getter getter;

  public ArrowFlightJdbcVarCharVectorAccessor(VarCharVector vector,
                                              IntSupplier currentRowSupplier,
                                              ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    this(vector::getObject, currentRowSupplier, setCursorWasNull);
  }

  public ArrowFlightJdbcVarCharVectorAccessor(LargeVarCharVector vector,
                                              IntSupplier currentRowSupplier,
                                              ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    this(vector::getObject, currentRowSupplier, setCursorWasNull);
  }

  ArrowFlightJdbcVarCharVectorAccessor(Getter getter,
                                       IntSupplier currentRowSupplier,
                                       ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.getter = getter;
  }

  @Override
  public Class<?> getObjectClass() {
    return String.class;
  }

  private Text getText() {
    final Text text = this.getter.get(getCurrentRow());
    this.wasNull = text == null;
    this.wasNullConsumer.setWasNull(this.wasNull);
    return text;
  }

  @Override
  public String getObject() {
    final Text text = getText();
    return text == null ? null : text.toString();
  }

  @Override
  public String getString() {
    return getObject();
  }

  @Override
  public byte[] getBytes() {
    final Text value = this.getText();
    return value == null ? null : value.copyBytes();
  }

  @Override
  public boolean getBoolean() throws SQLException {
    String value = getString();
    if (value == null || value.equalsIgnoreCase("false") || value.equals("0")) {
      return false;
    } else if (value.equalsIgnoreCase("true") || value.equals("1")) {
      return true;
    } else {
      throw new SQLException("Is not possible to convert this value for boolean: " + value);
    }
  }

  @Override
  public byte getByte() throws SQLException {
    try {
      return Byte.parseByte(this.getString());
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public short getShort() throws SQLException {
    try {
      return Short.parseShort(this.getString());
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public int getInt() throws SQLException {
    try {
      return Integer.parseInt(this.getString());
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public long getLong() throws SQLException {
    try {
      return Long.parseLong(this.getString());
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public float getFloat() throws SQLException {
    try {
      return Float.parseFloat(this.getString());
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public double getDouble() throws SQLException {
    try {
      return Double.parseDouble(this.getString());
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public BigDecimal getBigDecimal() throws SQLException {
    try {
      return new BigDecimal(this.getString());
    } catch (NumberFormatException exception) {
      throw new SQLException(exception);
    }
  }

  @Override
  public BigDecimal getBigDecimal(int i) throws SQLException {
    try {
      return BigDecimal.valueOf(this.getLong(), i);
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public InputStream getAsciiStream() {
    Text value = this.getText();
    return value == null ? null : new ByteArrayInputStream(value.getBytes(), 0, value.getLength());
  }

  @Override
  public Reader getCharacterStream() {
    return new CharArrayReader(getString().toCharArray());
  }

  @Override
  public Date getDate(Calendar calendar) throws SQLException {
    try {
      Date date = Date.valueOf(getString());
      if (calendar == null) {
        return date;
      }

      // Use Calendar to apply time zone's offset
      long milliseconds = date.getTime();
      return new Date(DateTimeUtils.applyCalendarOffset(milliseconds, calendar));
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public Time getTime(Calendar calendar) throws SQLException {
    try {
      Time time = Time.valueOf(getString());
      if (calendar == null) {
        return time;
      }

      // Use Calendar to apply time zone's offset
      long milliseconds = time.getTime();
      return new Time(DateTimeUtils.applyCalendarOffset(milliseconds, calendar));
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public Timestamp getTimestamp(Calendar calendar) throws SQLException {
    try {
      Timestamp timestamp = Timestamp.valueOf(getString());
      if (calendar == null) {
        return timestamp;
      }

      // Use Calendar to apply time zone's offset
      long milliseconds = timestamp.getTime();
      return new Timestamp(DateTimeUtils.applyCalendarOffset(milliseconds, calendar));
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  /**
   * Functional interface to help integrating VarCharVector and LargeVarCharVector.
   */
  @FunctionalInterface
  interface Getter {
    Text get(int index);
  }
}
