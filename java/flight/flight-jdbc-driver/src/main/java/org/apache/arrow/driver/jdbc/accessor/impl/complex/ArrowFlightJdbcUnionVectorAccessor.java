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

package org.apache.arrow.driver.jdbc.accessor.impl.complex;

import java.sql.SQLException;
import java.util.function.IntSupplier;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.complex.reader.FieldReader;

/**
 * Accessor for the Arrow type {@link UnionVector}.
 */
public class ArrowFlightJdbcUnionVectorAccessor extends AbstractArrowFlightJdbcUnionVectorAccessor {

  private final UnionVector vector;
  private final FieldReader reader;

  /**
   * Instantiate an accessor for a {@link UnionVector}.
   *
   * @param vector             an instance of a UnionVector.
   * @param currentRowSupplier the supplier to track the rows.
   * @param setCursorWasNull   the consumer to set if value was null.
   */
  public ArrowFlightJdbcUnionVectorAccessor(UnionVector vector, IntSupplier currentRowSupplier,
                                            ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(currentRowSupplier, setCursorWasNull);
    this.vector = vector;
    this.reader = vector.getReader();
  }

  @Override
  protected ArrowFlightJdbcAccessor createAccessorForVector(ValueVector vector) {
    return ArrowFlightJdbcAccessorFactory.createAccessor(vector, this::getCurrentRow, (boolean wasNull) -> {});
  }

  @Override
  protected byte getCurrentTypeId() {
    return (byte) vector.getTypeValue(getCurrentRow());
  }

  @Override
  public byte[] getBytes() throws SQLException {
    if (isNull()) {
      return null;
    }
    reader.setPosition(getCurrentRow());
    return reader.readByteArray();
  }

  private boolean isNull() {
    if (vector.isNull(getCurrentRow())) {
      wasNull = true;
      wasNullConsumer.setWasNull(true);
      return true;
    }
    return false;
  }

  @Override
  public Object getObject() throws SQLException {
    if (isNull()) return null;
    reader.setPosition(getCurrentRow());
    return reader.readObject();
  }

  @Override
  protected ValueVector getVectorByTypeId(byte typeId) {
    return vector.getVectorByType(typeId);
  }
}
