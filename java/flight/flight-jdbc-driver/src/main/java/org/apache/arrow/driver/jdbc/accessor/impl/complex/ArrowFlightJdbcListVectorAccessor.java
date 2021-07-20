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

import java.util.List;
import java.util.function.IntSupplier;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;

/**
 * Accessor for the Arrow type {@link ListVector}.
 */
public class ArrowFlightJdbcListVectorAccessor extends AbstractArrowFlightJdbcListVectorAccessor {

  private final ListVector vector;

  public ArrowFlightJdbcListVectorAccessor(ListVector vector, IntSupplier currentRowSupplier) {
    super(currentRowSupplier);
    this.vector = vector;
  }

  @Override
  protected long getStartOffset(int index) {
    return vector.getOffsetBuffer().getInt(index * 4L);
  }

  @Override
  protected long getEndOffset(int index) {
    return vector.getOffsetBuffer().getInt((index + 1) * 4L);
  }

  @Override
  protected FieldVector getDataVector() {
    return vector.getDataVector();
  }

  @Override
  public Object getObject() {
    List<?> object = vector.getObject(getCurrentRow());
    this.wasNull = object == null;

    return object;
  }
}