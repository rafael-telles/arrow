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

package org.apache.arrow.driver.jdbc.accessor.impl.numeric;

import static org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils.iterateOnAccessor;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

import java.math.BigDecimal;
import java.util.function.Function;

import org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.test.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.BitVector;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

public class ArrowFlightJdbcBitVectorAccessorTest {
  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  private BitVector vector;
  private BitVector vectorWithNull;
  private boolean[] arrayToAssert;

  private AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcBitVectorAccessor> accessorSupplier =
      (vector, getCurrentRow) -> new ArrowFlightJdbcBitVectorAccessor((BitVector) vector, getCurrentRow);

  @Before
  public void setup() {

    this.arrayToAssert = new boolean[]{false, true};
    this.vector = rootAllocatorTestRule.createBitVector();
    this.vectorWithNull = rootAllocatorTestRule.createBitVectorForNullTests();
  }

  @After
  public void tearDown() {
    this.vector.close();
    this.vectorWithNull.close();
  }

  private <T> void iterate(Function<ArrowFlightJdbcBitVectorAccessor, T> function, T result, T resultIfFalse, BitVector vector) throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        ((accessor, currentRow) -> {
          final T value = function.apply(accessor);
          collector.checkThat(value, is(arrayToAssert[currentRow] ? result : resultIfFalse));
        })
    );
  }

  @Test
  public void testShouldGetBooleanMethodFromBitVector() throws Exception {
    iterate(ArrowFlightJdbcBitVectorAccessor::getBoolean, true, false, vector);
  }

  @Test
  public void testShouldGetByteMethodFromBitVector() throws Exception {
    iterate(ArrowFlightJdbcBitVectorAccessor::getByte, (byte) 1, (byte) 0, vector);
  }

  @Test
  public void testShouldGetShortMethodFromBitVector() throws Exception {
    iterate(ArrowFlightJdbcBitVectorAccessor::getShort, (short) 1, (short) 0, vector);
  }

  @Test
  public void testShouldGetIntMethodFromBitVector() throws Exception {
    iterate(ArrowFlightJdbcBitVectorAccessor::getInt, 1, 0, vector);

  }

  @Test
  public void testShouldGetLongMethodFromBitVector() throws Exception {
    iterate(ArrowFlightJdbcBitVectorAccessor::getLong, (long) 1, (long) 0, vector);

  }

  @Test
  public void testShouldGetFloatMethodFromBitVector() throws Exception {
    iterate(ArrowFlightJdbcBitVectorAccessor::getFloat, (float) 1, (float) 0, vector);

  }

  @Test
  public void testShouldGetDoubleMethodFromBitVector() throws Exception {
    iterate(ArrowFlightJdbcBitVectorAccessor::getDouble, (double) 1, (double) 0, vector);

  }

  @Test
  public void testShouldGetBytesMethodFromBitVector() throws Exception {
    byte[][] bytes = new byte[][] {{0x0}, {0x1}};

    iterate(ArrowFlightJdbcBitVectorAccessor::getBytes, bytes[1], bytes[0], vector);
  }

  @Test
  public void testShouldGetBytesMethodFromBitVectorFromNll() throws Exception {
    iterate(ArrowFlightJdbcBitVectorAccessor::getBytes, null, null, vectorWithNull);
  }

  @Test
  public void testShouldGetBigDecimalMethodFromBitVector() throws Exception {
    iterate(ArrowFlightJdbcBitVectorAccessor::getBigDecimal, BigDecimal.ONE, BigDecimal.ZERO, vector);
  }

  @Test
  public void testShouldGetBigDecimalMethodFromBitVectorFromNull() throws Exception {
    iterate(ArrowFlightJdbcBitVectorAccessor::getBigDecimal, null, null, vectorWithNull);

  }

  @Test
  public void testShouldGetObjectMethodFromBitVector() throws Exception {
    iterate(ArrowFlightJdbcBitVectorAccessor::getObject, 1L, 0L, vector);

  }

  @Test
  public void testShouldGetObjectMethodFromBitVectorFromNull() throws Exception {
    iterate(ArrowFlightJdbcBitVectorAccessor::getObject, null, null, vectorWithNull);

  }

  @Test
  public void testShouldGetStringMethodFromBitVector() throws Exception {
    iterate(ArrowFlightJdbcBitVectorAccessor::getString, "true", "false", vector);

  }

  @Test
  public void testShouldGetStringMethodFromBitVectorFromNull() throws Exception {
    iterate(ArrowFlightJdbcBitVectorAccessor::getString, null, null, vectorWithNull);

  }

  @Test
  public void testShouldGetObjectClass() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {

          collector.checkThat(accessor.getObjectClass(), equalTo(Long.class));
        });
  }
}