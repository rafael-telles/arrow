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

package org.apache.arrow.driver.jdbc.accessor.impl.calendar;

import static org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils.iterateOnAccessor;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

import java.time.Duration;

import org.apache.arrow.driver.jdbc.test.utils.AccessorTestUtils;
import org.apache.arrow.driver.jdbc.test.utils.RootAllocatorTestRule;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

public class ArrowFlightJdbcIntervalDayVectorAccessorTest {

  @ClassRule
  public static RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  private IntervalDayVector vector;

  private final AccessorTestUtils.AccessorSupplier<ArrowFlightJdbcIntervalDayVectorAccessor> accessorSupplier =
      (vector, getCurrentRow) -> new ArrowFlightJdbcIntervalDayVectorAccessor((IntervalDayVector) vector,
          getCurrentRow);

  @Before
  public void setup() {
    FieldType fieldType = new FieldType(true, new ArrowType.Duration(TimeUnit.MILLISECOND), null);
    this.vector = new IntervalDayVector("", fieldType, rootAllocatorTestRule.getRootAllocator());

    int valueCount = 10;
    this.vector.setValueCount(valueCount);
    for (int i = 0; i < valueCount; i++) {
      this.vector.set(i, i + 1, (i + 1) * 1000);
    }
  }

  @After
  public void tearDown() {
    this.vector.close();
  }

  @Test
  public void getObject() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          Duration result = (Duration) accessor.getObject();

          collector.checkThat(result, is(Duration.ofDays(currentRow + 1).plusMillis((currentRow + 1) * 1000L)));
          collector.checkThat(accessor.wasNull(), is(false));
        });
  }

  @Test
  public void getObjectPassingDurationAsParameter() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          Duration result = accessor.getObject(Duration.class);

          collector.checkThat(result, is(Duration.ofDays(currentRow + 1).plusMillis((currentRow + 1) * 1000L)));
          collector.checkThat(accessor.wasNull(), is(false));
        });
  }

  @Test
  public void getObjectForNull() throws Exception {
    int valueCount = vector.getValueCount();
    for (int i = 0; i < valueCount; i++) {
      vector.setNull(i);
    }

    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          collector.checkThat(accessor.getObject(), equalTo(null));
          collector.checkThat(accessor.wasNull(), is(true));
        });
  }

  @Test
  public void getString() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          String expectedString = vector.getAsStringBuilder(currentRow).toString();
          collector.checkThat(accessor.getString(), is(expectedString));
          collector.checkThat(accessor.wasNull(), is(false));
        });
  }

  @Test
  public void getStringForNull() throws Exception {
    int valueCount = vector.getValueCount();
    for (int i = 0; i < valueCount; i++) {
      vector.setNull(i);
    }

    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {
          String result = accessor.getString();

          collector.checkThat(result, equalTo(null));
          collector.checkThat(accessor.wasNull(), is(true));
        });
  }

  @Test
  public void testShouldGetObjectClass() throws Exception {
    iterateOnAccessor(vector, accessorSupplier,
        (accessor, currentRow) -> {

          collector.checkThat(accessor.getObjectClass(), equalTo(Duration.class));
        });
  }
}