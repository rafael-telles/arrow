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

package org.apache.arrow.driver.jdbc.utils;

import static org.hamcrest.CoreMatchers.is;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Calendar;
import java.util.TimeZone;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

public class DateTimeUtilsTest {

  @ClassRule
  public static final ErrorCollector collector = new ErrorCollector();
  private static final TimeZone defaultTimezone = TimeZone.getTimeZone("UTC");
  private static final TimeZone alternateTimezone = TimeZone.getTimeZone("America/Vancouver");

  @Test
  public void testShouldGetOffsetWithSameTimeZone() {
    final TimeZone currentTimezone = TimeZone.getDefault();

    final long epochMillis = Instant.now().toEpochMilli();
    final long offset = defaultTimezone.getOffset(epochMillis);

    TimeZone.setDefault(defaultTimezone);

    try { // Trying to guarantee timezone returns to its original value
      final long expected = epochMillis + offset;
      final long actual = DateTimeUtils.applyCalendarOffset(epochMillis, Calendar.getInstance(defaultTimezone));

      collector.checkThat(actual, is(expected));
    } finally {
      // Reset Timezone
      TimeZone.setDefault(currentTimezone);
    }
  }

  @Test
  public void testShouldGetOffsetWithDifferentTimeZone() {
    final TimeZone currentTimezone = TimeZone.getDefault();

    final long epochMillis = Instant.now().toEpochMilli();
    final long offset = alternateTimezone.getOffset(epochMillis);

    TimeZone.setDefault(alternateTimezone);

    try { // Trying to guarantee timezone returns to its original value
      final long expectedEpochMillis = epochMillis + offset;
      final long actualEpochMillis = DateTimeUtils.applyCalendarOffset(epochMillis, Calendar.getInstance(
          defaultTimezone));

      collector.checkThat(actualEpochMillis, is(expectedEpochMillis));
    } finally {
      // Reset Timezone
      TimeZone.setDefault(currentTimezone);
    }
  }

  @Test
  public void testShouldGetTimestampPositive() {
    final Instant instant = Instant.now();
    long epochMilli = instant.toEpochMilli();

    final Timestamp expected = Timestamp.from(instant);
    final Timestamp actual = DateTimeUtils.getTimestampValue(epochMilli);

    collector.checkThat(expected, is(actual));
  }

  @Test
  public void testShouldGetTimestampNegative() {
    final long epochMilli = -618105600000L; // 1950-06-01 00:00:00 UTC
    final Instant instant = Instant.ofEpochMilli(epochMilli);

    final Timestamp expected = Timestamp.from(instant);
    final Timestamp actual = DateTimeUtils.getTimestampValue(epochMilli);

    collector.checkThat(expected, is(actual));
  }
}
