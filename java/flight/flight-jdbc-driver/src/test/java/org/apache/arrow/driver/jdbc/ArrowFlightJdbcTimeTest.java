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

package org.apache.arrow.driver.jdbc;

import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class ArrowFlightJdbcTimeTest {
  final int hour = 5;
  final int minute = 6;
  final int second = 7;

  @Test
  public void testPrintingMillisNoLeadingZeroes() {
    // testing the regular case where the precision of the millisecond is 3
    LocalTime dateTime = LocalTime.of(hour, minute, second, (int) TimeUnit.MILLISECONDS.toNanos(999));
    ArrowFlightJdbcTime time = new ArrowFlightJdbcTime(dateTime);
    Assert.assertTrue(time.toString().endsWith(".999"));
    Assert.assertEquals(time.getHours(), hour);
    Assert.assertEquals(time.getMinutes(), minute);
    Assert.assertEquals(time.getSeconds(), second);
  }

  @Test
  public void testPrintingMillisOneLeadingZeroes() {
    // test case where one leading zero needs to be added
    LocalTime dateTime = LocalTime.of(hour, minute, second, (int) TimeUnit.MILLISECONDS.toNanos(99));
    ArrowFlightJdbcTime time = new ArrowFlightJdbcTime(dateTime);
    Assert.assertTrue(time.toString().endsWith(".099"));
    Assert.assertEquals(time.getHours(), hour);
    Assert.assertEquals(time.getMinutes(), minute);
    Assert.assertEquals(time.getSeconds(), second);
  }

  @Test
  public void testPrintingMillisTwoLeadingZeroes() {
    // test case where two leading zeroes needs to be added
    LocalTime dateTime = LocalTime.of(hour, minute, second, (int) TimeUnit.MILLISECONDS.toNanos(1));
    ArrowFlightJdbcTime time = new ArrowFlightJdbcTime(dateTime);
    Assert.assertTrue(time.toString().endsWith(".001"));
    Assert.assertEquals(time.getHours(), hour);
    Assert.assertEquals(time.getMinutes(), minute);
    Assert.assertEquals(time.getSeconds(), second);
  }

  @Test
  public void testEquality() {
    // tests #equals and #hashCode for coverage checks
    LocalTime dateTime = LocalTime.of(hour, minute, second, (int) TimeUnit.MILLISECONDS.toNanos(1));
    ArrowFlightJdbcTime time1 = new ArrowFlightJdbcTime(dateTime);
    ArrowFlightJdbcTime time2 = new ArrowFlightJdbcTime(dateTime);
    Assert.assertEquals(time1, time2);
    Assert.assertEquals(time1.hashCode(), time2.hashCode());
  }
}
