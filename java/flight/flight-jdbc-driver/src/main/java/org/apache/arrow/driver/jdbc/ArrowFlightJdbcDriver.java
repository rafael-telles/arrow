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

import static org.apache.arrow.driver.jdbc.utils.BaseProperty.HOST;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.PORT;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.RegEx;

import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;

import com.google.common.base.Strings;

/**
 * JDBC driver for querying data from an Apache Arrow Flight server.
 */
public class ArrowFlightJdbcDriver extends UnregisteredDriver {

  private static final String CONNECT_STRING_PREFIX = "jdbc:arrow-flight://";

  private static final Pattern urlRegExPattern;

  private static DriverVersion version;

  static {
    // jdbc:arrow-flight://<host>:<port>[/?k1=v1&k2=v2&(...)]
    @RegEx
    final String pattern =
        "^(?:" + CONNECT_STRING_PREFIX.replace("(\\/)", "\\1") + ")" + // Prefix
        "(\\w+):" + // Group 1 (host): match any word before colon
        "([\\d]+)\\/*" + // Group 2 (port): match number before optional slash
        "\\?*([[\\w]+=[\\w]+&?]*)?"; // Group 3 (params): match key-value pairs

    urlRegExPattern = Pattern.compile(pattern);
    new ArrowFlightJdbcDriver().register();
  }

  @Override
  public Connection connect(final String url, final Properties info)
      throws SQLException {

    // FIXME DO NOT tamper with the original Properties!
    final Properties clonedProperties = info;

    try {
      final Map<Object, Object> args = getUrlsArgs(
          Preconditions.checkNotNull(url));

      clonedProperties.putAll(args);

      return new ArrowFlightConnection(this, factory, url, clonedProperties);
    } catch (AssertionError | FlightRuntimeException e) {
      throw new SQLException("Failed to connect.", e);
    }
  }

  @Override
  protected String getFactoryClassName(final JdbcVersion jdbcVersion) {
    return ArrowFlightJdbcFactory.class.getName();
  }

  @Override
  protected DriverVersion createDriverVersion() {

    CreateVersionIfNull: {

      if (version != null) {
        break CreateVersionIfNull;
      }

      try (Reader reader = new BufferedReader(new InputStreamReader(
          this.getClass().getResourceAsStream("/properties/flight.properties"), StandardCharsets.UTF_8))) {
        final Properties properties = new Properties();
        properties.load(reader);

        final String parentName = properties
            .getProperty("org.apache.arrow.flight.name");
        final String parentVersion = properties
            .getProperty("org.apache.arrow.flight.version");
        final String[] pVersion = parentVersion.split("\\.");

        final int parentMajorVersion = Integer.parseInt(pVersion[0]);
        final int parentMinorVersion = Integer.parseInt(pVersion[1]);

        final String childName = properties
            .getProperty("org.apache.arrow.flight.jdbc-driver.name");
        final String childVersion = properties
            .getProperty("org.apache.arrow.flight.jdbc-driver.version");
        final String[] cVersion = childVersion.split("\\.");

        final int childMajorVersion = Integer.parseInt(cVersion[0]);
        final int childMinorVersion = Integer.parseInt(cVersion[1]);

        version = new DriverVersion(childName, childVersion, parentName,
            parentVersion, true, childMajorVersion, childMinorVersion,
            parentMajorVersion, parentMinorVersion);
      } catch (final IOException e) {
        throw new RuntimeException("Failed to load driver version.", e);
      }
    }

    return version;
  }

  @Override
  public Meta createMeta(final AvaticaConnection connection) {
    return new ArrowFlightMetaImpl(connection);
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  @Override
  public boolean acceptsURL(final String url) throws SQLException {
    return Preconditions.checkNotNull(url).startsWith(CONNECT_STRING_PREFIX);
  }

  /**
   * Parses the provided url based on the format this driver accepts, retrieving
   * arguments after the {@link #CONNECT_STRING_PREFIX}.
   * <p>
   * This regular expression checks if the provided URL follows this pattern:
   * {@code jdbc:arrow-flight://<host>:<port>[/?key1=val1&key2=val2&(...)]}
   *
   * <table border="1">
   *    <tr>
   *        <td>Group</td>
   *        <td>Definition</td>
   *        <td>Value</td>
   *    </tr>
   *    <tr>
   *        <td>? — inaccessible</td>
   *        <td>{@link #getConnectStringPrefix}</td>
   *        <td>
   *            the URL prefix accepted by this driver, i.e.,
   *            {@code "jdbc:arrow-flight://"}
   *        </td>
   *    </tr>
   *    <tr>
   *        <td>1</td>
   *        <td>IPv4 host name</td>
   *        <td>
   *            first word after previous group and before "{@code :}"
   *        </td>
   *    </tr>
   *    <tr>
   *        <td>2</td>
   *        <td>IPv4 port number</td>
   *        <td>
   *            first number after previous group and before "{@code /?}"
   *        </td>
   *    </tr>
   *    <tr>
   *        <td>3</td>
   *        <td>custom call parameters</td>
   *        <td>
   *            all parameters provided after "{@code /?}" — must follow the
   *            pattern: "{@code key=value}" with "{@code &}" separating a
   *            parameter from another
   *        </td>
   *    </tr>
   * </table>
   *
   * @param url
   *          The url to parse.
   * @return the parsed arguments.
   * @throws SQLException
   *           If an error occurs while trying to parse the URL.
   */
  private Map<Object, Object> getUrlsArgs(final String url)
      throws SQLException {

    /*
     * FIXME Refactor this sub-optimal approach to URL parsing later.
     *
     * Perhaps this logic should be inside a utility class, separated from this
     * one, so as to better delegate responsibilities and concerns throughout
     * the code and increase maintainability.
     *
     * =====
     *
     * Keep in mind that the URL must ALWAYS follow the pattern:
     * "jdbc:arrow-flight://<host>:<port>[/?param1=value1&param2=value2&(...)]."
     *
     * TODO Come up with a RegEx better than #urlRegExPattern.
     */
    final Matcher matcher = urlRegExPattern.matcher(url);

    if (!matcher.matches()) {
      throw new SQLException("Malformed/invalid URL!");
    }

    final Map<Object, Object> resultMap = new HashMap<>();

    resultMap.put(HOST.getEntry().getKey(), matcher.group(1)); // host
    resultMap.put(PORT.getEntry().getKey(), matcher.group(2)); // port

    final String extraParams = matcher.group(3); // optional params

    if (!Strings.isNullOrEmpty(extraParams)) {
      for (final String params : extraParams.split("&")) {
        final String[] keyValuePair = params.split("=");

        /*
         * FIXME Regex should do this automatically.
         *
         * The pattern should automatically filter URLs with invalid
         * parameters (e.g., "k1=v1&k2," or "k1=v1&." There shouldn't
         * be the need to check whether every parameters is provided as a
         * key-value pair.
         */
        if (keyValuePair.length != 2) {
          throw new SQLException(
              "URL parameters must be provided in key-value pairs!");
        }
        resultMap.put(keyValuePair[0], keyValuePair[1]);
      }
    }

    return resultMap;
  }
}
