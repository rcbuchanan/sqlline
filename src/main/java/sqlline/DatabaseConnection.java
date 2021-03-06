/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Modified BSD License
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at:
//
// http://opensource.org/licenses/BSD-3-Clause
*/
package sqlline;

import java.io.IOException;
import java.sql.*;
import java.util.*;

import jline.console.completer.ArgumentCompleter;
import jline.console.completer.Completer;

/**
 * Holds a database connection, credentials, and other associated state.
 */
class DatabaseConnection {
  private final SqlLine sqlLine;
  Connection connection;
  DatabaseMetaData meta;
  Quoting quoting;
  private final String driver;
  private final String url;
  private final String username;
  private final String password;
  private Schema schema = null;
  private Completer sqlCompleter = null;

  public DatabaseConnection(SqlLine sqlLine, String driver, String url,
      String username, String password) throws SQLException {
    this.sqlLine = sqlLine;
    this.driver = driver;
    this.url = url;
    this.username = username;
    this.password = password;
  }

  @Override
  public String toString() {
    return getUrl() + "";
  }

  void setCompletions(boolean skipmeta) throws SQLException, IOException {
    // Deduce the string used to quote identifiers. For example, Oracle
    // uses double-quotes:
    //   SELECT * FROM "My Schema"."My Table"
    String startQuote = meta.getIdentifierQuoteString();
    final boolean upper = meta.storesUpperCaseIdentifiers();
    if (startQuote == null
        || startQuote.equals("")
        || startQuote.equals(" ")) {
      if (meta.getDatabaseProductName().startsWith("MySQL")) {
        // Some version of the MySQL JDBC driver lie.
        quoting = new Quoting('`', '`', upper);
      } else {
        quoting = new Quoting((char) 0, (char) 0, false);
      }
    } else if (startQuote.equals("[")) {
      quoting = new Quoting('[', ']', upper);
    } else if (startQuote.length() > 1) {
      sqlLine.error(
          "Identifier quote string is '" + startQuote
              + "'; quote strings longer than 1 char are not supported");
      quoting = Quoting.DEFAULT;
    } else {
      quoting =
          new Quoting(startQuote.charAt(0), startQuote.charAt(0), upper);
    }

    final String extraNameCharacters =
        meta == null
            || meta.getExtraNameCharacters() == null
            ? ""
            : meta.getExtraNameCharacters();

    // setup the completer for the database
    sqlCompleter = new ArgumentCompleter(
        new ArgumentCompleter.WhitespaceArgumentDelimiter() {
          // delimiters for SQL statements are any
          // non-letter-or-number characters, except
          // underscore and characters that are specified
          // by the database to be valid name identifiers.
          @Override
          public boolean isDelimiterChar(
              final CharSequence buffer, int pos) {
            char c = buffer.charAt(pos);
            if (Character.isWhitespace(c)) {
              return true;
            }

            return !Character.isLetterOrDigit(c)
                && c != '_'
                && extraNameCharacters.indexOf(c) == -1;
          }
        },
        new SqlCompleter(sqlLine, skipmeta));

    // not all argument elements need to hold true
    ((ArgumentCompleter) sqlCompleter).setStrict(false);
  }

  /**
   * Connection to the specified data source.
   */
  boolean connect() throws SQLException {
    try {
      if (driver != null && driver.length() != 0) {
        Class.forName(driver);
      }
    } catch (ClassNotFoundException cnfe) {
      return sqlLine.error(cnfe);
    }

    boolean foundDriver = false;
    Driver theDriver = null;
    try {
      theDriver = DriverManager.getDriver(url);
      foundDriver = theDriver != null;
    } catch (Exception e) {
      // ignore
    }

    if (!foundDriver) {
      sqlLine.output(sqlLine.loc("autoloading-known-drivers", url));
      sqlLine.registerKnownDrivers();
    }

    try {
      close();
    } catch (Exception e) {
      return sqlLine.error(e);
    }

    // Avoid using DriverManager.getConnection(). It is a synchronized
    // method and thus holds the lock while making the connection.
    // Deadlock can occur if the driver's connection processing uses any
    // synchronized DriverManager methods.  One such example is the
    // RMI-JDBC driver, whose RJDriverServer.connect() method uses
    // DriverManager.getDriver(). Because RJDriverServer.connect runs in
    // a different thread (RMI) than the getConnection() caller (here),
    // this sequence will hang every time.
/*
          connection = DriverManager.getConnection (url, username, password);
*/
    // Instead, we use the driver instance to make the connection

    final Properties info = new Properties();
    info.put("user", username);
    info.put("password", password);
    connection = theDriver.connect(url, info);
    meta = connection.getMetaData();

    try {
      sqlLine.debug(sqlLine.loc("connected",
          meta.getDatabaseProductName(),
          meta.getDatabaseProductVersion()));
    } catch (Exception e) {
      sqlLine.handleException(e);
    }

    try {
      sqlLine.debug(sqlLine.loc("driver",
          meta.getDriverName(),
          meta.getDriverVersion()));
    } catch (Exception e) {
      sqlLine.handleException(e);
    }

    try {
      connection.setAutoCommit(sqlLine.getOpts().getAutoCommit());
      sqlLine.autocommitStatus(connection);
    } catch (Exception e) {
      sqlLine.handleException(e);
    }

    try {
      // nothing is done off of this command beyond the handle so no
      // need to use the callback.
      sqlLine.getCommands().isolation("isolation: " + sqlLine.getOpts()
          .getIsolation(),
          new DispatchCallback());
    } catch (Exception e) {
      sqlLine.handleException(e);
    }

    sqlLine.showWarnings();

    return true;
  }

  public Connection getConnection() throws SQLException {
    if (connection != null) {
      return connection;
    }

    connect();

    return connection;
  }

  public void reconnect() throws Exception {
    close();
    getConnection();
  }

  public void close() {
    try {
      try {
        if (connection != null && !connection.isClosed()) {
          sqlLine.output(sqlLine.loc("closing", connection));
          connection.close();
        }
      } catch (Exception e) {
        sqlLine.handleException(e);
      }
    } finally {
      connection = null;
      meta = null;
    }
  }

  public Collection<String> getTableNames(boolean force) {
    Set<String> names = new TreeSet<String>();
    for (Schema.Table table : getSchema().getTables()) {
      names.add(table.getName());
    }
    return names;
  }

  Schema getSchema() {
    if (schema == null) {
      schema = new Schema();
    }

    return schema;
  }

  DatabaseMetaData getDatabaseMetaData() {
    return meta;
  }

  String getUrl() {
    return url;
  }

  Completer getSqlCompleter() {
    return sqlCompleter;
  }

  /** Schema. */
  class Schema {
    private List<Table> tables;

    List<Table> getTables() {
      if (tables != null) {
        return tables;
      }

      tables = new LinkedList<Table>();

      try {
        ResultSet rs =
            getDatabaseMetaData().getTables(getConnection().getCatalog(),
                null, "%", new String[]{"TABLE"});
        try {
          while (rs.next()) {
            tables.add(new Table(rs.getString("TABLE_NAME")));
          }
        } finally {
          try {
            rs.close();
          } catch (Exception e) {
            // ignore
          }
        }
      } catch (Throwable t) {
        // ignore
      }

      return tables;
    }

    Table getTable(String name) {
      for (Table table : getTables()) {
        if (name.equalsIgnoreCase(table.getName())) {
          return table;
        }
      }

      return null;
    }

    /** Table. */
    class Table {
      final String name;
      Column[] columns;

      public Table(String name) {
        this.name = name;
      }

      public String getName() {
        return name;
      }

      /** Column. */
      class Column {
        final String name;
        boolean isPrimaryKey;

        public Column(String name) {
          this.name = name;
        }
      }
    }
  }
}

// End DatabaseConnection.java
