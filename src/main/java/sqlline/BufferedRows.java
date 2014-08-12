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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Rows implementation which buffers all rows in a linked list.
 */
class BufferedRows extends Rows {
  private final List<Row> list;

  private final Iterator<Row> iterator;

  int totalwidth = 80;


  BufferedRows(SqlLine sqlLine, ResultSet rs) throws SQLException {
    super(sqlLine, rs);

    list = new LinkedList<Row>();

    int count = rsMeta.getColumnCount();

    list.add(new Row(count));

    while (rs.next()) {
      list.add(new Row(count, rs));
    }

    iterator = list.iterator();
  }

  public boolean hasNext() {
    return iterator.hasNext();
  }

  public Row next() {
    return iterator.next();
  }

  private int[] computeWidths(double[] min, double[] max) {
    int n = min.length;
    int[] result = new int[n];

    // quadratic column sizing based on 0 < r < 1
    //    r * r + r * (span[i] - 1) = width[i]
    // Adding up all "n" columns, this comes to
    //    n * r * r + r * (totalspan - n) = totalwidth
    // Quadratic parameters
    //    a = n
    //    b = totalspan - n
    //    c = -screenwidth

    double a = n;
    double b = -n;
    double c = -totalwidth;
    for (int i = 0; i < n; i++) {
      b += max[i] - min[i];
    }

    double r = (Math.sqrt(b * b - 4 * a * c) - b) / (2 * a);
    for (int i = 0; i < n; i++) {
      result[i] = (int) (r * r + (max[i] - min[i] - 1) * r);
    }

    return result;
  }

  void normalizeWidths() {
    if (list.size() <= 0) {
      return;
    }

    int n = list.get(0).values.length;
    double[] min = new double[n];
    double[] max = new double[n];

    for (Row row : list) {
      if (row.isMeta) {
        for (int j = 0; j < n; j++) {
          min[j] = row.sizes[j];
        }
      } else {
        for (int j = 0; j < n; j++) {
          max[j] = Math.max(max[j], row.sizes[j]);
        }
      }
    }

    double minsum = 0.0;
    for (int i = 0; i < n; i++) {
      max[i] = Math.max(min[i], max[i]);
      minsum += min[i];
    }

    // scale down min widths if columns are impossible to fit
    if (minsum > totalwidth) {
      for (int i = 0; i < n; i++) {
        min[i] *= totalwidth / minsum;
      }
    }

    int[] rv = computeWidths(min, max);
    for (Row row : list) {
      row.sizes = rv;
    }
  }
}

// End BufferedRows.java
