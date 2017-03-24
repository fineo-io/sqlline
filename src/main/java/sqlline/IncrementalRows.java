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
import java.util.NoSuchElementException;

/**
 * Rows implementation which returns rows incrementally from result set
 * without any buffering.
 */
class IncrementalRows extends Rows {
  private final ResultSet rs;
  private boolean firstRow;
  private Row labelRow;
  private Row maxRow;
  private Row nextRow;
  private Row nextNextRow;
  private boolean endOfResult;
  private boolean normalizingWidths;
  private DispatchCallback dispatchCallback;

  IncrementalRows(SqlLine sqlLine, ResultSet rs,
      DispatchCallback dispatchCallback) throws SQLException {
    super(sqlLine, rs);
    this.rs = rs;
    this.dispatchCallback = dispatchCallback;
    endOfResult = false;
    computeMetadata();
    firstRow = true;
  }

  private void computeMetadata() throws SQLException {
    labelRow = new Row(rsMeta.getColumnCount());
    maxRow = new Row(rsMeta.getColumnCount());

    // pre-compute normalization so we don't have to deal
    // with SQLExceptions later
    for (int i = 0; i < maxRow.sizes.length; ++i) {
      // Normalized display width is based on maximum of display size
      // and label size.
      //
      // H2 returns Integer.MAX_VALUE, so avoid that.
      final int displaySize = rsMeta.getColumnDisplaySize(i + 1);
      if (displaySize > maxRow.sizes[i]
          && displaySize < Integer.MAX_VALUE) {
        maxRow.sizes[i] = displaySize;
      }
    }

    nextRow = labelRow;
  }

  public boolean hasNext() {
    if (endOfResult || dispatchCallback.isCanceled()) {
      return false;
    }

    // check to see if we have more data and maybe need to compute a
    // change in metadata right off the bat
    if (firstRow) {
      firstRow = false;
      try {
        // no next row, so just return the current label
        if (!rs.next()) {
          return true;
        }

        // we have a next row, store that as the next-next row
        // but first, check to see if we need to change the label
        recomputeMeta();
        this.nextNextRow = new Row(labelRow.sizes.length, rs);
        if (normalizingWidths) {
          normalize(nextNextRow);
        }
        return true;
      } catch (SQLException ex) {
        throw new WrappedSqlException(ex);
      }
    }

    // its not the first row
    if (nextRow != null) {
      return true;
    }

    // we don't have a next row, try and get one
    try {
      if (!rs.next()) {
        // no more data - we are done!
        endOfResult = true;
        return false;
      }

      // we have a next row. Maybe the metadata needs to be recomputed
      if (recomputeMeta()) {
        // we need a new label row and to push off the 'next' row off by one row
        this.nextRow = this.labelRow;
        this.nextNextRow = new Row(labelRow.sizes.length, rs);
      } else {
        // its just a normal row, so add it normally
        nextRow = new Row(labelRow.sizes.length, rs);
      }
      if (normalizingWidths) {
        normalize(nextRow);
        normalize(nextNextRow);
      }
    } catch (SQLException e) {
      throw new WrappedSqlException(e);
    }
    return nextRow != null;
  }
//      try {
//        if (rs.next()) {
//          // check to see if the
//
//          nextRow = new Row(labelRow.sizes.length, rs);
//
//          if (normalizingWidths) {
//            // perform incremental normalization
//            nextRow.sizes = labelRow.sizes;
//          }
//        } else {
//          endOfResult = true;
//        }
//      } catch (SQLException ex) {
//        throw new WrappedSqlException(ex);
//      }
//    }
//
//    return nextRow != null;
//  }

  private void normalize(Row row) {
    if (row != null) {
      row.sizes = labelRow.sizes;
    }
  }

  private boolean recomputeMeta() throws SQLException {
    int count = rsMeta.getColumnCount();
    if (count != this.labelRow.sizes.length) {
      computeMetadata();
      return true;
    }
    return false;
  }

  public Row next() {
    if (!hasNext() && !dispatchCallback.isCanceled()) {
      throw new NoSuchElementException();
    }

    Row ret = nextRow;
    nextRow = nextNextRow;
    nextNextRow = null;
    return ret;
  }

  void normalizeWidths() {
    // normalize label row
    labelRow.sizes = maxRow.sizes;

    // and remind ourselves to perform incremental normalization
    // for each row as it is produced
    normalizingWidths = true;
  }
}

// End IncrementalRows.java
