package org.hbase.async;

import java.util.ArrayList;

public class GetResultOrException {
  final private ArrayList<KeyValue> cells;
  final private Exception exception;

  public GetResultOrException(final ArrayList<KeyValue> cells) {
    this.cells = cells;
    this.exception = null;
  }

  public GetResultOrException(final Exception exp) {
    this.exception = exp;
    this.cells = null;
  }

  public ArrayList<KeyValue> getCells() {
    return this.cells;
  }

  public Exception getException() {
    return this.exception;
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append("cells : ").append(this.cells == null ? "null" : this.cells);
    sb.append("; exception: ").append(this.exception == null ? "null" : this.exception);
    sb.append("}");
    return sb.toString();
  }
}

