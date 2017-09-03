/*
 * Copyright (C) 2014-2017  The Async HBase Authors.  All rights reserved.
 * This file is part of Async HBase.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   - Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   - Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   - Neither the name of the StumbleUpon nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.hbase.async;

/**
 * A generic scan filter to be used to filter by comparison. It takes an
 * operator (equal, greater, not equal, etc) and a filter comparator.
 * @since 1.6
 */
public abstract class CompareFilter extends ScanFilter {

  /** Comparison operators. */
  public enum CompareOp {
    /** less than */
    LESS,
    /** less than or equal to */
    LESS_OR_EQUAL,
    /** equals */
    EQUAL,
    /** not equal */
    NOT_EQUAL,
    /** greater than or equal to */
    GREATER_OR_EQUAL,
    /** greater than */
    GREATER,
    /** no operation */
    NO_OP;
    
    static org.apache.hadoop.hbase.filter.CompareFilter.CompareOp convert(final CompareOp op) {
      switch(op) {
      case LESS:
        return org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.LESS;
      case LESS_OR_EQUAL:
        return org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.LESS_OR_EQUAL;
      case EQUAL:
        return org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.EQUAL;
      case NOT_EQUAL:
        return org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.NOT_EQUAL;
      case GREATER_OR_EQUAL:
        return org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.GREATER_OR_EQUAL;
      case GREATER: 
        return org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.GREATER;
      default:
        return org.apache.hadoop.hbase.filter.CompareFilter.CompareOp.NO_OP;
      }
    }
  }

  protected final CompareOp compare_op;
  protected final FilterComparator comparator;

  public CompareFilter(final CompareOp compareOp,
                       final FilterComparator comparator) {
    this.compare_op = compareOp;
    this.comparator = comparator;
  }

  public CompareOp compareOperation() {
      return compare_op;
  }

  public FilterComparator comparator() {
      return comparator;
  }
  
  @Override
  public String toString() {
    return String.format("%s(%s, %s)",
        getClass().getSimpleName(),
        compare_op.name(),
        comparator.toString());
  }
  
}
