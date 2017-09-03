/*
 * Copyright (C) 2015-2017  The Async BigTable Authors.  All rights reserved.
 * This file is part of Async BigTable.
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

import org.apache.hadoop.hbase.filter.Filter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Combines a list of filters into one.
 * Since 1.5
 */
public final class FilterList extends ScanFilter {

  private static final byte[] NAME = Bytes.ISO88591("org.apache.hadoop"
      + ".hbase.filter.FilterList");

  private final List<ScanFilter> scan_filters;
  private final List<Filter> filters;
  private final Operator op;

  /**
   * Operator to combine the list of filters together.
   * since 1.5
   */
  public enum Operator {
    /** All the filters must pass ("and" semantic).  */
    MUST_PASS_ALL,
    /** At least one of the filters must pass ("or" semantic).  */
    MUST_PASS_ONE;
    
    static org.apache.hadoop.hbase.filter.FilterList.Operator convert(final Operator op) {
      switch(op) {
      case MUST_PASS_ALL:
        return org.apache.hadoop.hbase.filter.FilterList.Operator.MUST_PASS_ALL;
      default:
        return org.apache.hadoop.hbase.filter.FilterList.Operator.MUST_PASS_ONE;
      }
    }
  }

  /**
   * Constructor.
   * Equivalent to {@link #FilterList(List, Operator)
   * FilterList}{@code (filters, }{@link Operator#MUST_PASS_ALL}{@code )}
   */
  public FilterList(final List<ScanFilter> filters) {
    this(filters, Operator.MUST_PASS_ALL);
  }

  /**
   * Constructor.
   * @param filters The filters to combine.  <strong>This list does not get
   * copied, do not mutate it after passing it to this object</strong>.
   * @param op The operator to use to combine the filters together.
   * @throws IllegalArgumentException if the list of filters is empty.
   */
  public FilterList(final List<ScanFilter> filters, final Operator op) {
    if (filters.isEmpty()) {
      throw new IllegalArgumentException("Empty filter list");
    }
    scan_filters = filters;
    this.filters = Lists.newArrayListWithCapacity(filters.size());
    for (final ScanFilter f : filters) {
      this.filters.add(f.getBigtableFilter());
    }
    this.op = op;
  }

  @Override
  byte[] name() {
    return NAME;
  }

  public String toString() {
    final StringBuilder buf = new StringBuilder(32 + filters.size() * 48);
    buf.append("FilterList(filters=[");
    for (final Filter filter : filters) {
      buf.append(filter.toString());
      buf.append(", ");
    }
    buf.setLength(buf.length() - 2);  // Remove the last ", "
    buf.append("], op=").append(op).append(")");
    return buf.toString();
  }

  /** @return An immutable copy of the filter list (though each filter could 
   * still be modified)
   *  @since 1.8 */
  public List<ScanFilter> filters() {
    return ImmutableList.copyOf(scan_filters);
  }
  
  List<Filter> getFilters() {
    return filters;
  }

  @Override
  Filter getBigtableFilter() {
    org.apache.hadoop.hbase.filter.FilterList list = 
        new org.apache.hadoop.hbase.filter.FilterList(Operator.convert(op));
    for (final Filter f : filters) {
      list.addFilter(f);
    }
    return list;
  }
}
