package org.hbase.async;

import org.apache.hadoop.hbase.filter.Filter;

public final class ColumnPrefixFilter extends ScanFilter {

    private static final byte[] NAME =
            Bytes.UTF8("org.apache.hadoop.hbase.filter.ColumnPrefixFilter");
    private final byte[] prefix;

    public byte[] getPrefix() {
        return this.prefix;
    }

    public ColumnPrefixFilter(byte[] prefix) {
        this.prefix = prefix;
    }

    Filter getBigtableFilter() {
        return new org.apache.hadoop.hbase.filter.ColumnPrefixFilter(prefix);
    }

    @Override
    byte[] name() {
        return NAME;
    }

    public String toString() {
        return "ColumnPrefixFilter(" + Bytes.pretty(prefix) + ")";
    }
}
