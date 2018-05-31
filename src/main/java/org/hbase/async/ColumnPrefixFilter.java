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

    @Override
    byte[] name() {
        return NAME;
    }

    Filter getBigtableFilter() {
        return new org.apache.hadoop.hbase.filter.ColumnPrefixFilter(prefix);
    }
}
