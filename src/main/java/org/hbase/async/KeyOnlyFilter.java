package org.hbase.async;

import org.apache.hadoop.hbase.filter.Filter;

public class KeyOnlyFilter extends ScanFilter {

    private static final byte[] NAME =
            Bytes.UTF8("org.apache.hadoop.hbase.filter.KeyOnlyFilter");
    private final boolean lenAsVal;

    public KeyOnlyFilter() {
        this(false);
    }

    public KeyOnlyFilter(boolean lenAsVal) {
        this.lenAsVal = lenAsVal;
    }

    Filter getBigtableFilter() {
        return new org.apache.hadoop.hbase.filter.KeyOnlyFilter(lenAsVal);
    }

    @Override
    byte[] name() {
        return NAME;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + " "
                + "lenAsVal: " + (lenAsVal ? "true" : "false");
    }
}
