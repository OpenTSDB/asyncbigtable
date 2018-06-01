package org.hbase.async;

import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FiltersTest {

    @Test
    public void ensureKeyOnlyFilterIsCorrectlyCreated() {
        KeyOnlyFilter keyOnlyFilter = new KeyOnlyFilter();
        org.apache.hadoop.hbase.filter.KeyOnlyFilter filter = (org.apache.hadoop.hbase.filter.KeyOnlyFilter) keyOnlyFilter.getBigtableFilter();
        Assert.assertNotNull(filter);
        Assert.assertArrayEquals(filter.toByteArray(), keyOnlyToByteArray(false));
    }

    @Test
    public void ensureKeyOnlyFilterIsCorrectlyCreatedWithArgs() {
        KeyOnlyFilter keyOnlyFilter = new KeyOnlyFilter(true);
        org.apache.hadoop.hbase.filter.KeyOnlyFilter filter = (org.apache.hadoop.hbase.filter.KeyOnlyFilter) keyOnlyFilter.getBigtableFilter();
        Assert.assertNotNull(filter);
        Assert.assertArrayEquals(filter.toByteArray(), keyOnlyToByteArray(true));
    }

    @Test
    public void ensureColumnPrefixFilterIsCorrectlyCreated() {
        final byte[] prefix = Bytes.UTF8("aoeu");
        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(prefix);
        org.apache.hadoop.hbase.filter.ColumnPrefixFilter filter = (org.apache.hadoop.hbase.filter.ColumnPrefixFilter) columnPrefixFilter.getBigtableFilter();
        Assert.assertNotNull(filter);
        Assert.assertArrayEquals(filter.getPrefix(), prefix);
    }

    private byte[] keyOnlyToByteArray(boolean value) {
        FilterProtos.KeyOnlyFilter.Builder builder = org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos.KeyOnlyFilter.newBuilder();
        builder.setLenAsVal(value);
        return builder.build().toByteArray();
    }

}
