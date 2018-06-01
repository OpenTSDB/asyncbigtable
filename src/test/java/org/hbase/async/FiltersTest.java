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
