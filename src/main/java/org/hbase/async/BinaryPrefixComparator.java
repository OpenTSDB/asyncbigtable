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

import java.lang.reflect.Field;

import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.RegexStringComparator;

/**
 * A binary comparator used in comparison filters. Compares byte arrays
 * lexicographically up to the length of the provided byte array.
 * @since 1.6
 */
public final class BinaryPrefixComparator extends FilterComparator {

  private static final byte[] NAME =
      Bytes.UTF8("org.apache.hadoop.hbase.filter.BinaryPrefixComparator");
  private static final byte CODE = 47;

  private final byte[] value;

  public BinaryPrefixComparator(byte[] value) {
    this.value = value;
  }

  public byte[] value() {
    return value.clone();
  }

  @Override
  byte[] name() {
    return NAME;
  }
  
  @Override
  public String toString() {
    return String.format("%s(%s)",
        getClass().getSimpleName(),
        Bytes.pretty(value));
  }

  public ByteArrayComparable getBigtableFilter() {
    // NOTE: Bigtable doesn't support the BinaryPrefixComparator (grrrr).
    // Se https://cloud.google.com/bigtable/docs/hbase-differences#filters.
    // So we have to convert it to a regex filter.
    StringBuilder buf = new StringBuilder()
        .append("(?s)\\Q");
    
    for (int i = 0; i < value.length; i++) {
      buf.append((char) (value[i] & 0xFF));
    }
    buf.append("\\E.*");
    try {
      // WARNING: This is some ugly ass code. It WILL break at some point. 
      // Bigtable uses RE2 and runs in raw byte mode. TSDB writes regex with 
      // byte values but when passing it through the HTable APIs it's converted
      // to UTF and serializes differently than the old AsyncHBase client. The
      // native BigTable client will pass the regex along properly BUT we need
      // to bypass the RegexStringComparator methods and inject our ASCII regex
      // directly into the underlying comparator object. Hopefully this is 
      // temporary (famous last words) until we get to a native Bigtable wrapper.
      RegexStringComparator comparator = new RegexStringComparator(buf.toString());
      final Field field = ByteArrayComparable.class.getDeclaredField("value");
      field.setAccessible(true);
      field.set(comparator, buf.toString().getBytes(HBaseClient.ASCII));
      field.setAccessible(false);
      return comparator;
    } catch (NoSuchFieldException e) {
      throw new RuntimeException("ByteArrayComparator must have changed, "
          + "can't find the field", e);
    } catch (IllegalAccessException e) {
       throw new RuntimeException("Access denied when hacking the "
          + "regex comparator field", e);
    }
  }
}
