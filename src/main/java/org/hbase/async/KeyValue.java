/*
 * Copyright (C) 2015  The Async BigTable Authors.  All rights reserved.
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

import java.util.Arrays;

/**
 * A "cell" in an HBase table.
 * <p>
 * This represents one unit of HBase data, one "record".
 *
 * <h1>A note {@code byte} arrays</h1>
 * This class will never copy any {@code byte[]} that's given to it, neither
 * will it create a copy before returning one to you.
 * <strong>Changing a byte array get from or pass to this class will have
 * <em>unpredictable</em> consequences</strong>.  In particular, multiple
 * {@link KeyValue} instances may share the same byte arrays, so changing
 * one instance may also unexpectedly affect others.
 */
public final class KeyValue implements Comparable<KeyValue> {

  /**
   * Timestamp value to let the server set the timestamp at processing time.
   * When this value is used as a timestamp on a {@code KeyValue}, the server
   * will substitute a real timestamp at the time it processes it.  HBase uses
   * current UNIX time in milliseconds.
   */
  public static final long TIMESTAMP_NOW = Long.MAX_VALUE;

  //private static final Logger LOG = LoggerFactory.getLogger(KeyValue.class);

  private final byte[] key;     // Max length: Short.MAX_VALUE = 32768
  private final byte[] family;  // Max length: Byte.MAX_VALUE  =   128
  private final byte[] qualifier;
  private final byte[] value;
  private final long timestamp;
  
  /**
   * Constructor.
   * @param key The row key.  Length must fit in 16 bits.
   * @param family The column family.  Length must fit in 8 bits.
   * @param qualifier The column qualifier.
   * @param timestamp Timestamp on the value.  This timestamp can be set to
   * guarantee ordering of values or operations.  It is strongly advised to
   * use a UNIX timestamp in milliseconds, e.g. from a source such as
   * {@link System#currentTimeMillis}.  This value must be strictly positive.
   * @param value The value, the contents of the cell.
   * @throws IllegalArgumentException if any argument is invalid (e.g. array
   * size is too long) or if the timestamp is negative.
   */
  public KeyValue(final byte[] key,
                  final byte[] family, final byte[] qualifier,
                  final long timestamp,
                  //final byte type,
                  final byte[] value) {
    checkKey(key);
    checkFamily(family);
    checkQualifier(qualifier);
    checkTimestamp(timestamp);
    checkValue(value);
    this.key = key;
    this.family = family;
    this.qualifier = qualifier;
    this.value = value;
    this.timestamp = timestamp;
    //this.type = type;
  }

  /**
   * Constructor.
   * <p>
   * This {@code KeyValue} will be timestamped by the server at the time
   * the server processes it.
   * @param key The row key.  Length must fit in 16 bits.
   * @param family The column family.  Length must fit in 8 bits.
   * @param qualifier The column qualifier.
   * @param value The value, the contents of the cell.
   * @throws IllegalArgumentException if any argument is invalid (e.g. array
   * size is too long).
   * @see #TIMESTAMP_NOW
   */
  public KeyValue(final byte[] key,
                  final byte[] family, final byte[] qualifier,
                  final byte[] value) {
    this(key, family, qualifier, TIMESTAMP_NOW, value);
  }

  /** Returns the row key.  */
  public byte[] key() {
    return key;
  }

  /** Returns the column family.  */
  public byte[] family() {
    return family;
  }

  /** Returns the column qualifier.  */
  public byte[] qualifier() {
    return qualifier;
  }

  /**
   * Returns the timestamp stored in this {@code KeyValue}.
   * @see #TIMESTAMP_NOW
   */
  public long timestamp() {
    return timestamp;
  }
  
  /** Returns the value, the contents of the cell.  */
  public byte[] value() {
    return value;
  }

  @Override
  public int compareTo(final KeyValue other) {
    int d;
    if ((d = Bytes.memcmp(key, other.key)) != 0) {
      return d;
    } else if ((d = Bytes.memcmp(family, other.family)) != 0) {
      return d;
    } else if ((d = Bytes.memcmp(qualifier, other.qualifier)) != 0) {
      return d;
    //} else if ((d = Bytes.memcmp(value, other.value)) != 0) {
    //  return d;
    } else if ((d = Long.signum(timestamp - other.timestamp)) != 0) {
      return d;
    } else {
    //  d = type - other.type;
      d = Bytes.memcmp(value, other.value);
    }
    return d;
  }

  public boolean equals(final Object other) {
    if (other == null || !(other instanceof KeyValue)) {
      return false;
    }
    return compareTo((KeyValue) other) == 0;
  }

  public int hashCode() {
    return Arrays.hashCode(key)
      ^ Arrays.hashCode(family)
      ^ Arrays.hashCode(qualifier)
      ^ Arrays.hashCode(value)
      ^ (int) (timestamp ^ (timestamp >>> 32))
      //^ type
      ;
  }

  public String toString() {
    final StringBuilder buf = new StringBuilder(84  // Boilerplate + timestamp
      // the row key is likely to contain non-ascii characters, so
      // let's multiply its length by 2 to avoid re-allocations.
      + key.length * 2 + family.length + qualifier.length + value.length);
    buf.append("KeyValue(key=");
    Bytes.pretty(buf, key);
    buf.append(", family=");
    Bytes.pretty(buf, family);
    buf.append(", qualifier=");
    Bytes.pretty(buf, qualifier);
    buf.append(", value=");
    Bytes.pretty(buf, value);
    buf.append(", timestamp=").append(timestamp);
    //  .append(", type=").append(type);
    buf.append(')');
    return buf.toString();
  }
  
  // OK this isn't technically part of a KeyValue but since all the similar
  // functions are here, let's keep things together in one place.
  /**
   * Validates a table name.
   * @throws IllegalArgumentException if the table name is too big or
   * malformed.
   * @throws NullPointerException if the table name is {@code null}.
   */
  static void checkTable(final byte[] table) {
    if (table.length > Byte.MAX_VALUE) {
      throw new IllegalArgumentException("Table name too long: "
        + table.length + " bytes long " + Bytes.pretty(table));
    } else if (table.length == 0) {
      throw new IllegalArgumentException("empty table name");
    }
  }

  /**
   * Validates a row key.
   * @throws IllegalArgumentException if the key is too big.
   * @throws NullPointerException if the key is {@code null}.
   */
  static void checkKey(final byte[] key) {
    if (key.length > Short.MAX_VALUE) {
      throw new IllegalArgumentException("row key too long: "
        + key.length + " bytes long " + Bytes.pretty(key));
    }
  }

  /**
   * Validates a column family.
   * @throws IllegalArgumentException if the family name is too big.
   * @throws NullPointerException if the family is {@code null}.
   */
  static void checkFamily(final byte[] family) {
    if (family.length > Byte.MAX_VALUE) {
      throw new IllegalArgumentException("column family too long: "
        + family.length + " bytes long " + Bytes.pretty(family));
    }
  }

  /**
   * Validates a column qualifier.
   * @throws IllegalArgumentException if the qualifier name is too big.
   * @throws NullPointerException if the qualifier is {@code null}.
   */
  static void checkQualifier(final byte[] qualifier) {
    HBaseRpc.checkArrayLength(qualifier);
  }

  /**
   * Validates a timestamp.
   * @throws IllegalArgumentException if the timestamp is zero or negative.
   */
  static void checkTimestamp(final long timestamp) {
    if (timestamp < 0) {
      throw new IllegalArgumentException("Negative timestamp: " + timestamp);
    }
  }

  /**
   * Validates a value (the contents of an HBase cell).
   * @throws IllegalArgumentException if the value is too big.
   * @throws NullPointerException if the value is {@code null}.
   */
  static void checkValue(final byte[] value) {
    HBaseRpc.checkArrayLength(value);
  }
  
}
