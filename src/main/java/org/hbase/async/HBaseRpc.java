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

import com.stumbleupon.async.Deferred;

/**
 * Abstract base class for all RPC requests going out to BigTable.
 * <p>
 * Implementations of this class are <b>not</b> expected to be synchronized.
 *
 * <h1>A note on passing {@code byte} arrays in argument</h1>
 * None of the method that receive a {@code byte[]} in argument will copy it.
 * If you change the contents of any byte array you give to an instance of
 * this class, you <em>may</em> affect the behavior of the request in an
 * <strong>unpredictable</strong> way.  If you need to change the byte array,
 * {@link Object#clone() clone} it before giving it to this class.  For those
 * familiar with the term "defensive copy", we don't do it in order to avoid
 * unnecessary memory copies when you know you won't be changing (or event
 * holding a reference to) the byte array, which is frequently the case.
 */
public abstract class HBaseRpc {

  /**
   * An RPC from which you can get a table name.
   */
  public interface HasTable {
    /**
     * Returns the name of the table this RPC is for.
     * <p>
     * <strong>DO NOT MODIFY THE CONTENTS OF THE ARRAY RETURNED.</strong>
     */
    public byte[] table();
  }

  /**
   * An RPC from which you can get a row key name.
   */
  public interface HasKey {
    /**
     * Returns the row key this RPC is for.
     * <p>
     * <strong>DO NOT MODIFY THE CONTENTS OF THE ARRAY RETURNED.</strong>
     */
    public byte[] key();
  }

  /**
   * An RPC from which you can get a family name.
   */
  public interface HasFamily {
    /**
     * Returns the family this RPC is for.
     * <p>
     * <strong>DO NOT MODIFY THE CONTENTS OF THE ARRAY RETURNED.</strong>
     */
    public byte[] family();
  }

  /**
   * An RPC from which you can get a column qualifier name.
   */
  public interface HasQualifier {
    /**
     * Returns the column qualifier this RPC is for.
     * <p>
     * <strong>DO NOT MODIFY THE CONTENTS OF THE ARRAY RETURNED.</strong>
     */
    public byte[] qualifier();
  }

  /**
   * An RPC from which you can get multiple column qualifier names.
   */
  public interface HasQualifiers {
    /**
     * Returns the column qualifiers this RPC is for.
     * <p>
     * <strong>DO NOT MODIFY THE CONTENTS OF THE ARRAY RETURNED.</strong>
     */
    public byte[][] qualifiers();
  }

  /**
   * An RPC from which you can get a value.
   */
  public interface HasValue {
    /**
     * Returns the value contained in this RPC.
     * <p>
     * <strong>DO NOT MODIFY THE CONTENTS OF THE ARRAY RETURNED.</strong>
     */
    public byte[] value();
  }

  /**
   * An RPC from which you can get multiple values.
   */
  public interface HasValues {
    /**
     * Returns the values contained in this RPC.
     * <p>
     * <strong>DO NOT MODIFY THE CONTENTS OF THE ARRAY RETURNED.</strong>
     */
    public byte[][] values();
  }

  /**
   * An RPC from which you can get a timestamp.
   */
  public interface HasTimestamp {
    /** Returns the strictly positive timestamp contained in this RPC.  */
    public long timestamp();
  }

  /**
   * Package-private interface to mark RPCs that are changing data in HBase.
   */
  interface IsEdit {
    /** RPC method name to use with HBase 0.95+.  */
    static final byte[] MUTATE = { 'M', 'u', 't', 'a', 't', 'e' };
  }
  
  /**
   * The Deferred that will be invoked when this RPC completes or fails.
   * In case of a successful completion, this Deferred's first callback
   * will be invoked with an {@link Object} containing the de-serialized
   * RPC response in argument.
   * Once an RPC has been used, we create a new Deferred for it, in case
   * the user wants to re-use it.
   */
  private Deferred<Object> deferred;
  
  /**
   * The table for which this RPC is.
   * {@code null} if this RPC isn't for a particular table.
   * Invariants:
   *   table == null  =>  key == null
   *   table != null  =>  key != null
   */
  final byte[] table;  // package-private for subclasses, not other classes.

  /**
   * The row key for which this RPC is.
   * {@code null} if this RPC isn't for a particular row key.
   * Invariants:
   *   table == null  =>  key == null
   *   table != null  =>  key != null
   */
  final byte[] key;  // package-private for subclasses, not other classes.

  /**
   * How many times have we retried this RPC?.
   * Only used by the low-level retry logic in {@link RegionClient} in order
   * to detect broken META tables (e.g. we keep getting an NSRE but META keeps
   * sending us to the same RegionServer that sends us the NSREs, or we keep
   * looking up the same row in META because of a "hole" in META).
   * <p>
   * Proper synchronization is required, although in practice most of the code
   * that access this attribute will have a happens-before relationship with
   * the rest of the code, due to other existing synchronization.
   */
  byte attempt;  // package-private for RegionClient and HBaseClient only.

  /**
   * If true, this RPC should fail-fast as soon as we know we have a problem.
   */
  boolean failfast = false;

  /**
   * Set whether or not the RPC not be retried upon encountering a problem.
   * <p>
   * RPCs can be retried for various legitimate reasons (e.g. NSRE due to a
   * region moving), but under certain failure circumstances (such as a node
   * going down) we want to give up and be alerted as soon as possible.
   * @param failfast If {@code true}, this RPC should fail-fast as soon as
   * we know we have a problem.
   */
  public final boolean setFailfast(final boolean failfast) {
    return this.failfast = failfast;
  }

  /**
   * Returns whether or not the RPC not be retried upon encountering a problem.
   * @see #setFailfast
   */
  public final boolean failfast() {
    return failfast;
  }

  /**
   * Package private constructor for RPCs that aren't for any region.
   */
  HBaseRpc() {
    table = null;
    key = null;
  }

  /**
   * Package private constructor for RPCs that are for a region.
   * @param table The name of the table this RPC is for.
   * @param row The name of the row this RPC is for.
   */
  HBaseRpc(final byte[] table, final byte[] key) {
    KeyValue.checkTable(table);
    KeyValue.checkKey(key);
    this.table = table;
    this.key = key;
  }

  // ---------------------- //
  // Package private stuff. //
  // ---------------------- //

  /** Package private way of accessing / creating the Deferred of this RPC.  */
  final Deferred<Object> getDeferred() {
    if (deferred == null) {
      deferred = new Deferred<Object>();
    }
    return deferred;
  }

  /**
   * Package private way of making an RPC complete by giving it its result.
   * If this RPC has no {@link Deferred} associated to it, nothing will
   * happen.  This may happen if the RPC was already called back.
   * <p>
   * Once this call to this method completes, this object can be re-used to
   * re-send the same RPC, provided that no other thread still believes this
   * RPC to be in-flight (guaranteeing this may be hard in error cases).
   */
  final void callback(final Object result) {
    final Deferred<Object> d = deferred;
    if (d == null) {
      return;
    }
    deferred = null;
    attempt = 0;
    d.callback(result);
  }

  /** Checks whether or not this RPC has a Deferred without creating one.  */
  final boolean hasDeferred() {
    return deferred != null;
  }

  public String toString() {
    // Try to rightsize the buffer.
    final String method = "method"; //new String(this.method((byte) 0));
    final StringBuilder buf = new StringBuilder(16 + method.length() + 2
      + 8 + (table == null ? 4 : table.length + 2)  // Assumption: ASCII => +2
      + 6 + (key == null ? 4 : key.length * 2)      // Assumption: binary => *2
     // + 9 + (region == null ? 4 : region.stringSizeHint())
      + 10 + 1 + 1);
    buf.append("HBaseRpc(method=");
    buf.append(method);
    buf.append(", table=");
    Bytes.pretty(buf, table);
    buf.append(", key=");
    Bytes.pretty(buf, key);
    buf.append(", region=");

    buf.append(", attempt=").append(attempt);
    buf.append(')');
    return buf.toString();
  }

  /**
   * Helper for subclass's {@link #toString} implementations.
   * <p>
   * This is used by subclasses such as {@link DeleteRequest}
   * or {@link GetRequest}, to avoid code duplication.
   * @param classname The name of the class of the caller.
   * @param family A possibly null family name.
   * @param qualifiers A non-empty list of qualifiers or null.
   */
  final String toStringWithQualifiers(final String classname,
                                      final byte[] family,
                                      final byte[][] qualifiers) {
    return toStringWithQualifiers(classname, family, qualifiers, null, "");
  }


  /**
   * Helper for subclass's {@link #toString} implementations.
   * <p>
   * This is used by subclasses such as {@link DeleteRequest}
   * or {@link GetRequest}, to avoid code duplication.
   * @param classname The name of the class of the caller.
   * @param family A possibly null family name.
   * @param qualifiers A non-empty list of qualifiers or null.
   * @param values A non-empty list of values or null.
   * @param fields Additional fields to include in the output.
   */
  final String toStringWithQualifiers(final String classname,
                                      final byte[] family,
                                      final byte[][] qualifiers,
                                      final byte[][] values,
                                      final String fields) {
    final StringBuilder buf = new StringBuilder(256  // min=182
                                                + fields.length());
    buf.append(classname).append("(table=");
    Bytes.pretty(buf, table);
    buf.append(", key=");
    Bytes.pretty(buf, key);
    buf.append(", family=");
    Bytes.pretty(buf, family);
    buf.append(", qualifiers=");
    Bytes.pretty(buf, qualifiers);
    if (values != null) {
      buf.append(", values=");
      Bytes.pretty(buf, values);
    }
    buf.append(fields);
    buf.append(", attempt=").append(attempt)
      .append(", region=");
//    if (region == null) {
//      buf.append("null");
//    } else {
//      region.toStringbuf(buf);
//    }
    buf.append(')');
    return buf.toString();
  }

  /**
   * Helper for subclass's {@link #toString} implementations.
   * <p>
   * This is used by subclasses such as {@link DeleteRequest}
   * or {@link GetRequest}, to avoid code duplication.
   * @param classname The name of the class of the caller.
   * @param family A possibly null family name.
   * @param qualifier A possibly null column qualifier.
   * @param fields Additional fields to include in the output.
   */
  final String toStringWithQualifier(final String classname,
                                     final byte[] family,
                                     final byte[] qualifier,
                                     final String fields) {
    final StringBuilder buf = new StringBuilder(256  // min=181
                                                + fields.length());
    buf.append(classname).append("(table=");
    Bytes.pretty(buf, table);
    buf.append(", key=");
    Bytes.pretty(buf, key);
    buf.append(", family=");
    Bytes.pretty(buf, family);
    buf.append(", qualifier=");
    Bytes.pretty(buf, qualifier);
    buf.append(fields);
    buf.append(", attempt=").append(attempt)
      .append(", region=");
//    if (region == null) {
//      buf.append("null");
//    } else {
//      region.toStringbuf(buf);
//    }
    buf.append(')');
    return buf.toString();
  }
  
  /**
   * Upper bound on the size of a byte array we de-serialize.
   * This is to prevent BigTable from OOM'ing us, should there be a bug or
   * undetected corruption of an RPC on the network, which would turn a
   * an innocuous RPC into something allocating a ton of memory.
   * The Hadoop RPC protocol doesn't do any checksumming as they probably
   * assumed that TCP checksums would be sufficient (they're not).
   */
   static final long MAX_BYTE_ARRAY_MASK =
      0xFFFFFFFFF0000000L;  // => max = 256MB == 268435455
  
  /**
   * Verifies that the given array looks like a reasonably big array.
   * This method accepts empty arrays.
   * @param array The array to check.
   * @throws IllegalArgumentException if the length of the array is
   * suspiciously large.
   * @throws NullPointerException if the array is {@code null}.
   */
  static void checkArrayLength(final byte[] array) {
    if ((array.length & MAX_BYTE_ARRAY_MASK) != 0) {
      if (array.length < 0) {  // Not possible unless there's a JVM bug.
        throw new AssertionError("Negative byte array length: "
                                 + array.length + ' ' + Bytes.pretty(array));
      } else {
        throw new IllegalArgumentException("Byte array length too big: "
          + array.length + " > " + ~MAX_BYTE_ARRAY_MASK);
        // Don't dump the gigantic byte array in the exception message.
      }
    }
  }
}
