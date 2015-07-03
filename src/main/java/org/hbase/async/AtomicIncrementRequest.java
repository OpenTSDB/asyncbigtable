/*
 * Copyright (C) 2010-2012  The Async HBase Authors.  All rights reserved.
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
 * Atomically increments a value in HBase.
 *
 * <h1>A note on passing {@code byte} arrays in argument</h1>
 * None of the method that receive a {@code byte[]} in argument will copy it.
 * For more info, please refer to the documentation of {@link HBaseRpc}.
 * <h1>A note on passing {@code String}s in argument</h1>
 * All strings are assumed to use the platform's default charset.
 */
public final class AtomicIncrementRequest extends HBaseRpc
  implements HBaseRpc.HasTable, HBaseRpc.HasKey,
             HBaseRpc.HasFamily, HBaseRpc.HasQualifier, HBaseRpc.IsEdit {


  private final byte[] family;
  private final byte[] qualifier;
  private long amount;
  private boolean durable = true;

  /**
   * Constructor.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The non-empty name of the table to use.
   * @param key The row key of the value to increment.
   * @param family The column family of the value to increment.
   * @param qualifier The column qualifier of the value to increment.
   * @param amount Amount by which to increment the value in HBase.
   * If negative, the value in HBase will be decremented.
   */
  public AtomicIncrementRequest(final byte[] table,
                                final byte[] key,
                                final byte[] family,
                                final byte[] qualifier,
                                final long amount) {
    super(table, key);
    KeyValue.checkFamily(family);
    KeyValue.checkQualifier(qualifier);
    this.family = family;
    this.qualifier = qualifier;
    this.amount = amount;
  }

  /**
   * Constructor.  This is equivalent to:
   * {@link #AtomicIncrementRequest(byte[], byte[], byte[], byte[], long)
   * AtomicIncrementRequest}{@code (table, key, family, qualifier, 1)}
   * <p>
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The non-empty name of the table to use.
   * @param key The row key of the value to increment.
   * @param family The column family of the value to increment.
   * @param qualifier The column qualifier of the value to increment.
   */
  public AtomicIncrementRequest(final byte[] table,
                                final byte[] key,
                                final byte[] family,
                                final byte[] qualifier) {
    this(table, key, family, qualifier, 1);
  }

  /**
   * Constructor.
   * All strings are assumed to use the platform's default charset.
   * @param table The non-empty name of the table to use.
   * @param key The row key of the value to increment.
   * @param family The column family of the value to increment.
   * @param qualifier The column qualifier of the value to increment.
   * @param amount Amount by which to increment the value in HBase.
   * If negative, the value in HBase will be decremented.
   */
  public AtomicIncrementRequest(final String table,
                                final String key,
                                final String family,
                                final String qualifier,
                                final long amount) {
    this(table.getBytes(), key.getBytes(), family.getBytes(),
         qualifier.getBytes(), amount);
  }

  /**
   * Constructor.  This is equivalent to:
   * All strings are assumed to use the platform's default charset.
   * {@link #AtomicIncrementRequest(String, String, String, String, long)
   * AtomicIncrementRequest}{@code (table, key, family, qualifier, 1)}
   * @param table The non-empty name of the table to use.
   * @param key The row key of the value to increment.
   * @param family The column family of the value to increment.
   * @param qualifier The column qualifier of the value to increment.
   */
  public AtomicIncrementRequest(final String table,
                                final String key,
                                final String family,
                                final String qualifier) {
    this(table.getBytes(), key.getBytes(), family.getBytes(),
         qualifier.getBytes(), 1);
  }

  /**
   * Returns the amount by which the value is going to be incremented.
   */
  public long getAmount() {
    return amount;
  }

  /**
   * Changes the amount by which the value is going to be incremented.
   * @param amount The new amount.  If negative, the value will be decremented.
   */
  public void setAmount(final long amount) {
    this.amount = amount;
  }


  @Override
  public byte[] table() {
    return table;
  }

  @Override
  public byte[] key() {
    return key;
  }

  @Override
  public byte[] family() {
    return family;
  }

  @Override
  public byte[] qualifier() {
    return qualifier;
  }

  public String toString() {
    return super.toStringWithQualifier("AtomicIncrementRequest",
                                       family, qualifier,
                                       ", amount=" + amount);
  }

  // ---------------------- //
  // Package private stuff. //
  // ---------------------- //

  /**
   * Changes whether or not this atomic increment should use the WAL.
   * @param durable {@code true} to use the WAL, {@code false} otherwise.
   */
  void setDurable(final boolean durable) {
    this.durable = durable;
  }

  boolean isDurable() {
      return this.durable;
  }

}
