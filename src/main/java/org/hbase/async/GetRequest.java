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
 * Reads something from HBase.
 *
 * <h1>A note on passing {@code byte} arrays in argument</h1>
 * None of the method that receive a {@code byte[]} in argument will copy it.
 * For more info, please refer to the documentation of {@link HBaseRpc}.
 * <h1>A note on passing {@code String}s in argument</h1>
 * All strings are assumed to use the platform's default charset.
 */
public final class GetRequest extends HBaseRpc
  implements HBaseRpc.HasTable, HBaseRpc.HasKey,
             HBaseRpc.HasFamily, HBaseRpc.HasQualifiers {

  private static final byte[] GET = new byte[] { 'g', 'e', 't' };
  static final byte[] GGET = new byte[] { 'G', 'e', 't' };  // HBase 0.95+
  private static final byte[] EXISTS =
    new byte[] { 'e', 'x', 'i', 's', 't', 's' };

  private byte[] family;     // TODO(tsuna): Handle multiple families?
  private byte[][] qualifiers;
  private long lockid = RowLock.NO_LOCK;

  /**
   * How many versions of each cell to retrieve.
   * The least significant bit is used as a flag to indicate when
   * this Get request is in fact an Exist request (i.e. like Get
   * except that instead of returning a result the RPC returns a
   * boolean indicating whether the row exists or not).
   */
  private int versions = 1 << 1;

  /** When set in the `versions' field, this is an Exist RPC. */
  private static final int EXIST_FLAG = 0x1;

  /**
   * Constructor.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   */
  public GetRequest(final byte[] table, final byte[] key) {
    super(table, key);
  }

  /**
   * Constructor.
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   * <strong>This byte array will NOT be copied.</strong>
   */
  public GetRequest(final String table, final byte[] key) {
    this(table.getBytes(), key);
  }

  /**
   * Constructor.
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   */
  public GetRequest(final String table, final String key) {
    this(table.getBytes(), key.getBytes());
  }

  /**
   * Constructor.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   * @param family The column family.
   * @since 1.5
   */
  public GetRequest(final byte[] table,
                    final byte[] key,
                    final byte[] family) {
    super(table, key);
    this.family(family);
  }

  /**
   * Constructor.
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   * @param family The column family.
   * @since 1.5
   */
  public GetRequest(final String table,
                    final String key,
                    final String family) {
    this(table, key);
    this.family(family);
  }

  /**
   * Constructor.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   * @param family The column family.
   * @param qualifier The column qualifier.
   * @since 1.5
   */
  public GetRequest(final byte[] table,
                    final byte[] key,
                    final byte[] family,
                    final byte[] qualifier) {
    super(table, key);
    this.family(family);
    this.qualifier(qualifier);
  }

  /**
   * Constructor.
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   * @param family The column family.
   * @param qualifier The column qualifier.
   * @since 1.5
   */
  public GetRequest(final String table,
                    final String key,
                    final String family,
                    final String qualifier) {
    this(table, key);
    this.family(family);
    this.qualifier(qualifier);
  }

  /**
   * Private constructor to build an "exists" RPC.
   * @param unused Unused, simply used to help the compiler find this ctor.
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   */
  private GetRequest(final float unused,
                     final byte[] table,
                     final byte[] key) {
    super(table, key);
    this.versions |= EXIST_FLAG;
  }

  /**
   * Package-private factory method to build an "exists" RPC.
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   * @return An {@link HBaseRpc} that will return a {@link Boolean}
   * indicating whether or not the given table / key exists.
   */
  static HBaseRpc exists(final byte[] table, final byte[] key) {
    return new GetRequest(0F, table, key);
  }

  /**
   * Package-private factory method to build an "exists" RPC.
   * @param table The non-empty name of the table to use.
   * @param key The row key to get in that table.
   * @param family The column family to get in the table.
   * @return An {@link HBaseRpc} that will return a {@link Boolean}
   * indicating whether or not the given table / key exists.
   */
  static HBaseRpc exists(final byte[] table,
                         final byte[] key, final byte[] family) {
    final GetRequest rpc = new GetRequest(0F, table, key);
    rpc.family(family);
    return rpc;
  }

  /** Returns true if this is actually an "Get" RPC. */
  private boolean isGetRequest() {
    return (versions & EXIST_FLAG) == 0;
  }

  /**
   * Specifies a particular column family to get.
   * @param family The column family.
   * <strong>This byte array will NOT be copied.</strong>
   * @return {@code this}, always.
   */
  public GetRequest family(final byte[] family) {
    KeyValue.checkFamily(family);
    this.family = family;
    return this;
  }

  /** Specifies a particular column family to get.  */
  public GetRequest family(final String family) {
    return family(family.getBytes());
  }

  /**
   * Specifies a particular column qualifier to get.
   * @param qualifier The column qualifier.
   * <strong>This byte array will NOT be copied.</strong>
   * @return {@code this}, always.
   */
  public GetRequest qualifier(final byte[] qualifier) {
    if (qualifier == null) {
      throw new NullPointerException("qualifier");
    }
    KeyValue.checkQualifier(qualifier);
    this.qualifiers = new byte[][] { qualifier };
    return this;
  }

  /**
   * Specifies a particular set of column qualifiers to get.
   * @param qualifiers The column qualifiers.
   * <strong>This byte array will NOT be copied.</strong>
   * @return {@code this}, always.
   * @since 1.1
   */
  public GetRequest qualifiers(final byte[][] qualifiers) {
    if (qualifiers == null) {
      throw new NullPointerException("qualifiers");
    }
    for (final byte[] qualifier : qualifiers) {
      KeyValue.checkQualifier(qualifier);
    }
    this.qualifiers = qualifiers;
    return this;
  }

  /** Specifies a particular column qualifier to get.  */
  public GetRequest qualifier(final String qualifier) {
    return qualifier(qualifier.getBytes());
  }

  /** Specifies an explicit row lock to use with this request.  */
  public GetRequest withRowLock(final RowLock lock) {
    lockid = lock.id();
    return this;
  }

  /**
   * Sets the maximum number of versions to return for each cell read.
   * <p>
   * By default only the most recent version of each cell is read.
   * If you want to get all possible versions available, pass
   * {@link Integer#MAX_VALUE} in argument.
   * @param versions A strictly positive number of versions to return.
   * @return {@code this}, always.
   * @since 1.4
   * @throws IllegalArgumentException if {@code versions <= 0}
   */
  public GetRequest maxVersions(final int versions) {
    if (versions <= 0) {
      throw new IllegalArgumentException("Need a strictly positive number: "
                                         + versions);
    }
    this.versions = (versions << 1) | (this.versions & EXIST_FLAG);
    return this;
  }

  /**
   * Returns the maximum number of versions to return for each cell scanned.
   * @return A strictly positive integer.
   * @since 1.4
   */
  public int maxVersions() {
    return versions >>> 1;
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
  public byte[][] qualifiers() {
    return qualifiers;
  }

  public String toString() {
    final String klass = isGetRequest() ? "GetRequest" : "Exists";
    return super.toStringWithQualifiers(klass, family, qualifiers);
  }

}
