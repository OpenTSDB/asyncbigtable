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
 * Deletes some data into HBase.
 *
 * <h1>A note on passing {@code byte} arrays in argument</h1>
 * None of the method that receive a {@code byte[]} in argument will copy it.
 * For more info, please refer to the documentation of {@link HBaseRpc}.
 * <h1>A note on passing {@code String}s in argument</h1>
 * All strings are assumed to use the platform's default charset.
 * <h1>A note on passing {@code timestamp}s in argument</h1>
 * Irrespective of the order in which you send RPCs, a {@code DeleteRequest}
 * that is created with a specific timestamp in argument will only delete
 * values in HBase that were previously stored with a timestamp less than
 * or equal to that of the {@code DeleteRequest} unless
 * {@link #setDeleteAtTimestampOnly} is also called, in which case only the
 * value at the specified timestamp is deleted.
 */
public final class DeleteRequest extends BatchableRpc
  implements HBaseRpc.HasTable, HBaseRpc.HasKey,
             HBaseRpc.HasFamily, HBaseRpc.HasQualifiers, HBaseRpc.IsEdit {


  /** Special value for {@link #qualifiers} when deleting a whole family.  */
  private static final byte[][] DELETE_FAMILY_MARKER =
    new byte[][] { HBaseClient.EMPTY_ARRAY };

  /** Special value for {@link #family} when deleting a whole row.  */
  static final byte[] WHOLE_ROW = new byte[0];

  private final byte[][] qualifiers;

  /** Whether to delete the value only at the specified timestamp. */
  private boolean at_timestamp_only = false;

  /**
   * Constructor to delete an entire row.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @throws IllegalArgumentException if any argument is malformed.
   */
  public DeleteRequest(final byte[] table, final byte[] key) {
    this(table, key, null, null, KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete an entire row before a specific timestamp.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param timestamp The timestamp to set on this edit.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.2
   */
  public DeleteRequest(final byte[] table, final byte[] key,
                       final long timestamp) {
    this(table, key, null, null, timestamp, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific family.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.1
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family) {
    this(table, key, family, null, KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific family before a specific timestamp.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param timestamp The timestamp to set on this edit.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.2
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family,
                       final long timestamp) {
    this(table, key, family, null, timestamp, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific cell.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to delete in that family.
   * Can be {@code null} since version 1.1.
   * @throws IllegalArgumentException if any argument is malformed.
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family,
                       final byte[] qualifier) {
      this(table, key, family,
           qualifier == null ? null : new byte[][] { qualifier },
           KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific cell before a specific timestamp.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to delete in that family.
   * Can be {@code null}, to delete the whole family.
   * @param timestamp The timestamp to set on this edit.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.2
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family,
                       final byte[] qualifier,
                       final long timestamp) {
      this(table, key, family,
           qualifier == null ? null : new byte[][] { qualifier },
           timestamp, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific number of cells in a row.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifiers The column qualifiers to delete in that family.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.1
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family,
                       final byte[][] qualifiers) {
    this(table, key, family, qualifiers,
         KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific number of cells in a row.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifiers The column qualifiers to delete in that family.
   * @param timestamp The timestamp to set on this edit.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.2
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family,
                       final byte[][] qualifiers,
                       final long timestamp) {
    this(table, key, family, qualifiers, timestamp, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific cell with an explicit row lock.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to delete in that family.
   * @param lock An explicit row lock to use with this request.
   * @throws IllegalArgumentException if any argument is malformed.
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family,
                       final byte[] qualifier,
                       final RowLock lock) {
    this(table, key, family,
         qualifier == null ? null : new byte[][] { qualifier },
         KeyValue.TIMESTAMP_NOW, lock.id());
  }

  /**
   * Constructor to delete a specific cell with an explicit row lock.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to delete in that family.
   * @param timestamp The timestamp to set on this edit.
   * @param lock An explicit row lock to use with this request.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.2
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family,
                       final byte[] qualifier,
                       final long timestamp,
                       final RowLock lock) {
    this(table, key, family,
         qualifier == null ? null : new byte[][] { qualifier },
         timestamp, lock.id());
  }

  /**
   * Constructor to delete a specific number of cells in a row.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifiers The column qualifiers to delete in that family.
   * Can be {@code null}.
   * @param lock An explicit row lock to use with this request.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.1
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family,
                       final byte[][] qualifiers,
                       final RowLock lock) {
    this(table, key, family, qualifiers, KeyValue.TIMESTAMP_NOW, lock.id());
  }

  /**
   * Constructor to delete a specific number of cells in a row with a row lock.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifiers The column qualifiers to delete in that family.
   * Can be {@code null}.
   * @param timestamp The timestamp to set on this edit.
   * @param lock An explicit row lock to use with this request.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.2
   */
  public DeleteRequest(final byte[] table,
                       final byte[] key,
                       final byte[] family,
                       final byte[][] qualifiers,
                       final long timestamp,
                       final RowLock lock) {
    this(table, key, family, qualifiers, timestamp, lock.id());
  }

  /**
   * Constructor to delete an entire row.
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @throws IllegalArgumentException if any argument is malformed.
   */
  public DeleteRequest(final String table, final String key) {
    this(table.getBytes(), key.getBytes(), null, null,
         KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific family.
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @throws IllegalArgumentException if any argument is malformed.
   * @since 1.1
   */
  public DeleteRequest(final String table,
                       final String key,
                       final String family) {
    this(table.getBytes(), key.getBytes(), family.getBytes(), null,
         KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific cell.
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to delete in that family.
   * Can be {@code null} since version 1.1.
   * @throws IllegalArgumentException if any argument is malformed.
   */
  public DeleteRequest(final String table,
                       final String key,
                       final String family,
                       final String qualifier) {
    this(table.getBytes(), key.getBytes(), family.getBytes(),
         qualifier == null ? null : new byte[][] { qualifier.getBytes() },
         KeyValue.TIMESTAMP_NOW, RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific cell with an explicit row lock.
   * @param table The table to edit.
   * @param key The key of the row to edit in that table.
   * @param family The column family to edit in that table.
   * @param qualifier The column qualifier to delete in that family.
   * Can be {@code null} since version 1.1.
   * @param lock An explicit row lock to use with this request.
   * @throws IllegalArgumentException if any argument is malformed.
   */
  public DeleteRequest(final String table,
                       final String key,
                       final String family,
                       final String qualifier,
                       final RowLock lock) {
    this(table.getBytes(), key.getBytes(), family.getBytes(),
         qualifier == null ? null : new byte[][] { qualifier.getBytes() },
         KeyValue.TIMESTAMP_NOW, lock.id());
  }

  /**
   * Constructor to delete a specific cell.
   * @param table The table to edit.
   * @param kv The specific {@link KeyValue} to delete.  Note that if this
   * {@link KeyValue} specifies a timestamp, then this specific timestamp only
   * will be deleted.
   * @since 1.2
   */
  public DeleteRequest(final byte[] table, final KeyValue kv) {
    this(table, kv.key(), kv.family(), new byte[][] { kv.qualifier() },
         kv.timestamp(), RowLock.NO_LOCK);
  }

  /**
   * Constructor to delete a specific cell with an explicit row lock.
   * @param table The table to edit.
   * @param kv The specific {@link KeyValue} to delete.  Note that if this
   * {@link KeyValue} specifies a timestamp, then this specific timestamp only
   * will be deleted.
   * @param lock An explicit row lock to use with this request.
   * @since 1.2
   */
  public DeleteRequest(final byte[] table,
                       final KeyValue kv,
                       final RowLock lock) {
    this(table, kv.key(), kv.family(), new byte[][] { kv.qualifier() },
         kv.timestamp(), lock.id());
  }

  /** Private constructor.  */
  private DeleteRequest(final byte[] table,
                        final byte[] key,
                        final byte[] family,
                        final byte[][] qualifiers,
                        final long timestamp,
                        final long lockid) {
    super(table, key, family == null ? WHOLE_ROW : family, timestamp, lockid);
    if (family != null) {
      KeyValue.checkFamily(family);
    }

    if (qualifiers != null) {
      if (family == null) {
        throw new IllegalArgumentException("You can't delete specific qualifiers"
          + " without specifying which family they belong to."
          + "  table=" + Bytes.pretty(table)
          + ", key=" + Bytes.pretty(key));
      }
      if (qualifiers.length == 0) {
        throw new IllegalArgumentException("Don't pass an empty list of"
          + " qualifiers, this would delete the entire row of table="
          + Bytes.pretty(table) + " at key " + Bytes.pretty(key));
      }
      for (final byte[] qualifier : qualifiers) {
        KeyValue.checkQualifier(qualifier);
      }
      this.qualifiers = qualifiers;
    } else {
      // No specific qualifier to delete: delete the entire family.  Not that
      // if `family == null', we'll delete the whole row anyway.
      this.qualifiers = DELETE_FAMILY_MARKER;
    }
  }

  /**
   * Deletes only the cell value with the timestamp specified in the
   * constructor.
   * <p>
   * Only applicable when qualifier(s) is also specified.
   * @since 1.5
   */
  public void setDeleteAtTimestampOnly(final boolean at_timestamp_only) {
    this.at_timestamp_only = at_timestamp_only;
  }

  /**
   * Returns whether to only delete the cell value at the timestamp.
   * @since 1.5
   */
  public boolean deleteAtTimestampOnly() {
    return at_timestamp_only;
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
  public byte[][] qualifiers() {
    return qualifiers;
  }

  public String toString() {
    return super.toStringWithQualifiers("DeleteRequest", family, qualifiers);
  }




}
