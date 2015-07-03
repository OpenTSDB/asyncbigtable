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
 * Acquires an explicit row lock.
 * <p>
 * <strong>Row locks are no longer supported as of HBase 0.96.</strong>
 * While they can still be used with earlier HBase versions,
 * attempting to use them with HBase 0.95 and up will cause a
 * {@link UnsupportedOperationException} to be thrown.
 * <p>
 * For a description of what row locks are, see {@link RowLock}.
 *
 * <h1>A note on passing {@code byte} arrays in argument</h1>
 * None of the method that receive a {@code byte[]} in argument will copy it.
 * For more info, please refer to the documentation of {@link HBaseRpc}.
 * <h1>A note on passing {@code String}s in argument</h1>
 * All strings are assumed to use the platform's default charset.
 */
public final class RowLockRequest extends HBaseRpc
  implements HBaseRpc.HasTable, HBaseRpc.HasKey {

  private static final byte[] LOCK_ROW = new byte[] {
    'l', 'o', 'c', 'k', 'R', 'o', 'w'
  };

  /**
   * Constructor.
   * <strong>These byte arrays will NOT be copied.</strong>
   * @param table The table containing the row to lock.
   * @param key The key of the row to lock in that table.
   */
  public RowLockRequest(final byte[] table, final byte[] key) {
    super(table, key);
  }

  /**
   * Constructor.
   * @param table The table containing the row to lock.
   * @param key The key of the row to lock in that table.
   */
  public RowLockRequest(final String table, final String key) {
    this(table.getBytes(), key.getBytes());
  }


  @Override
  public byte[] table() {
    return table;
  }

  @Override
  public byte[] key() {
    return key;
  }

}
