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

import com.google.common.cache.LoadingCache;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.NavigableMap;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A fully asynchronous, thread-safe, modern HBase client.
 * <p>
 * Unlike the traditional HBase client ({@code HTable}), this client should be
 * instantiated only once.  You can use it with any number of tables at the
 * same time.  The only case where you should have multiple instances is when
 * you want to use multiple different clusters at the same time.
 * <p>
 * If you play by the rules, this client is (in theory {@code :D}) completely
 * thread-safe.  Read the documentation carefully to know what the requirements
 * are for this guarantee to apply.
 * <p>
 * This client is fully non-blocking, any blocking operation will return a
 * {@link Deferred} instance to which you can attach a {@link Callback} chain
 * that will execute when the asynchronous operation completes.
 *
 * <h1>Note regarding {@code HBaseRpc} instances passed to this class</h1>
 * Every {@link HBaseRpc} passed to a method of this class should not be
 * changed or re-used until the {@code Deferred} returned by that method
 * calls you back.  <strong>Changing or re-using any {@link HBaseRpc} for
 * an RPC in flight will lead to <em>unpredictable</em> results and voids
 * your warranty</strong>.
 *
 * <a name="#durability"></a>
 * <h1>Data Durability</h1>
 * Some methods or RPC types take a {@code durable} argument.  When an edit
 * requests to be durable, the success of the RPC guarantees that the edit is
 * safely and durably stored by HBase and won't be lost.  In case of server
 * failures, the edit won't be lost although it may become momentarily
 * unavailable.  Setting the {@code durable} argument to {@code false} makes
 * the operation complete faster (and puts a lot less strain on HBase), but
 * removes this durability guarantee.  In case of a server failure, the edit
 * may (or may not) be lost forever.  When in doubt, leave it to {@code true}
 * (or use the corresponding method that doesn't accept a {@code durable}
 * argument as it will default to {@code true}).  Setting it to {@code false}
 * is useful in cases where data-loss is acceptable, e.g. during batch imports
 * (where you can re-run the whole import in case of a failure), or when you
 * intend to do statistical analysis on the data (in which case some missing
 * data won't affect the results as long as the data loss caused by machine
 * failures preserves the distribution of your data, which depends on how
 * you're building your row keys and how you're using HBase, so be careful).
 * <p>
 * Bear in mind that this durability guarantee holds only once the RPC has
 * completed successfully.  Any edit temporarily buffered on the client side
 * or in-flight will be lost if the client itself crashes.  You can control
 * how much buffering is done by the client by using {@link #setFlushInterval}
 * and you can force-flush the buffered edits by calling {@link #flush}.  When
 * you're done using HBase, you <strong>must not</strong> just give up your
 * reference to your {@code HBaseClient}, you must shut it down gracefully by
 * calling {@link #shutdown}.  If you fail to do this, then all edits still
 * buffered by the client will be lost.
 * <p>
 * <b>NOTE</b>: This entire section assumes that you use a distributed file
 * system that provides HBase with the required durability semantics.  If
 * you use HDFS, make sure you have a version of HDFS that provides HBase
 * the necessary API and semantics to durability store its data.
 *
 * <h1>{@code throws} clauses</h1>
 * None of the asynchronous methods in this API are expected to throw an
 * exception.  But the {@link Deferred} object they return to you can carry an
 * exception that you should handle (using "errbacks", see the javadoc of
 * {@link Deferred}).  In order to be able to do proper asynchronous error
 * handling, you need to know what types of exceptions you're expected to face
 * in your errbacks.  In order to document that, the methods of this API use
 * javadoc's {@code @throws} to spell out the exception types you should
 * handle in your errback.  Asynchronous exceptions will be indicated as such
 * in the javadoc with "(deferred)".
 * <p>
 * For instance, if a method {@code foo} pretends to throw an
 * {@link UnknownScannerException} and returns a {@code Deferred<Whatever>},
 * then you should use the method like so:
 * <pre>
 *   HBaseClient client = ...;
 *   {@link Deferred}{@code <Whatever>} d = client.foo();
 *   d.addCallbacks(new {@link Callback}{@code <Whatever, SomethingElse>}() {
 *     SomethingElse call(Whatever arg) {
 *       LOG.info("Yay, RPC completed successfully!");
 *       return new SomethingElse(arg.getWhateverResult());
 *     }
 *     String toString() {
 *       return "handle foo response";
 *     }
 *   },
 *   new {@link Callback}{@code <Exception, Object>}() {
 *     Object call(Exception arg) {
 *       if (arg instanceof {@link UnknownScannerException}) {
 *         LOG.error("Oops, we used the wrong scanner?", arg);
 *         return otherAsyncOperation();  // returns a {@code Deferred<Blah>}
 *       }
 *       LOG.error("Sigh, the RPC failed and we don't know what to do", arg);
 *       return arg;  // Pass on the error to the next errback (if any);
 *     }
 *     String toString() {
 *       return "foo errback";
 *     }
 *   });
 * </pre>
 * This code calls {@code foo}, and upon successful completion transforms the
 * result from a {@code Whatever} to a {@code SomethingElse} (which will then
 * be given to the next callback in the chain, if any).  When there's a
 * failure, the errback is called instead and it attempts to handle a
 * particular type of exception by retrying the operation differently.
 */
public final class HBaseClient {
  /*
   * TODO(tsuna): Address the following.
   *
   * - Properly handle disconnects.
   *    - Attempt to reconnect a couple of times, see if it was a transient
   *      network blip.
   *    - If the -ROOT- region is unavailable when we start, we should
   *      put a watch in ZK instead of polling it every second.
   * - Handling RPC timeouts.
   * - Stats:
   *     - QPS per RPC type.
   *     - Latency histogram per RPC type (requires open-sourcing the SU Java
   *       stats classes that I wrote in a separate package).
   *     - Cache hit rate in the local META cache.
   *     - RPC errors and retries.
   *     - Typical batch size when flushing edits (is that useful?).
   * - Write unit tests and benchmarks!
   */

  private static final Logger LOG = LoggerFactory.getLogger(HBaseClient.class);

  /**
   * An empty byte array you can use.  This can be useful for instance with
   * {@link Scanner#setStartKey} and {@link Scanner#setStopKey}.
   */
  public static final byte[] EMPTY_ARRAY = new byte[0];

  /** A byte array containing a single zero byte.  */
  private static final byte[] ZERO_ARRAY = new byte[] { 0 };

  /**
   * In HBase 0.95 and up, this magic number is found in a couple places.
   * It's used in the znode that points to the .META. region, to
   * indicate that the contents of the znode is a protocol buffer.
   * It's also used in the value of the KeyValue found in the .META. table
   * that contain a {@link RegionInfo}, to indicate that the value contents
   * is a protocol buffer.
   */
  static final int PBUF_MAGIC = 1346524486;  // 4 bytes: "PBUF"

  /**
   * Timer we use to handle all our timeouts.
   * TODO(tsuna): Get it through the ctor to share it with others.
   * TODO(tsuna): Make the tick duration configurable?
   */
  private final HashedWheelTimer timer = new HashedWheelTimer(20, MILLISECONDS);

  /** Up to how many milliseconds can we buffer an edit on the client side.  */
  private volatile short flush_interval = 1000;  // ms

  /**
   * How many different counters do we want to keep in memory for buffering.
   * Each entry requires storing the table name, row key, family name and
   * column qualifier, plus 4 small objects.
   *
   * Assuming an average table name of 10 bytes, average key of 20 bytes,
   * average family name of 10 bytes and average qualifier of 8 bytes, this
   * would require 65535 * (10 + 20 + 10 + 8 + 4 * 32) / 1024 / 1024 = 11MB
   * of RAM, which isn't too excessive for a default value.  Of course this
   * might bite people with large keys or qualifiers, but then it's normal
   * to expect they'd tune this value to cater to their unusual requirements.
   */
  private volatile int increment_buffer_size = 65535;


  /**
   * Buffer for atomic increment coalescing.
   * This buffer starts out null, and remains so until the first time we need
   * to buffer an increment.  Once lazily initialized, this buffer will never
   * become null again.
   * <p>
   * We do this so that we can lazily schedule the flush timer only if we ever
   * have buffered increments.  Applications without buffered increments don't
   * need to pay any memory for the buffer or any CPU time for a useless timer.
   * @see #setupIncrementCoalescing
   */
  private volatile LoadingCache<BufferedIncrement, BufferedIncrement.Amount> increment_buffer;

  // ------------------------ //
  // Client usage statistics. //
  // ------------------------ //

  /** Number of connections created by {@link #newClient}.  */
  private final Counter num_connections_created = new Counter();

  /** How many {@code -ROOT-} lookups were made.  */
  private final Counter root_lookups = new Counter();

  /** How many {@code .META.} lookups were made (with a permit).  */
  private final Counter meta_lookups_with_permit = new Counter();

  /** How many {@code .META.} lookups were made (without a permit).  */
  private final Counter meta_lookups_wo_permit = new Counter();

  /** Number of calls to {@link #flush}.  */
  private final Counter num_flushes = new Counter();

  /** Number of NSREs handled by {@link #handleNSRE}.  */
  private final Counter num_nsres = new Counter();

  /** Number of RPCs delayed by {@link #handleNSRE}.  */
  private final Counter num_nsre_rpcs = new Counter();

  /** Number of {@link MultiAction} sent to the network.  */
  final Counter num_multi_rpcs = new Counter();

  /** Number of calls to {@link #get}.  */
  private final Counter num_gets = new Counter();

  /** Number of calls to {@link #openScanner}.  */
  private final Counter num_scanners_opened = new Counter();

  /** Number of calls to {@link #scanNextRows}.  */
  private final Counter num_scans = new Counter();

  /** Number calls to {@link #put}.  */
  private final Counter num_puts = new Counter();
  
  /** Number calls to {@link #append}.  */
  private final Counter num_appends = new Counter();

  /** Number calls to {@link #lockRow}.  */
  private final Counter num_row_locks = new Counter();

  /** Number calls to {@link #delete}.  */
  private final Counter num_deletes = new Counter();

  /** Number of {@link AtomicIncrementRequest} sent.  */
  private final Counter num_atomic_increments = new Counter();

  /** HBase client configuration used by the standard HBase drive */
  private final Configuration hbaseConfig;

 /** HBase client connection using the standard HBase drive */
  private final Connection hbaseConnection;

  private final ExecutorService executor;

  private final ConcurrentHashMap<TableName, BufferedMutator> mutators = new ConcurrentHashMap<TableName, BufferedMutator>();

  /**
   * Constructor.
   * @param quorum_spec The specification of the quorum, e.g.
   * {@code "host1,host2,host3"}.
   */
  public HBaseClient(final String quorum_spec) {
    this(quorum_spec, "/hbase");
  }

  /**
   * Constructor.
   * @param quorum_spec The specification of the quorum, e.g.
   * {@code "host1,host2,host3"}.
   * @param base_path The base path under which is the znode for the
   * -ROOT- region.
   */
  public HBaseClient(final String quorum_spec, final String base_path) {
    this(quorum_spec, base_path, Executors.newCachedThreadPool());
  }


  /**
   * Constructor for advanced users with special needs.
   * <p>
   * <strong>NOTE:</strong> Only advanced users who really know what they're
   * doing should use this constructor.  Passing an inappropriate thread
   * pool, or blocking its threads will prevent this {@code HBaseClient}
   * from working properly or lead to poor performance.
   * @param quorum_spec The specification of the quorum, e.g.
   * {@code "host1,host2,host3"}.
   * @param base_path The base path under which is the znode for the
   * -ROOT- region.
   * @param executor The executor from which to obtain threads for NIO
   * operations.  It is <strong>strongly</strong> encouraged to use a
   * {@link Executors#newCachedThreadPool} or something equivalent unless
   * you're sure to understand how Netty creates and uses threads.
   * Using a fixed-size thread pool will not work the way you expect.
   * <p>
   * Note that calling {@link #shutdown} on this client will <b>NOT</b>
   * shut down the executor.
   * @see NioClientSocketChannelFactory
   * @since 1.2
   */
  public HBaseClient(final String quorum_spec, final String base_path,
                     final ExecutorService executor) {

      this.executor = executor;
      this.hbaseConfig = HBaseConfiguration.create();
      LOG.info("HBase API: Connecting with config: {}", this.hbaseConfig);
      try {
          this.hbaseConnection = ConnectionFactory.createConnection(hbaseConfig);
      } catch (IOException e) {
          throw new NonRecoverableException("Failed to create conection with config: " + hbaseConfig, e);
      }
  }



  /**
   * Constructor for advanced users with special needs.
   * <p>
   * Most users don't need to use this constructor.
   * @param quorum_spec The specification of the quorum, e.g.
   * {@code "host1,host2,host3"}.
   * @param base_path The base path under which is the znode for the
   * -ROOT- region.
   * @param channel_factory A custom factory to use to create sockets.
   * <p>
   * Note that calling {@link #shutdown} on this client will also cause the
   * shutdown and release of the factory and its underlying thread pool.
   * @since 1.2
   */


  /**
   * Returns a snapshot of usage statistics for this client.
   * @since 1.3
   */
  public ClientStats stats() {
    final LoadingCache<BufferedIncrement, BufferedIncrement.Amount> cache =
      increment_buffer;
    return new ClientStats(
      num_connections_created.get(),
      root_lookups.get(),
      meta_lookups_with_permit.get(),
      meta_lookups_wo_permit.get(),
      num_flushes.get(),
      num_nsres.get(),
      num_nsre_rpcs.get(),
      num_multi_rpcs.get(),
      num_gets.get(),
      num_scanners_opened.get(),
      num_scans.get(),
      num_puts.get(),
      num_appends.get(),
      num_row_locks.get(),
      num_deletes.get(),
      num_atomic_increments.get(),
      cache != null ? cache.stats() : BufferedIncrement.ZERO_STATS
    );
  }

  /**
   * Flushes to HBase any buffered client-side write operation.
   * <p>
   * @return A {@link Deferred}, whose callback chain will be invoked when
   * everything that was buffered at the time of the call has been flushed.
   * <p>
   * Note that this doesn't guarantee that <b>ALL</b> outstanding RPCs have
   * completed.  This doesn't introduce any sort of global sync point.  All
   * it does really is it sends any buffered RPCs to HBase.
   */
  public Deferred<Object> flush() {
      LOG.info("Flushing buffered mutations");
      final ArrayList<Deferred<Object>> d = new ArrayList<Deferred<Object>>(mutators.size());
      for (BufferedMutator bm : mutators.values()) {
          try {
              bm.flush();
              d.add(Deferred.fromResult(null));
          } catch (IOException e) {
              LOG.error("Error occurred while flushing buffer", e);
              d.add(Deferred.fromError(e));
          }
      }
//    {
//      // If some RPCs are waiting for -ROOT- to be discovered, we too must wait
//      // because some of those RPCs could be edits that we must wait on.
//      final Deferred<Object> d = null; // zkclient.getDeferredRootIfBeingLookedUp();
//      if (d != null) {
//        LOG.debug("Flush needs to wait on {} to come back",
//                  has_root ? "-ROOT-" : ".META.");
//        final class RetryFlush implements Callback<Object, Object> {
//          public Object call(final Object arg) {
//            LOG.debug("Flush retrying after {} came back",
//                      has_root ? "-ROOT-" : ".META.");
//            return flush();
//          }
//          public String toString() {
//            return "retry flush";
//          }
//        }
//        return d.addBoth(new RetryFlush());
//      }
//    }

    num_flushes.increment();
//    final boolean need_sync;
//    {
//      final LoadingCache<BufferedIncrement, BufferedIncrement.Amount> buf =
//        increment_buffer;  // Single volatile-read.
//      if (buf != null && !buf.asMap().isEmpty()) {
//        flushBufferedIncrements(buf);
//        need_sync = true;
//      } else {
//        need_sync = false;
//      }
//    }
//    final ArrayList<Deferred<Object>> d =
//      new ArrayList<Deferred<Object>>(client2regions.size()
//                                      + got_nsre.size() * 8);
//    // Bear in mind that we're traversing a ConcurrentHashMap, so we may get
//    // clients that have been removed from the map since we started iterating.
//    for (final RegionClient client : client2regions.keySet()) {
//      d.add(need_sync ? client.sync() : client.flush());
//    }
//    for (final ArrayList<HBaseRpc> nsred : got_nsre.values()) {
//      synchronized (nsred) {
//        for (final HBaseRpc rpc : nsred) {
//          if (rpc instanceof HBaseRpc.IsEdit) {
//            d.add(rpc.getDeferred());
//          }
//        }
//      }
//    }
    @SuppressWarnings("unchecked")
    final Deferred<Object> flushed = (Deferred) Deferred.group(d);
    return flushed;
  }

  /**
   * Sets the maximum time (in milliseconds) for which edits can be buffered.
   * <p>
   * This interval will be honored on a "best-effort" basis.  Edits can be
   * buffered for longer than that due to GC pauses, the resolution of the
   * underlying timer, thread scheduling at the OS level (particularly if the
   * OS is overloaded with concurrent requests for CPU time), any low-level
   * buffering in the TCP/IP stack of the OS, etc.
   * <p>
   * Setting a longer interval allows the code to batch requests more
   * efficiently but puts you at risk of greater data loss if the JVM
   * or machine was to fail.  It also entails that some edits will not
   * reach HBase until a longer period of time, which can be troublesome
   * if you have other applications that need to read the "latest" changes.
   * <p>
   * Setting this interval to 0 disables this feature.
   * <p>
   * The change is guaranteed to take effect at most after a full interval
   * has elapsed, <i>using the previous interval</i> (which is returned).
   * @param flush_interval A positive time interval in milliseconds.
   * @return The previous flush interval.
   * @throws IllegalArgumentException if {@code flush_interval < 0}.
   */
  public short setFlushInterval(final short flush_interval) {
    // Note: if we have buffered increments, they'll pick up the new flush
    // interval next time the current timer fires.
    if (flush_interval < 0) {
      throw new IllegalArgumentException("Negative: " + flush_interval);
    }
    final short prev = this.flush_interval;
    this.flush_interval = flush_interval;
    return prev;
  }

  /**
   * Changes the size of the increment buffer.
   * <p>
   * <b>NOTE:</b> because there is no way to resize the existing buffer,
   * this method will flush the existing buffer and create a new one.
   * This side effect might be unexpected but is unfortunately required.
   * <p>
   * This determines the maximum number of counters this client will keep
   * in-memory to allow increment coalescing through
   * {@link #bufferAtomicIncrement}.
   * <p>
   * The greater this number, the more memory will be used to buffer
   * increments, and the more efficient increment coalescing can be
   * if you have a high-throughput application with a large working
   * set of counters.
   * <p>
   * If your application has excessively large keys or qualifiers, you might
   * consider using a lower number in order to reduce memory usage.
   * @param increment_buffer_size The new size of the buffer.
   * @return The previous size of the buffer.
   * @throws IllegalArgumentException if {@code increment_buffer_size < 0}.
   * @since 1.3
   */
  public int setIncrementBufferSize(final int increment_buffer_size) {
    if (increment_buffer_size < 0) {
      throw new IllegalArgumentException("Negative: " + increment_buffer_size);
    }
    final int current = this.increment_buffer_size;
    if (current == increment_buffer_size) {
      return current;
    }
    this.increment_buffer_size = increment_buffer_size;
    final LoadingCache<BufferedIncrement, BufferedIncrement.Amount> prev =
      increment_buffer;  // Volatile-read.
    if (prev != null) {  // Need to resize.
      makeIncrementBuffer();  // Volatile-write.
      flushBufferedIncrements(prev);
    }
    return current;
  }

  /**
   * Returns the timer used by this client.
   * <p>
   * All timeouts, retries and other things that need to "sleep
   * asynchronously" use this timer.  This method is provided so
   * that you can also schedule your own timeouts using this timer,
   * if you wish to share this client's timer instead of creating
   * your own.
   * <p>
   * The precision of this timer is implementation-defined but is
   * guaranteed to be no greater than 20ms.
   * @since 1.2
   */
  public Timer getTimer() {
    return timer;
  }

  /**
   * Schedules a new timeout.
   * @param task The task to execute when the timer times out.
   * @param timeout_ms The timeout, in milliseconds (strictly positive).
   */
  void newTimeout(final TimerTask task, final long timeout_ms) {
    try {
      timer.newTimeout(task, timeout_ms, MILLISECONDS);
    } catch (IllegalStateException e) {
      // This can happen if the timer fires just before shutdown()
      // is called from another thread, and due to how threads get
      // scheduled we tried to call newTimeout() after timer.stop().
      LOG.warn("Failed to schedule timer."
               + "  Ignore this if we're shutting down.", e);
    }
  }

  /**
   * Returns the maximum time (in milliseconds) for which edits can be buffered.
   * <p>
   * The default value is an unspecified and implementation dependant, but is
   * guaranteed to be non-zero.
   * <p>
   * A return value of 0 indicates that edits are sent directly to HBase
   * without being buffered.
   * @see #setFlushInterval
   */
  public short getFlushInterval() {
    return flush_interval;
  }

  /**
   * Returns the capacity of the increment buffer.
   * <p>
   * Note this returns the <em>capacity</em> of the buffer, not the number of
   * items currently in it.  There is currently no API to get the current
   * number of items in it.
   * @since 1.3
   */
  public int getIncrementBufferSize() {
    return increment_buffer_size;
  }

  /**
   * Performs a graceful shutdown of this instance.
   * <p>
   * <ul>
   *   <li>{@link #flush Flushes} all buffered edits.</li>
   *   <li>Completes all outstanding requests.</li>
   *   <li>Terminates all connections.</li>
   *   <li>Releases all other resources.</li>
   * </ul>
   * <strong>Not calling this method before losing the last reference to this
   * instance may result in data loss and other unwanted side effects</strong>
   * @return A {@link Deferred}, whose callback chain will be invoked once all
   * of the above have been done.  If this callback chain doesn't fail, then
   * the clean shutdown will be successful, and all the data will be safe on
   * the HBase side (provided that you use <a href="#durability">durable</a>
   * edits).  In case of a failure (the "errback" is invoked) you may want to
   * retry the shutdown to avoid losing data, depending on the nature of the
   * failure.  TODO(tsuna): Document possible / common failure scenarios.
   */
  public Deferred<Object> shutdown() {
      // 1. Flush everything.
      flush();

      // Close all open BufferedMutator instances
      ArrayList<Deferred<Object>> d = new ArrayList<Deferred<Object>>(mutators.size() + 1);
      for (BufferedMutator bm : mutators.values()) {
          try {
              bm.close();
              d.add(Deferred.fromResult(null));
          } catch (IOException e) {
              d.add(Deferred.fromError(e));
          }
      }

      if (executor != null) {
          executor.shutdown();
      }

      // Close HBase connection
      if (hbaseConnection != null) {
          try {
              hbaseConnection.close();
              d.add(Deferred.fromResult(null));
          } catch (IOException e) {
              LOG.error("Error occurred while disconnecting from HBase", e);
              d.add(Deferred.fromError(e));
          }
      }

      @SuppressWarnings("unchecked")
      final Deferred<Object> shutdown = (Deferred) Deferred.group(d);
      return shutdown;

    // This is part of step 3.  We need to execute this in its own thread
    // because Netty gets stuck in an infinite loop if you try to shut it
    // down from within a thread of its own thread pool.  They don't want
    // to fix this so as a workaround we always shut Netty's thread pool
    // down from another thread.
//    final class ShutdownThread extends Thread {
//      ShutdownThread() {
//        super("HBaseClient@" + HBaseClient.super.hashCode() + " shutdown");
//      }
//      public void run() {
//        // This terminates the Executor.
//        channel_factory.releaseExternalResources();
//      }
//    };
//
//    // 3. Release all other resources.
//    final class ReleaseResourcesCB implements Callback<Object, Object> {
//      public Object call(final Object arg) {
//        LOG.debug("Releasing all remaining resources");
//        timer.stop();
//        new ShutdownThread().start();
//        return arg;
//      }
//      public String toString() {
//        return "release resources callback";
//      }
//    }
//
//    // 2. Terminate all connections.
//    final class DisconnectCB implements Callback<Object, Object> {
//      public Object call(final Object arg) {
//        return disconnectEverything().addCallback(new ReleaseResourcesCB());
//      }
//      public String toString() {
//        return "disconnect callback";
//      }
//    }
//
//    // If some RPCs are waiting for -ROOT- to be discovered, we too must wait
//    // because some of those RPCs could be edits that we must not lose.
//    final Deferred<Object> d = zkclient.getDeferredRootIfBeingLookedUp();
//    if (d != null) {
//      LOG.debug("Shutdown needs to wait on {} to come back",
//                has_root ? "-ROOT-" : ".META.");
//      final class RetryShutdown implements Callback<Object, Object> {
//        public Object call(final Object arg) {
//          LOG.debug("Shutdown retrying after {} came back",
//                    has_root ? "-ROOT-" : ".META.");
//          return shutdown();
//        }
//        public String toString() {
//          return "retry shutdown";
//        }
//      }
//      return d.addBoth(new RetryShutdown());
//    }

    // 1. Flush everything.
    //return flush().addCallback(new DisconnectCB());
  }

  /**
   * Closes every socket, which will also flush all internal region caches.
   */
//  private Deferred<Object> disconnectEverything() {
//    HashMap<String, RegionClient> ip2client_copy;
//
//    synchronized (ip2client) {
//      // Make a local copy so we can shutdown every Region Server client
//      // without hold the lock while we iterate over the data structure.
//      ip2client_copy = new HashMap<String, RegionClient>(ip2client);
//    }
//
//    final ArrayList<Deferred<Object>> d =
//      new ArrayList<Deferred<Object>>(ip2client_copy.values().size() + 1);
//    // Shut down all client connections, clear cache.
//    for (final RegionClient client : ip2client_copy.values()) {
//      d.add(client.shutdown());
//    }
//    if (rootregion != null && rootregion.isAlive()) {
//      // It's OK if we already did that in the loop above.
//      d.add(rootregion.shutdown());
//    }
//    ip2client_copy = null;
//
//    final int size = d.size();
//    return Deferred.group(d).addCallback(
//      new Callback<Object, ArrayList<Object>>() {
//        public Object call(final ArrayList<Object> arg) {
//          // Normally, now that we've shutdown() every client, all our caches should
//          // be empty since each shutdown() generates a DISCONNECTED event, which
//          // causes RegionClientPipeline to call removeClientFromCache().
//          HashMap<String, RegionClient> logme = null;
//          synchronized (ip2client) {
//            if (!ip2client.isEmpty()) {
//              logme = new HashMap<String, RegionClient>(ip2client);
//            }
//          }
//
//          if (logme != null) {
//            // Putting this logging statement inside the synchronized block
//            // can lead to a deadlock, since HashMap.toString() is going to
//            // call RegionClient.toString() on each entry, and this locks the
//            // client briefly.  Other parts of the code lock clients first and
//            // the ip2client HashMap second, so this can easily deadlock.
//            LOG.error("Some clients are left in the client cache and haven't"
//                      + " been cleaned up: " + logme);
//            logme = null;
//            return disconnectEverything();  // Try again.
//          }
//          zkclient.disconnectZK();
//          return arg;
//        }
//        public String toString() {
//          return "wait " + size + " RegionClient.shutdown()";
//        }
//      });
//  }

  /**
   * Ensures that a given table/family pair really exists.
   * <p>
   * It's recommended to call this method in the startup code of your
   * application if you know ahead of time which tables / families you're
   * going to need, because it'll allow you to "fail fast" if they're missing.
   * <p>
   * Both strings are assumed to use the platform's default charset.
   * @param table The name of the table you intend to use.
   * @param family The column family you intend to use in that table.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).  But you probably want to attach
   * at least an errback to this {@code Deferred} to handle failures.
   * @throws TableNotFoundException (deferred) if the table doesn't exist.
   * @throws NoSuchColumnFamilyException (deferred) if the family doesn't exist.
   */
  public Deferred<Object> ensureTableFamilyExists(final String table,
                                                  final String family) {
    return ensureTableFamilyExists(table.getBytes(), family.getBytes());
  }

  /**
   * Ensures that a given table/family pair really exists.
   * <p>
   * It's recommended to call this method in the startup code of your
   * application if you know ahead of time which tables / families you're
   * going to need, because it'll allow you to "fail fast" if they're missing.
   * <p>
   * @param table The name of the table you intend to use.
   * @param family The column family you intend to use in that table.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).  But you probably want to attach
   * at least an errback to this {@code Deferred} to handle failures.
   * @throws TableNotFoundException (deferred) if the table doesn't exist.
   * @throws NoSuchColumnFamilyException (deferred) if the family doesn't exist.
   */
  public Deferred<Object> ensureTableFamilyExists(final byte[] table,
                                                  final byte[] family) {
    LOG.info("HBase API: Checking if table [{}] and family [{}] exist",
            org.apache.hadoop.hbase.util.Bytes.toString(table),
            org.apache.hadoop.hbase.util.Bytes.toString(family));

    Admin admin = null;
    try {
      admin = hbaseConnection.getAdmin();
      if(!admin.tableExists(TableName.valueOf(table))) {
          return Deferred.fromError(new TableNotFoundException(table));
      }
    } catch (IOException e) {
      if (admin != null) {
          try {
              admin.close();
          } catch (Exception e1) {

          }
      }
    }

    if (family != EMPTY_ARRAY) {
        Table t = null;

        try {
            t = hbaseConnection.getTable(TableName.valueOf(table));
            HColumnDescriptor[] descriptors = t.getTableDescriptor().getColumnFamilies();
            for (HColumnDescriptor descriptor : descriptors) {
                if (org.apache.hadoop.hbase.util.Bytes.compareTo(descriptor.getName(), family) == 0) {
                    return Deferred.fromResult(null);
                }
            }
            return Deferred.fromError(new NoSuchColumnFamilyException(org.apache.hadoop.hbase.util.Bytes.toString(family), null));
        } catch (IOException e) {
            return Deferred.fromError(e);
        } finally {
            try {
                if (t != null) {
                    t.close();
                }
            } catch (Exception e) {
                return Deferred.fromError(e);
            }
        }
    }

    return Deferred.fromResult(null);
  }

  /**
   * Ensures that a given table really exists.
   * <p>
   * It's recommended to call this method in the startup code of your
   * application if you know ahead of time which tables / families you're
   * going to need, because it'll allow you to "fail fast" if they're missing.
   * <p>
   * @param table The name of the table you intend to use.
   * The string is assumed to use the platform's default charset.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).  But you probably want to attach
   * at least an errback to this {@code Deferred} to handle failures.
   * @throws TableNotFoundException (deferred) if the table doesn't exist.
   */
  public Deferred<Object> ensureTableExists(final String table) {
    return ensureTableFamilyExists(table.getBytes(), EMPTY_ARRAY);
  }

  /**
   * Ensures that a given table really exists.
   * <p>
   * It's recommended to call this method in the startup code of your
   * application if you know ahead of time which tables / families you're
   * going to need, because it'll allow you to "fail fast" if they're missing.
   * <p>
   * @param table The name of the table you intend to use.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).  But you probably want to attach
   * at least an errback to this {@code Deferred} to handle failures.
   * @throws TableNotFoundException (deferred) if the table doesn't exist.
   */
  public Deferred<Object> ensureTableExists(final byte[] table) {
    return ensureTableFamilyExists(table, EMPTY_ARRAY);
  }

  /**
   * Retrieves data from HBase.
   * @param request The {@code get} request.
   * @return A deferred list of key-values that matched the get request.
   */
  public Deferred<ArrayList<KeyValue>> get(final GetRequest request) {
    num_gets.increment();

    long start = System.currentTimeMillis();
    Table table = null;
    try {
      table = hbaseConnection.getTable(TableName.valueOf(request.table()));
      Get get = new Get(request.key());

      if (request.qualifiers() != null) {
          for (byte[] qualifier : request.qualifiers()) {
              get.addColumn(request.family(), qualifier);
          }
      }

      Result result = table.get(get);
      ArrayList<KeyValue> keyValueList = new ArrayList<KeyValue>(result.size());

      if (!result.isEmpty()) {
          for (NavigableMap.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyEntry : result.getMap().entrySet()) {
              byte[] family = familyEntry.getKey();
              for (NavigableMap.Entry<byte[], NavigableMap<Long, byte[]>> qualifierEntry : familyEntry.getValue().entrySet()) {
                  byte[] qualifier = qualifierEntry.getKey();
                  long ts = qualifierEntry.getValue().firstKey();
                  byte[] value = qualifierEntry.getValue().get(ts);

                  KeyValue kv = new KeyValue(result.getRow(), family,
                          qualifier, ts, value);
                  keyValueList.add(kv);
              }
          }
      }
      long end = System.currentTimeMillis();

      LOG.debug("Retrieved data for {}: {} in {}ms", request, keyValueList, end-start);
          return Deferred.fromResult(keyValueList);
      } catch (IOException e) {
          return Deferred.fromError(e);
      } finally {
          if (table != null) {
              try {
                  table.close();
              } catch (IOException e) {
                  LOG.error("Failed to close table {}", table, e);
                  //   return Deferred.fromError(e);
              }
          }
      }
  }

  /**
   * Creates a new {@link Scanner} for a particular table.
   * @param table The name of the table you intend to scan.
   * @return A new scanner for this table.
   */
  public Scanner newScanner(final byte[] table) {
    return new Scanner(this, table);
  }

  /**
   * Creates a new {@link Scanner} for a particular table.
   * @param table The name of the table you intend to scan.
   * The string is assumed to use the platform's default charset.
   * @return A new scanner for this table.
   */
  public Scanner newScanner(final String table) {
    return new Scanner(this, table.getBytes());
  }

  /**
   * Package-private access point for {@link Scanner}s to open themselves.
   * @param scanner The scanner to open.
   * @return A deferred scanner ID (long) if HBase 0.94 and before, or a
   * deferred {@link Scanner.Response} if HBase 0.95 and up.
   */
  Deferred<Object> openScanner(final Scanner scanner) {
      num_scanners_opened.increment();

      LOG.info("HBase API: Scanning table with {}", scanner.toString());
      Table table = null;
      try {
          table = hbaseConnection.getTable(TableName.valueOf(scanner.table()));
          ResultScanner result = table.getScanner(scanner.getHbaseScan());
          scanner.setHbaseResultScanner(result);
          scanner.setHbaseTable(table);

          return Deferred.fromResult(new Object());
      } catch (IOException e) {
          if (table != null) {
              try {
                  table.close();
              } catch (Exception e1) {}
          }

          return Deferred.fromError(e);
      }
  }

  /** Singleton callback to handle responses of "openScanner" RPCs.  */
//  private static final Callback<Object, Object> scanner_opened =
//    new Callback<Object, Object>() {
//      public Object call(final Object response) {
//        if (response instanceof Scanner.Response) {  // HBase 0.95 and up
//          return (Scanner.Response) response;
//        } else if (response instanceof Long) {
//          // HBase 0.94 and before: we expect just a long (the scanner ID).
//          return (Long) response;
//        } else {
//          throw new InvalidResponseException(Long.class, response);
//        }
//      }
//      public String toString() {
//        return "type openScanner response";
//      }
//    };

  /**
   * Returns the client currently known to hose the given region, or NULL.
   */
//  private RegionClient clientFor(final RegionInfo region) {
//    if (region == null) {
//      return null;
//    } else if (region == META_REGION || Bytes.equals(region.table(), ROOT)) {
//      // HBase 0.95+: META_REGION (which is 0.95 specific) is our root.
//      // HBase 0.94 and earlier: if we're looking for -ROOT-, stop here.
//      return rootregion;
//    }
//    return region2client.get(region);
//  }


  /**
   * Package-private access point for {@link Scanner}s to close themselves.
   * @param scanner The scanner to close.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}.
   */
  Deferred<Object> closeScanner(final Scanner scanner) {
    LOG.info("HBase API: Closing scanner {}", scanner);

    try {
      if (scanner.getHbaseResultScanner() != null) {
          scanner.getHbaseResultScanner().close();
      } else {
          LOG.warn("Cannot close " + scanner + " properly, no result scanner open");
      }
      return Deferred.fromResult(null);
    } catch (Exception e) {
      return Deferred.fromError(e);
    } finally {
        scanner.setHbaseResultScanner(null);
        try {
            if (scanner.getHbaseTable() != null) {
                scanner.getHbaseTable().close();
            } else {
                LOG.warn("Cannot close " + scanner + " properly, no table open");
            }
        } catch (Exception e) {
          return Deferred.fromError(e);
        } finally {
            scanner.setHbaseTable(null);
        }
  }
//    final RegionInfo region = scanner.currentRegion();
//    final RegionClient client = clientFor(region);
//    if (client == null) {
//      // Oops, we no longer know anything about this client or region.  Our
//      // cache was probably invalidated while the client was scanning.  So
//      // we can't close this scanner properly.
//      LOG.warn("Cannot close " + scanner + " properly, no connection open for "
//               + Bytes.pretty(region == null ? null : region.name()));
//      return Deferred.fromResult(null);
//    }
//    final HBaseRpc close_request = scanner.getCloseRequest();
//    final Deferred<Object> d = close_request.getDeferred();
//    client.sendRpc(close_request);
//    return d;
  }

  /**
   * Atomically and durably increments a value in HBase.
   * <p>
   * This is equivalent to
   * {@link #atomicIncrement(AtomicIncrementRequest, boolean) atomicIncrement}
   * {@code (request, true)}
   * @param request The increment request.
   * @return The deferred {@code long} value that results from the increment.
   */
  public Deferred<Long> atomicIncrement(final AtomicIncrementRequest request) {
    num_atomic_increments.increment();

    Table table = null;
    try {
      table = hbaseConnection.getTable(TableName.valueOf(request.table()));
      long val = table.incrementColumnValue(request.key(),
              request.family(), request.qualifier(),
              request.getAmount(),
              request.isDurable() ? Durability.USE_DEFAULT : Durability.SKIP_WAL);

      LOG.info("HBase API: AtomicIncrement for {} returned {}", request, val);
      return Deferred.fromResult(val);
    } catch (IOException e) {
      return Deferred.fromError(e);
    } finally {
      if (table != null) {
          try {
              table.close();
          } catch (IOException e) {
              // return Deferred.fromError(e);
              LOG.error("Failed to close table {}", table, e);
          }
      }
    }
  }

  /**
   * Buffers a durable atomic increment for coalescing.
   * <p>
   * This increment will be held in memory up to the amount of time allowed
   * by {@link #getFlushInterval} in order to allow the client to coalesce
   * increments.
   * <p>
   * Increment coalescing can dramatically reduce the number of RPCs and write
   * load on HBase if you tend to increment multiple times the same working
   * set of counters.  This is very common in user-facing serving systems that
   * use HBase counters to keep track of user actions.
   * <p>
   * If client-side buffering is disabled ({@link #getFlushInterval} returns
   * 0) then this function has the same effect as calling
   * {@link #atomicIncrement(AtomicIncrementRequest)} directly.
   * @param request The increment request.
   * @return The deferred {@code long} value that results from the increment.
   * @since 1.3
   * @since 1.4 This method works with negative increment values.
   */
  public Deferred<Long> bufferAtomicIncrement(final AtomicIncrementRequest request) {
    final long value = request.getAmount();
    if (!BufferedIncrement.Amount.checkOverflow(value)  // Value too large.
        || flush_interval == 0) {           // Client-side buffer disabled.
      return atomicIncrement(request);
    }

    final BufferedIncrement incr =
      new BufferedIncrement(request.table(), request.key(), request.family(),
                            request.qualifier());

    do {
      BufferedIncrement.Amount amount;
      // Semi-evil: the very first time we get here, `increment_buffer' will
      // still be null (we don't initialize it in our constructor) so we catch
      // the NPE that ensues to allocate the buffer and kick off a timer to
      // regularly flush it.
      try {
        amount = increment_buffer.getUnchecked(incr);
      } catch (NullPointerException e) {
        setupIncrementCoalescing();
        amount = increment_buffer.getUnchecked(incr);
      }
      if (amount.update(value)) {
        final Deferred<Long> deferred = new Deferred<Long>();
        amount.deferred.chain(deferred);
        return deferred;
      }
      // else: Loop again to retry.
      increment_buffer.refresh(incr);
    } while (true);
  }

  /**
   * Called the first time we get a buffered increment.
   * Lazily creates the increment buffer and sets up a timer to regularly
   * flush buffered increments.
   */
  private synchronized void setupIncrementCoalescing() {
    // If multiple threads attempt to setup coalescing at the same time, the
    // first one to get here will make `increment_buffer' non-null, and thus
    // subsequent ones will return immediately.  This is important to avoid
    // creating more than one FlushBufferedIncrementsTimer below.
    if (increment_buffer != null) {
      return;
    }
    makeIncrementBuffer();  // Volatile-write.

    // Start periodic buffered increment flushes.
    final class FlushBufferedIncrementsTimer implements TimerTask {
      public void run(final Timeout timeout) {
        try {
          flushBufferedIncrements(increment_buffer);
        } finally {
          final short interval = flush_interval; // Volatile-read.
          // Even if we paused or disabled the client side buffer by calling
          // setFlushInterval(0), we will continue to schedule this timer
          // forever instead of pausing it.  Pausing it is troublesome because
          // we don't keep a reference to this timer, so we can't cancel it or
          // tell if it's running or not.  So let's just KISS and assume that
          // if we need the timer once, we'll need it forever.  If it's truly
          // not needed anymore, we'll just cause a bit of extra work to the
          // timer thread every 100ms, no big deal.
          newTimeout(this, interval > 0 ? interval : 100);
        }
      }
    }
    final short interval = flush_interval; // Volatile-read.
    // Handle the extremely unlikely yet possible racy case where:
    //   flush_interval was > 0
    //   A buffered increment came in
    //   It was the first one ever so we landed here
    //   Meanwhile setFlushInterval(0) to disable buffering
    // In which case we just flush whatever we have in 1ms.
    timer.newTimeout(new FlushBufferedIncrementsTimer(),
                     interval > 0 ? interval : 1, MILLISECONDS);
  }

  /**
   * Flushes all buffered increments.
   * @param increment_buffer The buffer to flush.
   */
  private static void flushBufferedIncrements(// JAVA Y U NO HAVE TYPEDEF? F U!
    final LoadingCache<BufferedIncrement, BufferedIncrement.Amount> increment_buffer) {
    // Calling this method to clean up before shutting down works solely
    // because `invalidateAll()' will *synchronously* remove everything.
    // The Guava documentation says "Discards all entries in the cache,
    // possibly asynchronously" but in practice the code in `LocalCache'
    // works as follows:
    //
    //   for each segment:
    //     segment.clear
    //
    // Where clearing a segment consists in:
    //
    //   lock the segment
    //   for each active entry:
    //     add entry to removal queue
    //   null out the hash table
    //   unlock the segment
    //   for each entry in removal queue:
    //     call the removal listener on that entry
    //
    // So by the time the call to `invalidateAll()' returns, every single
    // buffered increment will have been dealt with, and it is thus safe
    // to shutdown the rest of the client to let it complete all outstanding
    // operations.
    if (LOG.isDebugEnabled()) {
      LOG.debug("Flushing " + increment_buffer.size() + " buffered increments");
    }
    synchronized (increment_buffer) {
      increment_buffer.invalidateAll();
    }
  }

  /**
   * Creates the increment buffer according to current configuration.
   */
  private void makeIncrementBuffer() {
    final int size = increment_buffer_size;
    increment_buffer = BufferedIncrement.newCache(this, size);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created increment buffer of " + size + " entries");
    }
  }

//  /** Singleton callback to handle responses of incrementColumnValue RPCs.  */
//  private static final Callback<Long, Object> icv_done =
//    new Callback<Long, Object>() {
//      public Long call(final Object response) {
//        if (response instanceof Long) {
//          return (Long) response;
//        } else {
//          throw new InvalidResponseException(Long.class, response);
//        }
//      }
//      public String toString() {
//        return "type incrementColumnValue response";
//      }
//    };

  /**
   * Atomically increments a value in HBase.
   * @param request The increment request.
   * @param durable If {@code true}, the success of this RPC guarantees that
   * HBase has stored the edit in a <a href="#durability">durable</a> fashion.
   * When in doubt, use {@link #atomicIncrement(AtomicIncrementRequest)}.
   * @return The deferred {@code long} value that results from the increment.
   */
  public Deferred<Long> atomicIncrement(final AtomicIncrementRequest request,
                                        final boolean durable) {
    request.setDurable(durable);
    return atomicIncrement(request);
  }

  /**
   * Stores data in HBase.
   * <p>
   * Note that this provides no guarantee as to the order in which subsequent
   * {@code put} requests are going to be applied to the backend.  If you need
   * ordering, you must enforce it manually yourself by starting the next
   * {@code put} once the {@link Deferred} of this one completes successfully.
   * @param request The {@code put} request.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).  But you probably want to attach
   * at least an errback to this {@code Deferred} to handle failures.
   * TODO(tsuna): Document failures clients are expected to handle themselves.
   */
  public Deferred<Object> put(final PutRequest request) {
    num_puts.increment();

    long start = System.currentTimeMillis();
    try {
      Put put = new Put(request.key());

      long ts = request.timestamp();
      for (int i = 0; i < request.qualifiers().length; i++) {
          put.addColumn(request.family, request.qualifiers()[i], ts, request.values()[i]);
      }
      BufferedMutator bm = getBufferedMutator(TableName.valueOf(request.table()));
      bm.mutate(put);

      long end = System.currentTimeMillis();
//      LOG.info("Saved put {} in {}ms", request, end-start);
      return Deferred.fromResult(null);
    } catch (IOException e) {
      return Deferred.fromError(e);
    }
  }

  /**
   * Appends data to (or creates) one or more columns in BigTable.
   * <p>
   * Note that this provides no guarantee as to the order in which subsequent
   * {@code append} requests are going to be applied to the column(s).  If you 
   * need ordering, you must enforce it manually yourself by starting the next
   * {@code append} once the {@link Deferred} of this one completes successfully.
   * @param request The {@code append} request.
   * @return A deferred object that indicates the completion of the request and
   * may contain data from the column(s).
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).  But you probably want to attach
   * at least an errback to this {@code Deferred} to handle failures.
   */
  public Deferred<Object> append(final AppendRequest request) {
    num_appends.increment();
    
    try {
      final Append append = new Append(request.key);
      for (int i = 0; i < request.qualifiers().length; i++) {
        append.add(request.family, request.qualifiers()[i], request.values()[i]);
      }
      BufferedMutator bm = getBufferedMutator(TableName.valueOf(request.table()));
      bm.mutate(append);
      return Deferred.fromResult(null);
    } catch (IOException e) {
      return Deferred.fromError(e);
    }
  }
  
  /**
   * Atomic Compare-And-Set (CAS) on a single cell.
   * <p>
   * Note that edits sent through this method <b>cannot be batched</b>, and
   * won't be subject to the {@link #setFlushInterval flush interval}.  This
   * entails that write throughput will be lower with this method as edits
   * have to be sent out to the wire one by one.
   * <p>
   * This request enables you to atomically update the value of an existing
   * cell in HBase using a CAS operation.  It's like a {@link PutRequest}
   * except that you also pass an expected value.  If the last version of the
   * cell identified by your {@code PutRequest} matches the expected value,
   * HBase will atomically update it to the new value.
   * <p>
   * If the expected value is the empty byte array, HBase will atomically
   * create the cell provided that it doesn't exist already. This can be used
   * to ensure that your RPC doesn't overwrite an existing value.  Note
   * however that this trick cannot be used the other way around to delete
   * an expected value atomically.
   * @param edit The new value to write.
   * @param expected The expected value of the cell to compare against.
   * <strong>This byte array will NOT be copied.</strong>
   * @return A deferred boolean, if {@code true} the CAS succeeded, otherwise
   * the CAS failed because the value in HBase didn't match the expected value
   * of the CAS request.
   * @since 1.3
   */
  public Deferred<Boolean> compareAndSet(final PutRequest edit,
                                         final byte[] expected) {
    long ts1 = System.currentTimeMillis();

    Table table = null;
    try {
      table = hbaseConnection.getTable(TableName.valueOf(edit.table()));

      Put put = new Put(edit.key());
      long ts = edit.timestamp();
      for (int i = 0; i < edit.qualifiers().length; i++) {
        put.addColumn(edit.family, edit.qualifiers()[i], ts, edit.values()[i]);
      }

      boolean success = table.checkAndPut(edit.key(), edit.family(), edit.qualifier(),
              org.apache.hadoop.hbase.util.Bytes.compareTo(EMPTY_ARRAY, expected) == 0 ? null : expected,
              put);

      long ts2 = System.currentTimeMillis();

      LOG.debug("HBase API compareAndSet for cell: [{}], expected: [{}] returned success: {} in {}ms", edit,
              org.apache.hadoop.hbase.util.Bytes.toString(expected), success, ts2 - ts1);

      return Deferred.fromResult(success);
    } catch (IOException e) {
      return Deferred.fromError(e);
    } finally {
      if (table != null) {
          try {
              table.close();
          } catch (IOException e) {
              // return Deferred.fromError(e);
              LOG.error("Failed to close table {}", table, e);
          }
      }
    }
  }

  /**
   * Atomic Compare-And-Set (CAS) on a single cell.
   * <p>
   * Note that edits sent through this method <b>cannot be batched</b>.
   * @see #compareAndSet(PutRequest, byte[])
   * @param edit The new value to write.
   * @param expected The expected value of the cell to compare against.
   * This string is assumed to use the platform's default charset.
   * @return A deferred boolean, if {@code true} the CAS succeeded, otherwise
   * the CAS failed because the value in HBase didn't match the expected value
   * of the CAS request.
   * @since 1.3
   */
  public Deferred<Boolean> compareAndSet(final PutRequest edit,
                                         final String expected) {
    return compareAndSet(edit, expected.getBytes());
  }

  /**
   * Atomically insert a new cell in HBase.
   * <p>
   * Note that edits sent through this method <b>cannot be batched</b>.
   * <p>
   * This is equivalent to calling
   * {@link #compareAndSet(PutRequest, byte[]) compareAndSet}{@code (edit,
   * EMPTY_ARRAY)}
   * @see #compareAndSet(PutRequest, byte[])
   * @param edit The new value to insert.
   * @return A deferred boolean, {@code true} if the edit got atomically
   * inserted in HBase, {@code false} if there was already a value in the
   * given cell.
   * @since 1.3
   */
  public Deferred<Boolean> atomicCreate(final PutRequest edit) {
    return compareAndSet(edit, EMPTY_ARRAY);
  }

  /** Callback to type-check responses of {@link CompareAndSetRequest}.  */
  private static final class CompareAndSetCB implements Callback<Boolean, Object> {

    public Boolean call(final Object response) {
      if (response instanceof Boolean) {
        return (Boolean)response;
      } else {
        throw new InvalidResponseException(Boolean.class, response);
      }
    }

    public String toString() {
      return "type compareAndSet response";
    }

  }

  /** Singleton callback for responses of {@link CompareAndSetRequest}.  */
  private static final CompareAndSetCB CAS_CB = new CompareAndSetCB();

  /**
   * Acquires an explicit row lock.
   * <p>
   * For a description of what row locks are, see {@link RowLock}.
   * @param request The request specify which row to lock.
   * @return a deferred {@link RowLock}.
   * @see #unlockRow
   */
  public Deferred<RowLock> lockRow(final RowLockRequest request) {
    //num_row_locks.increment();

    return Deferred.fromError(new UnsupportedOperationException("Row locking is not supported in HBase 1.0 API"));
  }

  /**
   * Releases an explicit row lock.
   * <p>
   * For a description of what row locks are, see {@link RowLock}.
   * @param lock The lock to release.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public Deferred<Object> unlockRow(final RowLock lock) {
    return Deferred.fromError(new UnsupportedOperationException("Row locking is not supported in HBase 1.0 API"));
  }

  /**
   * Deletes data from HBase.
   * @param request The {@code delete} request.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).  But you probably want to attach
   * at least an errback to this {@code Deferred} to handle failures.
   */
  public Deferred<Object> delete(final DeleteRequest request) {
    num_deletes.increment();

      Table table = null;
      try {
          table = hbaseConnection.getTable(TableName.valueOf(request.table()));
          Delete delete = new Delete(request.key());
          long ts = request.timestamp();

          if (request.family() != null) {
              if (request.qualifiers() != null && request.qualifiers().length > 0) {
                  for (int i = 0; i < request.qualifiers().length; i++) {
                      if (request.deleteAtTimestampOnly()) {
                        delete.addColumn(request.family, request.qualifiers()[i], ts);
                      } else {
                        delete.addColumns(request.family, request.qualifiers()[i], ts);
                      }
                  }
              } else {
                  delete.addFamily(request.family, ts);
              }
          }
          table.delete(delete);

          return Deferred.fromResult(null);
      } catch (IOException e) {
          return Deferred.fromError(e);
      } finally {
          if (table != null) {
              try {
                  table.close();
              } catch (IOException e) {
                  return Deferred.fromError(e);
              }
          }
      }
  }



  // --------------- //
  // Little helpers. //
  // --------------- //

  /**
   * Gets a hostname or an IP address and returns the textual representation
   * of the IP address.
   * <p>
   * <strong>This method can block</strong> as there is no API for
   * asynchronous DNS resolution in the JDK.
   * @param host The hostname to resolve.
   * @return The IP address associated with the given hostname,
   * or {@code null} if the address couldn't be resolved.
   */
  private static String getIP(final String host) {
    final long start = System.nanoTime();
    try {
      final String ip = InetAddress.getByName(host).getHostAddress();
      final long latency = System.nanoTime() - start;
      if (latency > 500000/*ns*/ && LOG.isDebugEnabled()) {
        LOG.debug("Resolved IP of `" + host + "' to "
                  + ip + " in " + latency + "ns");
      } else if (latency >= 3000000/*ns*/) {
        LOG.warn("Slow DNS lookup!  Resolved IP of `" + host + "' to "
                 + ip + " in " + latency + "ns");
      }
      return ip;
    } catch (UnknownHostException e) {
      LOG.error("Failed to resolve the IP of `" + host + "' in "
                + (System.nanoTime() - start) + "ns");
      return null;
    }
  }

  /**
   * Parses a TCP port number from a string.
   * @param portnum The string to parse.
   * @return A strictly positive, validated port number.
   * @throws NumberFormatException if the string couldn't be parsed as an
   * integer or if the value was outside of the range allowed for TCP ports.
   */
  private static int parsePortNumber(final String portnum)
    throws NumberFormatException {
    final int port = Integer.parseInt(portnum);
    if (port <= 0 || port > 65535) {
      throw new NumberFormatException(port == 0 ? "port is zero" :
                                      (port < 0 ? "port is negative: "
                                       : "port is too large: ") + port);
    }
    return port;
  }


  private BufferedMutator getBufferedMutator(TableName table) throws IOException {
    BufferedMutator mutator = mutators.get(table);

    if (mutator == null) {
      synchronized (mutators) {

      BufferedMutator.ExceptionListener listener =
              new BufferedMutator.ExceptionListener() {
                  @Override
                  public void onException(RetriesExhaustedWithDetailsException e,
                                          BufferedMutator mutator) {
                      for (int i = 0; i < e.getNumExceptions(); i++) {
                          LOG.error("Failed to sent put: " + e.getRow(i));
                      }
                  }
              };
        BufferedMutatorParams params = new BufferedMutatorParams(table)
                .listener(listener);
          if (executor != null) {
              params = params.pool(executor);
          }

        mutator = hbaseConnection.getBufferedMutator(params);
        mutators.put(table, mutator);
      }
    }

    return mutator;
  }

}
