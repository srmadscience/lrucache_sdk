package org.voltdb.lrucache.sdk;

import java.io.File;
import java.io.FileInputStream;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.lrucache.client.LRUCacheClient;
import org.voltdb.lrucache.client.LruFillMissingCallbackIFace;
import org.voltdb.lrucache.client.rematerialize.AbstractSqlRematerializer;
import org.voltdb.lrucache.client.rematerialize.cassandra.CassandraRematerializerImpl;
import org.voltdb.types.TimestampType;
import org.voltdb.voltutil.stats.SafeHistogramCache;

public class DemoLruClient implements Runnable {

	Random m_randomness = null;
	long m_tps = 0;

	LRUCacheClient m_lru_client = null;
	SafeHistogramCache m_stats_histogram = SafeHistogramCache.getInstance();

	long m_txnCount = 0;
	long m_startTime = 0;
	int m_cacheSize = 0;
	long m_partitionCount = 8;
	int m_datasetSize = 0;
	long m_randomSeed = 0;
	int m_generation = 0;
	PrintWriter m_printwriter = null;

	ChattyCallback m_chattyCallback = new ChattyCallback();
	int[] m_session_generation = null;
	ErrorTracker m_et = new ErrorTracker();
	Date m_endTime;

	int m_lurk_secs = 10;

	SimpleDateFormat m_dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");

	UpsertTestDBIFace m_other_db = null;

	Properties m_rematConfig;
	String m_propfileName;
	File m_propFile;

	String m_classname;

	boolean m_doTickle = false;

	int m_soakpassesMins = 20;

	String[] m_voltdbHostnames = { "127.0.0.1" };

	public DemoLruClient(String propfilename, long randomSeed, Date endTime, long tps, int cacheSize, int datasetSize,
			boolean fakeData) {
		super();

		m_propfileName = propfilename;
		m_propFile = new File(m_propfileName);
		m_endTime = endTime;

		if (!m_propFile.exists() && m_propFile.canRead()) {
			System.err.println("Prop File " + propfilename + " is not usable");
			System.exit(1);
		}

		this.m_randomSeed = randomSeed;
		m_randomness = new Random(randomSeed);

		this.m_tps = tps;
		this.m_cacheSize = cacheSize;
		this.m_datasetSize = datasetSize;

		m_generation = 0;

		try {
			Properties config = new Properties();
			InputStream input = new FileInputStream(m_propFile);
			config.load(input);
			m_rematConfig = config;

			m_soakpassesMins = Integer.parseInt(config.getProperty("test.soakpasses", "20"));
			m_classname = config.getProperty("test.rematerializer");
			
			m_voltdbHostnames = getCommaDelimitedHostnameList("voltdb.hostname", config, "127.0.0.1");
			m_lru_client = connectVoltDB();
			m_partitionCount = getPartitionCount(m_lru_client);

			m_session_generation = new int[datasetSize];

			for (int i = 0; i < m_session_generation.length; i++) {
				m_session_generation[i] = m_generation;
			}

			String testDB = config.getProperty("test.backdoor");

			try {
				m_other_db = (UpsertTestDBIFace) Class.forName(testDB).newInstance();
				m_other_db.setConfig(config, m_et);

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	@Override
	public void run() {

		boolean isBroken = false;

		try {
			final int verySmallSize = 5000;
			int step = 0;

			int[] sizes = { verySmallSize, m_cacheSize, m_datasetSize };

			String startTimeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());

			String filename = "DemoLruCLient_" + getPropFilePrefix() + "_" + startTimeStamp + "_" + m_cacheSize + "_"
					+ m_partitionCount + "_" + m_datasetSize + "_" + m_tps + ".log";
			
			File logDir = new File("logs");
			logDir.mkdirs();
			File logFile = new File(logDir,filename);

			m_printwriter = new PrintWriter(logFile);
			step++;
			msg(step + ". Empty both databases.");
			msg(step + ". VoltDB");
			truncateSubscribers();

			msg(step + ". Other DB.");
			truncateSubscribersFromOtherDb();

			step++;
			msg(step + ". Load " + m_datasetSize + " rows plus min and max rows into VoltDB");

			// Create min and max rows
			upsertMinRow();
			upsertMaxRow();

			isBroken = initialLoad();

			step++;
			msg(step + ". Check generations");
			isBroken = checkGenerations();

			step++;
			msg(step + ". Flush " + m_datasetSize + " rows to other DB");
			if (!isBroken) {
				isBroken = drainSubscribersTable();

				tickleExport(m_datasetSize + 2);

			}

			if (!isBroken) {
				msg(step + ". Check VoltDb Count");
				isBroken = checkRowCountInVoltDB(0);
			}

			if (!isBroken) {
				msg(step + ". Check Other DB Count");
				isBroken = checkRowCountInOtherDB(0,m_datasetSize);
			}

			step++;
			msg(step + ". Check generations");
			if (!isBroken) {
				isBroken = checkGenerations();
			}

			step++;
			for (int i = 0; i < sizes.length; i++) {

				if (!isBroken) {
					msg(step + "." + i + ".1.Truncate all Subscribers");
					truncateSubscribers();

					msg(step + "." + i + ".2. Reset cache hit stats");
					m_lru_client.getCacheHitRatio("GetById");

					if (!isBroken) {

						msg(step + "." + i + ".3 Do a linear run at " + m_tps
								+ " with no data initially in the system and a per partition size of " + sizes[i]);
						isBroken = linearReload(sizes[i]);

					}

					if (!isBroken) {

						msg(step + ". Check generations");
						if (!isBroken) {
							isBroken = checkGenerations();
						}

						msg("Cache hit = " + m_lru_client.getCacheHitRatio("GetById"));
					}
				}

			}

			step++;
			for (int i = 0; i < sizes.length; i++) {

				if (!isBroken) {

					msg(step + "." + i + ".Do a linear run at " + m_tps
							+ " with residual data initially in the system and a per partition size of " + sizes[i]);
					isBroken = linearReload(sizes[i]);

				}

				if (!isBroken) {
					msg("Cache hit = " + m_lru_client.getCacheHitRatio("GetById"));
				}

			}

			step++;
			for (int i = 0; i < sizes.length; i++) {

				if (!isBroken) {
					msg(step + "." + i + ".1 Truncate all Subscribers");
					truncateSubscribers();

					if (!isBroken) {

						msg(step + "." + i + ".2 Do a random run at " + m_tps
								+ " with no data initially in the system and a per partition size of " + sizes[i]);
						msg("Cache hit = " + m_lru_client.getCacheHitRatio("GetById"));
						isBroken = randomReload(sizes[i]);

					}

					if (!isBroken) {
						isBroken = checkNoCacheMiss(); 
						checkAndResetTotals(m_datasetSize, true, true, false);
					}
				}

			}

			step++;
			for (int i = 0; i < sizes.length; i++) {

				if (!isBroken) {

					msg(step + "." + i + ". Do a random run at " + m_tps
							+ " with residual data initially in the system and a per partition size of " + sizes[i]);
					isBroken = randomReload(sizes[i]);

				}

				if (!isBroken) {
					msg("Cache hit = " + m_lru_client.getCacheHitRatio("GetById"));
					isBroken = checkNoCacheMiss(); //
					checkAndResetTotals(m_datasetSize, true, true, false);
				}

			}

			step++;
			for (int i = 0; i < sizes.length; i++) {

				if (!isBroken) {
					msg(step + "." + i + ".1 .Truncate all Subscribers");
					truncateSubscribers();

					if (!isBroken) {

						msg(step + "." + i + ".2 .Do a random update run at " + m_tps
								+ " with no data initially in the system and a per partition size of " + sizes[i]);
						isBroken = randomOrderQueryAndUpdate(sizes[i]);

						tickleExport(m_datasetSize + 2);

						msg(step + "." + i + ".3 Check generations");
						checkGenerations();

					}

					if (!isBroken) {
						msg("Cache hit = " + m_lru_client.getCacheHitRatio("GetById"));
						isBroken = checkNoCacheMiss();
					}

					msg(step + "." + i + ".3 Flush " + m_datasetSize + " rows to other DB");
					if (!isBroken) {
						isBroken = drainSubscribersTable();
					}

				}

			}

			step++;

			for (int z = 0; z < m_soakpassesMins; z++) {

				msg(step + " soakpass " + (z + 1) + " of " + m_soakpassesMins);

				if (m_endTime.getTime() < System.currentTimeMillis()) {
					msg(step + " soakpass out of time");
					break;
				}

				for (int i = 0; i < sizes.length; i++) {

					if (!isBroken) {

						msg(step + "." + i + ".1 Do a random order update run at " + m_tps
								+ " with residual data initially in the system and a per partition size of "
								+ sizes[i]);

						isBroken = randomOrderQueryAndUpdate(sizes[i]);

						tickleExport(m_datasetSize + 2);

						msg(step + "." + i + ".2 Check generations");
						checkGenerations();
					}

					if (!isBroken) {
						msg("Cache hit = " + m_lru_client.getCacheHitRatio("GetById"));
						isBroken = checkNoCacheMiss();
					}
				}
			}

			step++;
			msg(step + ". Flush " + m_datasetSize + " rows to other DB");
			if (!isBroken) {
				isBroken = drainSubscribersTable();

				tickleExport(m_datasetSize + 2);

			}

			step++;
			if (!isBroken) {

				msg(step + ". Remove all Subscribers");
				truncateSubscribers();
			}

			int tc = m_lru_client.getTaskCount();
			int tcLast = m_lru_client.getTaskCount();

			while (tc > 50) {
				try {
					int tcdelta = tc - tcLast;
					System.out.println(m_lru_client.getTaskCount() + " " + tcdelta);
					Thread.sleep(1000);

					tcLast = tc;
					tc = m_lru_client.getTaskCount();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			m_lru_client.shutdownExecutor();

			final int timeInSeconds = (int) ((System.currentTimeMillis() - m_startTime) / 1000);

			if (isBroken) {
				msg("TEST FAILED");
			} else {
				msg("TEST OK");
			}

			msg("Transactions = " + m_txnCount);
			msg("runtime = " + timeInSeconds);
			msg("cache size = " + m_cacheSize);
			msg("data set size = " + m_datasetSize);
			msg("partitions = " + m_partitionCount);
			msg("runtime = " + timeInSeconds);
			msg("TPS = " + m_txnCount / timeInSeconds);
			msg("Cache hit = " + m_lru_client.getCacheHitRatio("GetById"));

			msg(m_lru_client.getStatsInstance().toString());

			m_printwriter.flush();
			m_printwriter.close();

		} catch (IOException | ProcCallException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	private void upsertRow(long l, int generation) throws IOException, NoConnectionsException, ProcCallException {

		long s_id = l;
		String sub_nbr = l + " some text";
		byte f_tinyint = (byte) (l % 100);
		short f_smallint = (short) (l % 100);
		int f_integer = generation;
		long f_bigint = l;
		float f_float = l;
		BigDecimal f_decimal = new BigDecimal(l);
		String f_geography = null;
		String f_geography_point = null;
		String f_varchar = "some text " + l;
		byte[] f_varbinary = sub_nbr.getBytes();
		TimestampType last_use_date = new TimestampType(new Date(System.currentTimeMillis()));

		m_lru_client.callProcedure(m_chattyCallback, "subscriber.UPSERT", s_id, sub_nbr, f_tinyint, f_smallint,
				f_integer, f_bigint, f_float, f_decimal, f_geography, f_geography_point, f_varchar, f_varbinary, null,
				last_use_date);
	}

	private void upsertMinRow() throws IOException, NoConnectionsException, ProcCallException {

		msg("Creating min row");

		long s_id = Long.MIN_VALUE + 1;
		String sub_nbr = Long.MIN_VALUE + " some text";
		byte f_tinyint = Byte.MIN_VALUE + 1;
		short f_smallint = Short.MIN_VALUE + 1;
		int f_integer = Integer.MIN_VALUE + 1;
		long f_bigint = Long.MIN_VALUE + 1;
		float f_float = Float.MIN_VALUE;
		BigDecimal f_decimal = new BigDecimal(Long.MIN_VALUE);
		String f_geography = null;
		String f_geography_point = null;
		String f_varchar = "some text ";
		byte[] f_varbinary = sub_nbr.getBytes();
		TimestampType last_use_date = new TimestampType(new Date(System.currentTimeMillis()));

		m_lru_client.callProcedure(m_chattyCallback, "subscriber.UPSERT", s_id, sub_nbr, f_tinyint, f_smallint,
				f_integer, f_bigint, f_float, f_decimal, f_geography, f_geography_point, f_varchar, f_varbinary, null,
				last_use_date);
		try {
			m_lru_client.drain();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void upsertMaxRow() throws IOException, NoConnectionsException, ProcCallException {

		msg("Creating max row");

		long s_id = Long.MAX_VALUE;
		String sub_nbr = Long.MAX_VALUE + " some text";
		byte f_tinyint = Byte.MAX_VALUE;
		short f_smallint = Short.MAX_VALUE;
		int f_integer = Integer.MAX_VALUE;
		long f_bigint = Long.MAX_VALUE;
		float f_float = Float.MAX_VALUE;
		BigDecimal f_decimal = new BigDecimal(Long.MAX_VALUE);
		String f_geography = null;
		String f_geography_point = null;
		String f_varchar = "some text ";
		byte[] f_varbinary = sub_nbr.getBytes();
		TimestampType last_use_date = new TimestampType(new Date(System.currentTimeMillis()));

		m_lru_client.callProcedure(m_chattyCallback, "subscriber.UPSERT", s_id, sub_nbr, f_tinyint, f_smallint,
				f_integer, f_bigint, f_float, f_decimal, f_geography, f_geography_point, f_varchar, f_varbinary, null,
				last_use_date);
		try {
			m_lru_client.drain();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}


	private boolean checkRowCountInOtherDB(int i, int m_datasetSize2) {
		
		boolean broken = false;
		
		long howMany = m_other_db.getRowCount(0, m_datasetSize);
		if (howMany != (m_datasetSize)) {
			msg("Not seeing " + m_datasetSize + " rows in other DB - got " + howMany);
			broken = true;
		} else {
			broken = false;
		}
		
		return broken;
		
	}

	private boolean checkRowCountInVoltDB(long expectedCount) {
		
		boolean broken = true;

		long howMany;
		howMany = getRecordCount();

		msg("Howmany/error/hit/miss/mismatch = " + howMany + "/" + m_et.getErrorCount() + "/" + m_et.getHitCount() + "/"
				+ m_et.getMissCount() + "/" + m_et.getMismatchCount());

		
		if (expectedCount == howMany) {
			broken = false;
		}
		
		return broken;
	}

	private void tickleExport(int minValue) {

		if (m_doTickle) {

			long offset = 100000000;

			for (int j = 0; j < 5; j++) {
				msg("tickling export system - pass " + j);

				for (int i = 0; i < 24; i++) {
					try {
						upsertRow(offset++, i * -1);

					} catch (IOException | ProcCallException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}

				drainSubscribersTable();
				//
			}
		}
	}

	private String getPropFilePrefix() {

		String prefix = m_propFile.getName().substring(0, m_propFile.getName().indexOf('.'));
		return prefix;
	}

	private void truncateSubscribersFromOtherDb() {
		m_other_db.truncate();

	}

	private boolean checkNoCacheMiss() {

		if (m_et.getMissCount() > 0) {

			msg("ERROR miss count non zero: " + m_et.getMissCount());
			return true;

		}

		if (m_et.getMismatchCount() > 0) {

			msg("ERROR mismatch count non zero: " + m_et.getMismatchCount());
			return true;

		}

		return false;

	}

	/**
	 * @throws IOException
	 * @throws NoConnectionsException
	 * @throws ProcCallException
	 */
	private boolean initialLoad() throws IOException, NoConnectionsException, ProcCallException {
		boolean isBroken = false;

		// Create data
		msg("Loading " + m_session_generation.length + " subscribers");
		
		m_startTime = System.currentTimeMillis();
		for (int i = 0; i < m_session_generation.length; i++) {
			m_session_generation[i] = m_generation;
			upsertRow(i, m_session_generation[i]);
			if (i > 1 && i % 1000000 == 0) {
				msg("Loaded " + i + " subscribers");
			}
		}

		drainClient();
		float timeToUpsertInSeconds = (int) ((System.currentTimeMillis() - m_startTime) / 1000);
		msg("Loaded " + m_session_generation.length + " subscribers in " + timeToUpsertInSeconds + " seconds");

		long howMany = getRecordCount();
		if (howMany != m_session_generation.length + 2) {
			msg("ERROR row count differs " + howMany + ", " + (m_session_generation.length + 2));
			isBroken = true;
		}

		return isBroken;
	}

	/**
	 * 
	 */
	private boolean drainSubscribersTable() {

		final long initialRecordCount = getRecordCount();
		final int lurkSecs = 60;

		drainClient();

		long tpPerMs = m_tps / 1000;

		long txns = 0;
		msg("tpms = " + tpPerMs + ", records = " + initialRecordCount + " waiting for " + lurkSecs
				+ " seconds so data is old enough to purge");

		// Allow data to become old enough to purge
		lurk(lurkSecs);

		msg("Starting to drain");

		boolean isBroken = false;

		float timeToUpsertInSeconds;
		// Flush data to downstream system at max speed
		m_startTime = System.currentTimeMillis();
		int flushCallCount = 0;
		int i = 0;
		int maxMins = 60;
		long endTime = System.currentTimeMillis() + (maxMins * 60 * 1000);
		try {
			while (endTime > System.currentTimeMillis()) {

				long millisecs = System.currentTimeMillis();
				txns = 0;

				while (millisecs == System.currentTimeMillis()) {
					if (txns++ <= tpPerMs) {
						int passesPerCall = 10;
						// We know that all our records have positive id's.
						// Create a storm of negative ids that will flush
						// 'good' data out, as we ask for 0 records to remain.
						launchPurgeTransaction(i, 0, passesPerCall);
						i++;
						flushCallCount++;
					}
				}

				if (i % 25000 == 0) {
					long currentTotal = getRecordCount();
					msg("Rows left after " + (System.currentTimeMillis() - m_startTime) + "ms = " + currentTotal);

					if (currentTotal == 0) {
						break;
					}
				}

			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		msg("draining client");
		drainClient();
		final long firstpassRecordCount = getRecordCount();

		timeToUpsertInSeconds = (int) ((System.currentTimeMillis() - m_startTime) / 1000);
		msg(" Flushed " + (initialRecordCount - firstpassRecordCount) + "  of " + initialRecordCount
				+ " subscribers in " + timeToUpsertInSeconds + " seconds and around " + flushCallCount + " calls, "
				+ initialRecordCount + " left");

		long howMany = getRecordCount();
		if (howMany != 0) {
			msg("ERROR not all rows purged");
			isBroken = true;
		}

		return isBroken;
	}

	private void lurk(int i) {
		msg("Lurking for " + i + " seconds");

		try {
			Thread.sleep(i * 1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * 
	 */
	private boolean linearReload(int partitionCacheSize) {
		boolean isBroken = false;

		m_startTime = System.currentTimeMillis();

		for (int rc = 0; rc < m_session_generation.length; rc++) {

			launchQueryTransaction(rc, partitionCacheSize, m_session_generation[rc]);
			launchPurgeTransaction(rc, partitionCacheSize, 5);

			if (rc % 1000 == 0) {

				while (m_lru_client.getTaskCount() > 0) {
					try {
						Thread.sleep(100);
						// System.out.println("session = " + rc + " in flight =
						// " + client.getTaskCount());
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}

		}
		msg("in flight at end = " + m_lru_client.getTaskCount());

		while (m_lru_client.getTaskCount() > 0) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		drainClient();

		float timeToUpsertInSeconds = (int) ((System.currentTimeMillis() - m_startTime) / 1000);
		msg("Reloaded " + m_session_generation.length + " subscribers in " + timeToUpsertInSeconds + " seconds");

		return isBroken;
	}

	/**
	 * 
	 */
	private boolean randomReload(int partitionCacheSize) {
		boolean isBroken = false;

		boolean[] doneYet = new boolean[m_session_generation.length];
		int hits = 0;

		for (int i = 0; i < m_session_generation.length; i++) {
			doneYet[i] = false;
		}

		m_startTime = System.currentTimeMillis();

		for (int rc = 0; rc < m_session_generation.length * 10; rc++) {

			int randomId = m_randomness.nextInt(m_session_generation.length);

			if (!doneYet[randomId]) {

				hits++;

				launchQueryTransaction(randomId, partitionCacheSize, m_session_generation[randomId]);
				launchPurgeTransaction(randomId, partitionCacheSize, 5);

				if (rc % 1000 == 0) {
					// System.out.println(
					// "Reloaded " + rc + " random subscribers, in flight = " +
					// m_lru_client.getTaskCount());

					while (m_lru_client.getTaskCount() > 0) {
						try {
							Thread.sleep(100);
							// System.out.println("in flight = " +
							// client.getTaskCount());
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}

				if (hits > (m_session_generation.length * 0.9)) {
					break;
				}
			}

		}

		msg("in flight at end = " + m_lru_client.getTaskCount());

		while (m_lru_client.getTaskCount() > 0) {
			try {
				Thread.sleep(1000);
				// System.out.println("in flight = " + client.getTaskCount());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		drainClient();

		float timeToUpsertInSeconds = (int) ((System.currentTimeMillis() - m_startTime) / 1000);
		msg("Reloaded " + m_session_generation.length + " subscribers in " + timeToUpsertInSeconds + " seconds");

		return isBroken;
	}

	private boolean[] getUndoneArray() {
		boolean[] doneYet = new boolean[m_session_generation.length];

		for (int i = 0; i < m_session_generation.length; i++) {
			doneYet[i] = false;
		}

		return doneYet;
	}

	private boolean randomOrderQueryAndUpdate(int partitionCacheSize) {
		boolean isBroken = false;

		boolean[] doneYet = getUndoneArray();
		int hits = 0;

		m_startTime = System.currentTimeMillis();

		for (int rc = 0; rc < m_session_generation.length * 10; rc++) {

			int recordId = m_randomness.nextInt(m_session_generation.length);

			if (!doneYet[recordId]) {

				doneYet[recordId] = true;
				hits++;

				if (m_randomness.nextBoolean()) {

					m_session_generation[recordId] = m_session_generation[recordId] + 1;

					launchUpdateTransaction(recordId, m_session_generation[recordId]);
					launchPurgeTransaction(recordId, partitionCacheSize, 5);

				} else {

					launchQueryTransaction(recordId, partitionCacheSize, m_session_generation[recordId]);
					launchPurgeTransaction(recordId, partitionCacheSize, 5);

				}

				if (rc % 1000 == 0) {
					// System.out.println("Updated or queried " + rc + " random
					// subscribers, in flight = "
					// + m_lru_client.getTaskCount());

					while (m_lru_client.getTaskCount() > 0) {
						try {
							Thread.sleep(100);
							// System.out.println("in flight = " +
							// client.getTaskCount());
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}

				if (hits > (m_session_generation.length * 0.9)) {
					break;
				}
			}

		}

		msg("in flight at end = " + m_lru_client.getTaskCount());

		while (m_lru_client.getTaskCount() > 0) {
			try {
				Thread.sleep(1000);
				// System.out.println("in flight = " + client.getTaskCount());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		drainClient();

		float timeToUpsertInSeconds = (int) ((System.currentTimeMillis() - m_startTime) / 1000);
		msg("Updated or queried " + m_session_generation.length + " subscribers in " + timeToUpsertInSeconds
				+ " seconds");
		m_printwriter.flush();

		return isBroken;
	}

	private boolean randomChoiceQueryAndUpdate(int partitionCacheSize) {
		boolean isBroken = false;

		m_startTime = System.currentTimeMillis();

		long[] lastHitTime = new long[m_session_generation.length];

		for (int i = 0; i < lastHitTime.length; i++) {
			lastHitTime[i] = 0;
		}

		for (int rc = 0; rc < m_session_generation.length; rc++) {

			int recordId = m_randomness.nextInt(m_session_generation.length);

			if (m_randomness.nextBoolean()) {

				if (lastHitTime[recordId] + (60 * 1000) < System.currentTimeMillis()) {

					lastHitTime[recordId] = System.currentTimeMillis();
					m_session_generation[recordId] = m_session_generation[recordId] + 1;

					launchUpdateTransaction(recordId, m_session_generation[recordId]);
					launchPurgeTransaction(recordId, partitionCacheSize, 5);
				}

			} else {

				if (lastHitTime[recordId] + (60 * 1000) < System.currentTimeMillis()) {

					launchQueryTransaction(recordId, partitionCacheSize, m_session_generation[recordId]);
					launchPurgeTransaction(recordId, partitionCacheSize, 5);

				}
			}

			if (rc % 1000 == 0) {

				while (m_lru_client.getTaskCount() > 0) {
					try {
						Thread.sleep(100);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}

		}

		msg("in flight at end = " + m_lru_client.getTaskCount());

		while (m_lru_client.getTaskCount() > 0) {
			try {
				Thread.sleep(1000);
				// System.out.println("in flight = " + client.getTaskCount());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		drainClient();

		float timeToUpsertInSeconds = (int) ((System.currentTimeMillis() - m_startTime) / 1000);
		msg("Updated or queried " + m_session_generation.length + " subscribers in " + timeToUpsertInSeconds
				+ " seconds");
		m_printwriter.flush();

		return isBroken;
	}

	private boolean linearQueryAndUpdate(int partitionCacheSize) {
		boolean isBroken = false;

		m_startTime = System.currentTimeMillis();

		for (int rc = 0; rc < m_session_generation.length; rc++) {

			if (m_randomness.nextBoolean()) {

				m_session_generation[rc]++;
				launchUpdateTransaction(rc, m_session_generation[rc]);
				launchPurgeTransaction(rc, partitionCacheSize, 5);

			} else {

				launchQueryTransaction(rc, partitionCacheSize, m_session_generation[rc]);
				launchPurgeTransaction(rc, partitionCacheSize, 5);
			}

			if (rc % 1000 == 1) {

				// System.out.println(
				// "Updated or queried " + rc + " linear subscribers, in flight
				// = " + m_lru_client.getTaskCount());

				while (m_lru_client.getTaskCount() > 0) {
					try {
						Thread.sleep(100);
						// System.out.println("in flight = " +
						// client.getTaskCount());
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}

		}

		msg("in flight at end = " + m_lru_client.getTaskCount());

		while (m_lru_client.getTaskCount() > 0) {
			try {
				Thread.sleep(1000);
				msg("in flight = " + m_lru_client.getTaskCount());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		drainClient();

		float timeToUpsertInSeconds = (int) ((System.currentTimeMillis() - m_startTime) / 1000);
		msg("Updated or queried " + m_session_generation.length + " subscribers in " + timeToUpsertInSeconds
				+ " seconds");
		m_printwriter.flush();

		return isBroken;
	}

	private boolean checkGenerations() {
		boolean isBroken = false;

		m_startTime = System.currentTimeMillis();

		for (int rc = 0; rc < m_session_generation.length; rc++) {

			getVoltGeneration(rc);

	
			while (m_lru_client.getTaskCount() > 1000) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}

		try {
			m_lru_client.drain();

			while (m_lru_client.getTaskCount() > 0) {
				msg("Task count is " + m_lru_client.getTaskCount());
				Thread.sleep(1000);
			}

		} catch (NoConnectionsException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (m_et.getErrorCount() > 0 || m_et.getMismatchCount() > 0) {
			msg("Error tracker saw errors " + m_et.toString());
		}

		float timeToReconcile = (int) ((System.currentTimeMillis() - m_startTime) / 1000);
		msg("Reconciled " + m_session_generation.length + " subscribers in " + timeToReconcile + " seconds");
		m_printwriter.flush();

		return isBroken;
	}

	private boolean getVoltGeneration(int rc) {
		try {
			Object[] procArgs = { rc, m_session_generation.length };

			ProcedureCallback generationCallback = new GetByIdCallbackWithExpectedGenerationAndReconcile(rc,
					m_session_generation[rc], m_et, m_other_db);

			m_lru_client.callProcedureWithLRUCache(generationCallback, "GetByIdCheckGeneration", 1, procArgs);

		} catch (IOException e) {
			e.printStackTrace();
		}

		return true;
	}

	/**
	 * @param isBroken
	 * @return
	 */
	private boolean checkAndResetTotals(long expectedCount, boolean complain, boolean lessThanRowCountOK,
			boolean morethanBy1PctOK) {

		boolean isBroken = false;

		long howMany;
		howMany = getRecordCount();

		msg("Howmany/error/hit/miss/mismatch = " + howMany + "/" + m_et.getErrorCount() + "/" + m_et.getHitCount() + "/"
				+ m_et.getMissCount() + "/" + m_et.getMismatchCount());

		if (complain) {

			if (howMany != expectedCount) {
				if (howMany < expectedCount && lessThanRowCountOK) {
					// OK - we dont always have the right number of rows in a
					// partition

				} else if (morethanBy1PctOK && howMany > expectedCount && howMany <= (expectedCount * 1.05)) {
					// OK - we dont always have the right number of rows in a
					// partition
				} else {
					msg("ERROR row count differs " + howMany + " should be " + expectedCount);
					isBroken = true;
				}
			}

			if (m_et.getErrorCount() > 0) {
				msg("ERROR error count non zero: " + m_et.getErrorCount());
				isBroken = true;

			}

			if (m_et.getMissCount() > 0) {
				msg("ERROR miss count non zero: " + m_et.getMissCount());
				isBroken = true;

			}

			if (m_et.getMismatchCount() > 0) {
				msg("ERROR getMismatchCount > 0: " + m_et.getMismatchCount());
				isBroken = true;

			}
		}
		m_printwriter.flush();

		m_et.setErrorCount(0);
		m_et.setHitCount(0);
		m_et.setMissCount(0);
		m_et.setMismatchCount(0);

		return isBroken;
	}

	private long getRecordCount() {

		long recordCount = 0;

		try {
			ClientResponse r = m_lru_client.callProcedure("CountId");
			r.getResults()[0].advanceRow();
			recordCount = r.getResults()[0].getLong(0);
			msg(recordCount + " subscribers found in db");
			m_printwriter.flush();

		} catch (IOException | ProcCallException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return recordCount;
	}

	private void truncateSubscribers() {

		drainClient();

		long recordCount = 0;

		try {
			ClientResponse r = m_lru_client.callProcedure("TruncateSubscriber");
			r.getResults()[0].advanceRow();
			recordCount = r.getResults()[0].getLong(0);
			msg(recordCount + " subscribers removed from db");

		} catch (IOException | ProcCallException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * 
	 */
	private void drainClient() {
		try {

			while (m_lru_client.getTaskCount() > 0) {
				msg(m_lru_client.getTaskCount() + " transactions in flight");
				Thread.sleep(1000);
			}

			m_lru_client.drain();
		} catch (NoConnectionsException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void launchQueryTransaction(int id, long limit, int generation) {
		m_txnCount++;

		try {
			Object[] procArgs = { id, limit };

			ProcedureCallback generationCallback;

			if (id < 0) {
				generationCallback = new GetByIdCallback(id, m_et);
			} else {
				generationCallback = new GetByIdCallbackWithExpectedGeneration(id, generation, m_et);
			}

			m_lru_client.callProcedureWithLRUCache(generationCallback, "GetById", 1, procArgs);

		} catch (NoConnectionsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void launchPurgeTransaction(int id, long limit, int passes) {
		m_txnCount++;

		try {
			Object[] procArgs = { id, limit, passes };

			ProcedureCallback purgeCallback = new PurgeCallback(id, m_et);

			m_lru_client.callProcedureWithLRUCache(purgeCallback, "Purge", 1, procArgs);

		} catch (NoConnectionsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void launchUpdateTransaction(int id, long generation) {
		m_txnCount++;

		try {
			Object[] procArgs = { id, generation };

			ProcedureCallback ugc = new UpdateGenerationCallback(id, m_et);
			m_lru_client.callProcedureWithLRUCache(ugc, "UpdateGeneration", 1, procArgs);

		} catch (NoConnectionsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public LRUCacheClient connectVoltDB() throws Exception {
		LRUCacheClient client = null;
		ClientConfig config = null;
		try {
			config = new ClientConfig(); // "admin", "idontknow");
			config.setMaxOutstandingTxns(1000);
			config.setMaxTransactionsPerSecond(200000);
			config.setTopologyChangeAware(true);
			config.setReconnectOnConnectionLoss(true);
			String tableName = m_rematConfig.getProperty("database.tablename");
			String schema = m_rematConfig.getProperty(AbstractSqlRematerializer.DATABASE_USERNAME);
			client = new LRUCacheClient(config, m_classname, schema, tableName, m_rematConfig);

			for (int i=0; i < m_voltdbHostnames.length; i++) {
				System.out.println("Connecting to " + m_voltdbHostnames[i]);
				client.createConnection(m_voltdbHostnames[i]);
			}
			

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("VoltDB connection failed.." + e.getMessage(), e);
		}

		return client;

	}

	public static long getPartitionCount(Client c) {
		long partitionCount = 0;

		try {
			ClientResponse results = c.callProcedure("@Statistics", "PARTITIONCOUNT", 0);
			partitionCount = results.getResults()[0].fetchRow(0).getLong("PARTITION_COUNT");

		} catch (Exception e) {
			e.printStackTrace();
		}

		return partitionCount;
	}

	private void msg(String msg) {

		String timeString = m_dateFormat.format(new Date(System.currentTimeMillis()));

		m_printwriter.println(timeString + ":" + msg);
		System.out.println(timeString + ":" + msg);
	}

	public static void main(String[] args) {

		final String propfilename = args[0];
		final int CACHE_SIZE = Integer.parseInt(args[1]);
		final int DATASET_SIZE = Integer.parseInt(args[2]);
		final int TPS = Integer.parseInt(args[3]);
		final int DURATION_MINS = Integer.parseInt(args[4]);
		long RANDOM_SEED = System.currentTimeMillis();

		if (args.length == 6) {
			RANDOM_SEED = Long.parseLong(args[5]);
		}

		final long endTime = System.currentTimeMillis() + (1000 * 60 * DURATION_MINS);

		DemoLruClient c = new DemoLruClient(propfilename, RANDOM_SEED, new Date(endTime), TPS, CACHE_SIZE, DATASET_SIZE,
				true);

		c.run();

	}

	/**
	 * Turns a comma delimited list of hosts in a Properties object into a
	 * String array of hosts.
	 * 
	 * @param commaDelimitedHostnames
	 *            key in Properties that contains list of hosts
	 * @param p
	 *            Properties object
	 * @param defaultValue
	 * @return a String array of hosts
	 */
	public static String[] getCommaDelimitedHostnameList(String commaDelimitedHostnames, Properties p,
			String defaultValue) {
		String[] hostnames = {};

		if (p.containsKey(commaDelimitedHostnames)) {
			String rawValues = p.getProperty(commaDelimitedHostnames).trim();

			if (rawValues.indexOf(',') > -1) {
				hostnames = rawValues.split(",");
			} else {
				hostnames = new String[1];
				hostnames[0] = rawValues;
			}

		} else {
			hostnames = new String[1];
			hostnames[0] = defaultValue;
		}

		return hostnames;
	}
}
