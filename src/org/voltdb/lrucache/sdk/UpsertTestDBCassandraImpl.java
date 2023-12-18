package org.voltdb.lrucache.sdk;

import java.util.Properties;

import org.voltcore.logging.VoltLogger;
import org.voltdb.lrucache.client.rematerialize.AbstractSqlRematerializer;
import org.voltdb.seutils.wranglers.cassandra.CassandraWrangler;
import org.voltdb.upsertexport.AbstractExportMergeClient;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class UpsertTestDBCassandraImpl implements UpsertTestDBIFace {

	Properties config = null;
	ErrorTracker et = null;
	CassandraWrangler m_cw = null;
	Session m_session = null;

	/**
	 * Logger instance
	 */
	protected static final VoltLogger logger = new VoltLogger("UpsertTestDBOracleImpl");

	PreparedStatement m_psCheckgeneration = null;
	//PreparedStatement m_psCountSomeRows = null;

	public UpsertTestDBCassandraImpl() {

	}

	public void setConfig(Properties config, ErrorTracker et) {

		this.config = config;
		this.et = et;

		String hostsAsSingleString = config.getProperty(AbstractSqlRematerializer.DATABASE_HOSTNAME);
		int port = Integer.parseInt(config.getProperty(AbstractSqlRematerializer.DATABASE_PORT, "9042"));
		String username = config.getProperty(AbstractSqlRematerializer.DATABASE_USERNAME, "");
		String password = config.getProperty(AbstractSqlRematerializer.DATABASE_PASSWORD, "");

		String[] hosts = AbstractExportMergeClient.getCommaDelimitedHostnameList(hostsAsSingleString);

		m_cw = new CassandraWrangler(hosts, username, password, port, config);
		m_cw.confirmConnected();
		m_session = m_cw.getSession();

		m_psCheckgeneration = m_session.prepare("select f_integer  from  vtest.subscriber where s_id = ?");
		
		// See https://www.datastax.com/dev/blog/allow-filtering-explained-2
		
		/**
		 * You can't do range select count(*)s....
		 * 
		 * cqlsh> select count(*) from vtest.subscriber where s_id = 42;

 		count
		-------
     		1

		(1 rows)
		cqlsh> select count(*) from vtest.subscriber where s_id >= 42 and s_id <= 42;
		InvalidRequest: Error from server: code=2200 [Invalid query] message="Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING"
		cqlsh> select count(*) from vtest.subscriber where s_id >= 42 and s_id <= 42 allow filtering;
		ReadFailure: Error from server: code=1300 [Replica(s) failed to execute read] message="Operation failed - received 0 responses and 1 failures" info={'failures': 1, 'received_responses': 0, 'required_responses': 1, 'consistency': 'ONE'}
		cqlsh> 
		 */
		
		
		//m_psCountSomeRows = m_session.prepare("select count(*)  from  vtest.subscriber where s_id >= ? and s_id <= ? ALLOW FILTERING");
	}

	@Override
	public boolean truncate() {

		ResultSet r = m_session.execute("truncate table vtest.subscriber;");
		return true;
	}

	public int getGeneration(long sessionId) {
		int generation = -1;
		BoundStatement bs = m_psCheckgeneration.bind();
		bs.bind(sessionId);
		ResultSet rs = m_session.execute(bs);
		Row row = rs.one();

		try {
			Object[] theRow = CassandraWrangler.mapCassandraRowToVoltObjectArray(row);
			generation = (int)theRow[0];

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return generation;
	}

	@Override
	public long getRowCount(long start, long end) {
		long howMany = 0;
		
		for (long i=start; i <= end; i++ ) {
			
			int generation = getGeneration(i);
			
			if (generation > -1) {
				howMany++;
			}
		}
	
		return howMany;
	}

}
