package org.voltdb.lrucache.sdk;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.voltcore.logging.VoltLogger;
import org.voltdb.lrucache.client.rematerialize.AbstractSqlRematerializer;
import org.voltdb.seutils.log.VoltLog;
import org.voltdb.seutils.utils.CSException;
import org.voltdb.seutils.wranglers.oracle.*;
import org.voltdb.upsertexport.AbstractExportMergeClient;

public class UpsertTestDBOracleImpl implements UpsertTestDBIFace {

	Properties config = null;
	ErrorTracker et = null;
	OracleConnectionWrangler m_cw = null;

	/**
	 * Logger instance
	 */
	protected static final VoltLogger logger = new VoltLogger("UpsertTestDBOracleImpl");

	VoltLog theLog = new VoltLog(logger);
	
	PreparedStatement m_CountPs = null;
	
	PreparedStatement m_getGenerationPs = null;
	
	public UpsertTestDBOracleImpl() {

	}

	public void setConfig(Properties config, ErrorTracker et) {

		this.config = config;
		this.et = et;
		
		String hostsAsSingleString = config.getProperty(AbstractSqlRematerializer.DATABASE_HOSTNAME);
		int port = Integer.parseInt(config.getProperty(AbstractSqlRematerializer.DATABASE_PORT,"1521"));
		String sid = config.getProperty(AbstractSqlRematerializer.DATABASE_SID);
		String username = config.getProperty(AbstractSqlRematerializer.DATABASE_USERNAME);
		String password = config.getProperty(AbstractSqlRematerializer.DATABASE_PASSWORD);
		
		String[] hosts = AbstractExportMergeClient.getCommaDelimitedHostnameList(hostsAsSingleString);

		try {
			m_cw = new OracleConnectionWrangler(hosts[0], port, sid, username, password, theLog,
					OracleConnectionWrangler.ORA);
			m_cw.confirmConnected();
			
			m_CountPs = m_cw.mrConnection.prepareStatement(
						"select count(*) from  vtest.subscriber where s_id between ? and ?");
			 
			 m_getGenerationPs = m_cw.mrConnection
						.prepareStatement("select f_integer generation from  vtest.subscriber where s_id = ?");
			 
		} catch (CSException | SQLException e) {
			theLog.error(e);
		}

	}

	@Override
	public boolean truncate() {

		try {
			m_cw.mrConnection.createStatement();

			PreparedStatement ps = m_cw.mrConnection.prepareStatement("truncate table vtest.subscriber");
			ps.execute();
			ps.close();

		} catch (SQLException e) {
			theLog.error(e);
		}
		
		return true;
	}

	@Override
	public int getGeneration(long sessionId) {
		int generation = -1;
		
		try {
			
			m_cw.mrConnection.createStatement();

			m_getGenerationPs.setLong(1, sessionId);
			ResultSet r1 = m_getGenerationPs.executeQuery();
			r1.next();
			generation = r1.getInt(1);
			r1.close();
			

		} catch (SQLException e) {
			theLog.error(e);
		}

		return generation;
	}

	@Override
	public long getRowCount(long st, long end) {

		long howMany = -1;
		try {
			

			m_CountPs.setLong(1, st);
			m_CountPs.setLong(2, end);
			ResultSet r1 = m_CountPs.executeQuery();
			r1.next();
			howMany = r1.getLong(1);
			

		} catch (SQLException e) {
			theLog.error(e);
		}

		return howMany;
	}
	
	protected void finalize() throws Throwable 
	{
		
		if (m_CountPs != null) {
			m_CountPs.close();
		}
		
		if (m_getGenerationPs != null) {
			m_getGenerationPs.close();
		}
		
		if (m_cw != null) {
			m_cw.disconnect();
			m_cw = null;
		}
		
	}

	


}
