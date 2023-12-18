package org.voltdb.lrucache.sdk;

import java.util.Properties;

public interface UpsertTestDBIFace {
		
	public boolean truncate();
	
	public int getGeneration(long sessionId);
	
	public void setConfig(Properties config, ErrorTracker et) ;
	
	public long getRowCount(long st, long end);

}
