package org.voltdb.lrucache.sdk;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.lrucache.client.LRUCacheClient;
import org.voltdb.types.TimestampType;

public class DataLoader {
	
	Client client = null;
	
	

	
	public  void connectVoltDB() throws Exception {
		ClientConfig config = null;
		try {
			config = new ClientConfig(); // "admin", "idontknow");
			config.setMaxOutstandingTxns(20000);
			config.setMaxTransactionsPerSecond(200000);
			config.setTopologyChangeAware(true);
			config.setReconnectOnConnectionLoss(true);
			
			client = new LRUCacheClient(config);

			client.createConnection("127.0.0.1");

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("VoltDB connection failed.." + e.getMessage(), e);
		}


	}

	/**
	 * @param d
	 * @param i
	 * @param cb 
	 * @throws IOException
	 * @throws NoConnectionsException
	 * @throws ProcCallException
	 */
	private static void upsertRow(DataLoader d, int i, int generation, ProcedureCallback cb) throws IOException, NoConnectionsException, ProcCallException {
		long s_id = i;
		String sub_nbr = i + " some text";
		byte f_tinyint = (byte) (i % 100);
		short f_smallint = (short) (i % 100);
		int f_integer = generation;
		int f_bigint = i;
		float f_float = i;
		BigDecimal f_decimal = new BigDecimal(i);
		String f_geography = null;
		String f_geography_point = null;
		String f_varchar = "some text " + i;
		byte[] f_varbinary = sub_nbr.getBytes();
		TimestampType last_use_date = new TimestampType(new Date(System.currentTimeMillis()));			
		
		d.client.callProcedure(cb,"subscriber.UPSERT", s_id, sub_nbr, f_tinyint, f_smallint, f_integer, f_bigint, f_float, f_decimal
				,f_geography, f_geography_point, f_varchar, f_varbinary, last_use_date);
	}
	
	
	public static void main(String[] args) {
		
		
		DataLoader d = new DataLoader();
		ChattyCallback cb = new ChattyCallback();
		try {
			d.connectVoltDB();
			
			int generation = 0;
			int start = 1;
			int end = 30000000;
			
			for (int i = start; i <= end; i++) {
				
				upsertRow(d, i,generation,cb);
				
				if (i %100000 == 0) {
					System.out.println(i + " rows");
				}
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

}
