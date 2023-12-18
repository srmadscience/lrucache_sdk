package org.voltdb.lrucache.sdk;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

public class ChattyCallback implements ProcedureCallback {
	
	int badCount = 0;

	@Override
	public void clientCallback(ClientResponse arg0) throws Exception {
		
		if (arg0.getStatusString() != null && (!arg0.getStatusString().equals("null"))) {
			System.out.println(arg0.getStatusString());
			
			badCount++;
		}
		
		if (arg0.getAppStatusString() != null && arg0.getAppStatusString().length() >0 
				 ) {
			System.out.println(arg0.getAppStatusString());
		}
		
	}

	public int getBadCount() {
		return badCount;
	}

	public void setBadCount(int badCount) {
		this.badCount = badCount;
	}

}
