package org.voltdb.lrucache.sdk;

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

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.voltutil.stats.SafeHistogramCache;

public class UpdateGenerationCallback implements ProcedureCallback {

	long m_sessionId;
	ErrorTracker m_et;

	public UpdateGenerationCallback(int sessionId, ErrorTracker et) {
		
		this.m_sessionId = sessionId;
		this.m_et = et;
		
	}

	@Override
	public void clientCallback(ClientResponse response) {

		// Make sure the procedure succeeded.
		if (response.getStatus() != ClientResponse.SUCCESS) {

			System.out.println("VoltDB Asynchronous stored procedure failed. Res: " + response.getStatus() + " "
					+ response.getStatusString());
			m_et.incErrorCount();

		} else {

			if (response.getResults()[0].getRowCount() > 0) {
				m_et.incHitCount();	
	
			} else {
				if (m_sessionId >= 0) {
					
					m_et.incMissCount();
					System.out.println("UGC NOT FOUND: " + m_sessionId);
				}

			}

		}

	}

}
