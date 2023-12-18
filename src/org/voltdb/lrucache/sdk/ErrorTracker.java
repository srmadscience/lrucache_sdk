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


public class ErrorTracker  {

   private int missCount = 0;
    private int hitCount = 0;
    private int errorCount = 0;
    private int mismatchCount = 0;
    

	
    public  synchronized void incMissCount() {
		missCount++;
	}
	
    public  synchronized void incHitCount() {
		hitCount++;
	}
	
    public  synchronized void incErrorCount() {
		errorCount++;
	}
	
	public synchronized int getMissCount() {
		return missCount;
	}

	public synchronized void setMissCount(int missCount) {
		this.missCount = missCount;
	}

	public synchronized int getHitCount() {
		return hitCount;
	}

	public synchronized void setHitCount(int hitCount) {
		this.hitCount = hitCount;
	}

	public synchronized int getErrorCount() {
		return errorCount;
	}

	public synchronized void setErrorCount(int errorCount) {
		this.errorCount = errorCount;
	}
	
	public synchronized void setMismatchCount(int mismatchCount) {
		this.mismatchCount = mismatchCount;
	}

	public synchronized void incMismatch() {
		mismatchCount++;
		
	}

	public synchronized int getMismatchCount() {
		return mismatchCount;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		
		return "ErrorTracker Hit/Miss/Error/Mismatch=" + hitCount+"/"
				+ missCount +"/"
				+ errorCount + "/"
				+ mismatchCount;
	}
	

}
