package org.apache.flink.contrib.siddhi.utils;

public class EmittedTimestampTracker {
	private volatile long timestamp = Long.MIN_VALUE;

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long newTimestamp) {
		timestamp = newTimestamp;
	}
}
