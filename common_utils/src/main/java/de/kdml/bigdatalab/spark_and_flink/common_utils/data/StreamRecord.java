package de.kdml.bigdatalab.spark_and_flink.common_utils.data;

import java.io.Serializable;

/***
 * Stream record of value (i.e., ADB-S message) {@link PositionMessage} and
 * streamed time
 * 
 * @author Ehab Qadah
 * 
 *         Jan 7, 2017
 */
public class StreamRecord implements Serializable {

	private static final String VALUE_TIME_SPERATOR = "___";
	private static final long serialVersionUID = 4698870467722034731L;
	private String value;
	private long StreamedTime;

	public StreamRecord() {
	}

	public StreamRecord(String value) {

		setValue(value);
		setStreamedTime(System.currentTimeMillis());
	}

	public StreamRecord(String value, long streamedTime) {

		setValue(value);
		setStreamedTime(streamedTime);
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public long getStreamedTime() {
		return StreamedTime;
	}

	public void setStreamedTime(long streamedTime) {
		StreamedTime = streamedTime;
	}

	@Override
	public String toString() {
		return getValue() + VALUE_TIME_SPERATOR + getStreamedTime();
	}

	/**
	 * Parse a stream record from string line 
	 */
	public static StreamRecord parseData(String line) {

		String[] attributes = line.split(VALUE_TIME_SPERATOR);
		if (attributes.length == 2) {
			try {
				long time = Long.parseLong(attributes[1]);
				return new StreamRecord(attributes[0], time);
			} catch (NumberFormatException e) {
				System.out.println("invlid time " + e);
			}
		}

		return null;
	}

}
