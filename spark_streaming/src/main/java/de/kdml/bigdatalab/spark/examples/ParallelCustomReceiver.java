package de.kdml.bigdatalab.spark.examples;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

import com.google.common.io.Closeables;

import de.kdml.bigdatalab.spark.Configs;
import de.kdml.bigdatalab.spark.SparkConfigsUtils;
import de.kdml.bigdatalab.spark.examples.NetworkCustomReceiver.CustomStreamReceiver;
import scala.Tuple2;

/**
 * his a example of the Spark stream custom receiver 
 * Please refer to {@linkplain https://spark.apache.org/docs/2.0.2/streaming-custom-receivers.html#spark-streaming-custom-receivers}
 
 * @author Ehab Qadah 
 * 
 * Dec 10, 2016
 */
public class ParallelCustomReceiver {



	private static Configs configs = Configs.getInstance();

	public static void main(String[] args) throws Exception {

		JavaSparkContext sc = SparkConfigsUtils.getSparkContext("CustomReceiver");
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(configs.getIntProp("batchDuration")));

		// Create an input stream with the custom receiver on host1:port1 and host2:port2
		CustomStreamReceiver customStreamReceiver = new CustomStreamReceiver(configs.getStringProp("socketHost"),
				configs.getIntProp("socketPort"),configs.getStringProp("socketHost"),configs.getIntProp("socketPort")+1);

		JavaReceiverInputDStream<String> lines = ssc.receiverStream(customStreamReceiver);

		
		JavaPairDStream<String, Integer> wordCounts = lines.flatMapToPair(line -> {

			List<Tuple2<String, Integer>> tuples = new ArrayList<>();
			// create list of tuples of words and their counts
			for (String word : line.split(" ")) {

				tuples.add(new Tuple2<>(word, 1));
			}
			return tuples.iterator();

		}).reduceByKey((i1, i2) -> {
			// Aggregate the word counts
			return i1 + i2;
		});

		wordCounts.print();
		ssc.start();
		ssc.awaitTermination();
	}

	/***
	 * Receiver that receives data over a socket
	 * 
	 * @author Ehab Qadah
	 * 
	 *         Dec 10, 2016
	 */
	public static class CustomStreamReceiver extends Receiver<String> {

		private static final long serialVersionUID = 1L;
		private String host = null, host2 = null;
		private int port = -1, port2 = -1;

		public CustomStreamReceiver(String host, int port, String host2, int port2) {
			super(StorageLevel.MEMORY_AND_DISK_2());
			this.host = host;
			this.port = port;
			this.host2 = host2;
			this.port2 = port2;
		}

		public void onStart() {
			// Start the thread that receives data over a connection
			new Thread() {
				@Override
				public void run() {
					receive(CustomStreamReceiver.this.host, CustomStreamReceiver.this.port);
					
				}
			}.start();
		
		
		new Thread() {
			@Override
			public void run() {
				receive(CustomStreamReceiver.this.host2, CustomStreamReceiver.this.port2);
				
			}
		}.start();
	}

		public void onStop() {
			// There is nothing much to do as the thread calling receive()
			// is designed to stop by itself isStopped() returns false
		}

		/**
		 * Create a socket connection and receive data until receiver is stopped
		 */
		private void receive(String host, int port) {
			try {
				Socket socket = null;
				BufferedReader reader = null;
				String userInput = null;
				try {
					// connect to the server
					socket = new Socket(host, port);
					reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
					// Until stopped or connection broken continue reading
					while (!isStopped() && (userInput = reader.readLine()) != null) {
						System.out.println(" Received data '" + userInput + "' from " + host + ":" + port);
						
						store(userInput);
					}
				} finally {
					Closeables.close(reader, /* swallowIOException = */ true);
					Closeables.close(socket, /* swallowIOException = */ true);
				}
				// Restart in an attempt to connect again when server is active
				// again
				restart("Trying to connect again");
			} catch (ConnectException ce) {
				// restart if could not connect to server
				restart("Could not connect", ce);
			} catch (Throwable t) {
				restart("Error receiving data", t);
			}
		}
	}
}
