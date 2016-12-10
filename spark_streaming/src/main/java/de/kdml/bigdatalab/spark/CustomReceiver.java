package de.kdml.bigdatalab.spark;

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

import scala.Tuple2;

/**
 * his a example of the Spark stream custom receiver 
 * Please refer to {@linkplain https://spark.apache.org/docs/2.0.2/streaming-custom-receivers.html#spark-streaming-custom-receivers}
 
 * @author Ehab Qadah 
 * 
 * Dec 10, 2016
 */
public class CustomReceiver {



	private static Configs configs = Configs.getInstance();

	public static void main(String[] args) throws Exception {

		JavaSparkContext sc = SparkConfigsUtils.getSparkContext("CustomReceiver");
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(configs.getIntProp("batchDuration")));

		// Create an input stream with the custom receiver on target ip:port and
		// count the words in input stream of \n delimited text (eg. generated
		// by 'nc')

		CustomStreamReceiver customStreamReceiver = new CustomStreamReceiver(configs.getStringProp("socketHost"),
				configs.getIntProp("socketPort"));

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
	 * Dec 10, 2016
	 */
	public static class CustomStreamReceiver extends Receiver<String> {
		// ============= 
		// ==============
			private static long count=0;
		/**
			 * 
			 */
		private static final long serialVersionUID = 1L;
		String host = null;
		int port = -1;

		public CustomStreamReceiver(String host_, int port_) {
			super(StorageLevel.MEMORY_AND_DISK_2());
			host = host_;
			port = port_;
		}

		public void onStart() {
			// Start the thread that receives data over a connection
			new Thread() {
				@Override
				public void run() {
					receive();
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
		private void receive() {
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
						System.out.println(++count+" > Received data '" + userInput + "'");
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
