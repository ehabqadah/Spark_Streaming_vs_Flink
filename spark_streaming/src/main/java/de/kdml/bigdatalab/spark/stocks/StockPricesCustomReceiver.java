package de.kdml.bigdatalab.spark.stocks;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

import com.google.common.io.Closeables;

import de.kdml.bigdatalab.spark.Configs;
import de.kdml.bigdatalab.spark.SparkConfigsUtils;
import scala.Tuple3;

/**
 * his a example of the Spark stream custom receiver Please refer to
 * {@linkplain https://spark.apache.org/docs/2.0.2/streaming-custom-receivers.html#spark-streaming-custom-receivers}
 * 
 * @author Ehab Qadah
 * 
 *         Dec 10, 2016
 */
public class StockPricesCustomReceiver {

	private static Configs configs = Configs.getInstance();

	public static void main(String[] args) throws Exception {

		JavaSparkContext sc = SparkConfigsUtils.getSparkContext("CustomReceiver");
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(configs.getIntProp("batchDuration")));

		// Create an input stream with the custom receiver

		CustomStreamReceiver customStreamReceiver = new CustomStreamReceiver(configs.getStringProp("socketHost"),
				configs.getIntProp("socketPort"));

		JavaReceiverInputDStream<Tuple3<String, Double, Double>> prices = ssc.receiverStream(customStreamReceiver);

		// TODO

		prices.print();
		ssc.start();
		ssc.awaitTermination();
	}

	/***
	 * Receiver that receives data over a socket in format stock name,price
	 * 
	 * @author Ehab Qadah
	 * 
	 *         Dec 10, 2016
	 */
	public static class CustomStreamReceiver extends Receiver<Tuple3<String, Double, Double>> {

		/**
		 * Stocks and last prices map
		 */
		private static TreeMap<String, Double> lastPrices = new TreeMap<String, Double>();
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
						System.out.println("> Received data '" + userInput + "'");

						String[] priceData = userInput.split(",");// stock,price
						if (priceData.length == 2) {

							String stockId = priceData[0];
							Double price = Double.valueOf(priceData[1]);
							Double lastPrice = lastPrices.getOrDefault(stockId, price);
							//update stock price
							lastPrices.put(stockId, price);
							Tuple3<String, Double, Double> stockTuple = new Tuple3<String, Double, Double>(stockId,
									price, lastPrice);
							
							// store received data into Spark's memory
							store(stockTuple);
						}
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
