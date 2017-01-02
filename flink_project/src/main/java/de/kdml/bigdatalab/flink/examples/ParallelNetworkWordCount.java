package de.kdml.bigdatalab.flink.examples;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * Network word counts flink's example with parallel source function
 * 
 * @author Ehab Qadah
 * 
 *         Dec 9, 2016
 */
public class ParallelNetworkWordCount {

	public static void main(String[] args) throws Exception {

		// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// disable system logs
		env.getConfig().disableSysoutLogging();

		// get input data by connecting to the sockets
		DataStream<String> lines = env.addSource(new ParallelSourceFunction("localhost", 9999, "localhost",10000 ));

		// parse the data, group it, and aggregate the counts
		DataStream<Tuple2<String, Integer>> wordCounts = lines
				.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
					// normalize and split the line into words
					String[] tokens = line.toLowerCase().split("\\W+");

					// emit the pairs
					for (String token : tokens) {
						if (token.length() > 0) {
							out.collect(new Tuple2<String, Integer>(token, 1));
						}
					}

				}).keyBy(0).sum(1);
		// print the results with a single thread, rather than in parallel
		wordCounts.print().setParallelism(1);

		env.execute("Network flink WordCount with parallel source function");
	}

	/**
	 * Parallel Source function connect to two sockets
	 * 
	 * @author Ehab Qadah 
	 * 
	 * Dec 13, 2016
	 */

	public static class ParallelSourceFunction implements SourceFunction<String> {

	
		private static final long serialVersionUID = -2565561689763376958L;
		boolean running = true;
		Thread firstReceiver = null, secondReceiver = null;
		private String host = null, host2 = null;
		private int port = -1, port2 = -1;

		public ParallelSourceFunction(String host, int port, String host2, int port2) {

			this.host = host;
			this.port = port;
			this.host2 = host2;
			this.port2 = port2;
		}

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while (running) {

				if (firstReceiver == null) {
					firstReceiver = new Thread(new Runnable() {

						@Override
						public void run() {
							receive(ctx, host, port);
							

						}
					});
					firstReceiver.start();
				}
				//start the second connection 
				if (secondReceiver == null) {
					secondReceiver = new Thread(new Runnable() {

						@Override
						public void run() {
							receive(ctx, host2, port2);

						}
					});

					secondReceiver.start();
				}
			}
		}

		@Override
		public void cancel() {
			running = false;
			firstReceiver = null;
			secondReceiver = null;

		}

		/**
		 * Create a socket connection and receive data until SourceFunction is
		 * stopped
		 */
		private void receive(SourceContext<String> ctx, String host, int port) {
			try {
				Socket socket = null;
				BufferedReader reader = null;
				String line = null;

				// connect to the server
				socket = new Socket(host, port);
				reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
				// Until stopped or connection broken continue reading
				while (running && (line = reader.readLine()) != null) {
					System.out.println(" Received data '" + line + "' from " + host + ":" + port);
					synchronized (ctx.getCheckpointLock()) {
						ctx.collect(line);
					}

				}

			} catch (Exception e) {
				System.out.println("receiving error:" + e);
			}
		}
	}
}
