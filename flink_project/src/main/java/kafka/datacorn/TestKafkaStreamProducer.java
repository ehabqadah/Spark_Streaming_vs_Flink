package kafka.datacorn;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Serializable;
import java.util.Properties;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * This kafka's stream of lines producer using FlinkKafkaProducer09 it writes a
 * new random line every 1 second
 * 
 * @author Ehab Qadah
 * 
 *         Dec 8, 2016
 */
public class TestKafkaStreamProducer {

	public static void main(String[] args) throws Exception {
		// create execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		env.setParallelism(4);

		// add a simple source which is writing some strings
		DataStream<String> messageStream = env.addSource(new SimpleStringGenerator());

		Properties producerConfig = new Properties();
		producerConfig.put("bootstrap.servers", "localhost:9092,localhost:9093");
		// // write stream to Kafka
		messageStream.addSink(new FlinkKafkaProducer09<String>("datacorn", new SimpleStringSchema(), producerConfig,
				new TestPartitioner()));

		env.execute("kafka stream of random lines every 1 second!");
	}

	public static class TestPartitioner extends KafkaPartitioner<String> implements Serializable {
		private static final long serialVersionUID = 1627268846962918126L;

		private int targetPartition = -1;

		@Override
		public void open(int parallelInstanceId, int parallelInstances, int[] partitions) {
			if (parallelInstanceId < 0 || parallelInstances <= 0 || partitions.length == 0) {
				throw new IllegalArgumentException();
			}

			System.out.println("parallelInstanceId=" + parallelInstanceId + " parallelInstances " + parallelInstances
					+ "partions" + partitions.length);
			this.targetPartition = partitions[parallelInstanceId % partitions.length];
		}

		@Override
		public int partition(String next, byte[] serializedKey, byte[] serializedValue, int numPartitions) {
			if (targetPartition >= 0) {
				System.out.println("part=" + targetPartition + " next=" + next);
				return targetPartition;
			} else {
				throw new RuntimeException("The partitioner has not been initialized properly");
			}
		}
	}

	public static class SimpleStringGenerator implements SourceFunction<String> {
		/**
		 * Generates a new random line every LINE_SLIDE_TIME_MS
		 */
		private static final int LINE_SLIDE_TIME_MS = 1000;

		private static final long serialVersionUID = 2174904787118597072L;

		boolean running = true;
		int i = 0;

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while (running) {
				i++;

				// process the line.
				// get a random line from the
				// ctx.collect(loremLines[i % loremLines.length]);
				ctx.collect("str" + i);
				Thread.sleep(LINE_SLIDE_TIME_MS);
			}

		}

		@Override
		public void cancel() {
			running = false;
		}

	}

}
