package kafka.datacorn;

import java.io.Serializable;
import java.util.Properties;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import de.kdml.bigdatalab.spark_and_flink.common_utils.Configs;

/**
 * This kafka's stream of lines producer using FlinkKafkaProducer09 it writes
 * the ADS-B messages
 * 
 * @author Ehab Qadah
 * 
 *         Dec 8, 2016
 */
public class DatacornKafkaStreamProducer {

	private static Configs configs = Configs.getInstance();

	public static void main(String[] args) throws Exception {

		// create execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		env.setParallelism(4);

		// add a simple source which is writing some strings
		DataStream<String> messageStream = env.addSource(new AircraftPositionsStreamGenerator());

		Properties producerConfig = new Properties();
		producerConfig.put("bootstrap.servers", configs.getStringProp("bootstrap.servers"));
		// // write stream to Kafka
		messageStream.addSink(new FlinkKafkaProducer09<String>(configs.getStringProp("topicId"),
				new SimpleStringSchema(), producerConfig, new RandomPartitioner()));

		env.execute("kafka stream ");
	}

	public static class RandomPartitioner extends KafkaPartitioner<String> implements Serializable {
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
				return targetPartition;
			} else {
				throw new RuntimeException("The partitioner has not been initialized properly");
			}
		}
	}

}
