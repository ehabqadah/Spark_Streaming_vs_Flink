package de.kdml.bigdatalab.flink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * This kafka's stream of lines producer using FlinkKafkaProducer09 it writes a
 * new random line every 1 second
 * 
 * @author Ehab Qadah
 * 
 *         Dec 8, 2016
 */
public class KafkaLinesStreamProducer {

	public static void main(String[] args) throws Exception {
		// create execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));

		// add a simple source which is writing some strings
		DataStream<String> messageStream = env.addSource(new SimpleStringGenerator());

		// // write stream to Kafka
		messageStream.addSink(new FlinkKafkaProducer09<String>("localhost:9092", "test", new SimpleStringSchema()));

		env.execute("kafka stream of random lines every 1 second!");
	}

	public static class SimpleStringGenerator implements SourceFunction<String> {
		/**
		 * Generates a new random line every  LINE_SLIDE_TIME_MS 
		 */
		private static final int LINE_SLIDE_TIME_MS = 1000;

		private static final long serialVersionUID = 2174904787118597072L;

		boolean running = true;
		int i = 0;

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while (running) {
				i++;
				//get a random line from the 
				ctx.collect(loremLines[i % loremLines.length]);
				Thread.sleep(LINE_SLIDE_TIME_MS);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		private static String randomLoremText = "Lorem ipsum dolor sit amet, \n consectetur adipiscing elit. Cras porta faucibus euismod. Nulla eget urna rutrum, \n sagittis orci sed, porta risus. Quisque sed bibendum orci. Proin ac pretium nibh. Sed metus elit, tincidunt non dui quis, venenatis consectetur ipsum. Nam lobortis elementum eros. Curabitur maximus a est a mattis. Duis semper venenatis sem sed dapibus. Maecenas facilisis risus vel neque ultrices facilisis. Phasellus auctor tortor non laoreet faucibus. Praesent sodales justo elit, vel pretium tellus tincidunt in. Fusce in lectus tortor. In porta sodales mattis. Pellentesque \n  habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas."

				+ " \n Suspendisse potenti. Sed id tristique erat. Ut mattis ornare erat, \n sodales fringilla ex viverra posuere. Curabitur in felis euismod metus \n mollis mattis sed ac turpis. Quisque ultricies volutpat bibendum. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Suspendisse scelerisque, massa non dapibus faucibus, velit felis dignissim nunc, in ultricies ante nunc non massa. In a odio in neque \n feugiat dictum. Vivamus sit amet ex tellus. Duis \n ultricies porttitor congue."

				+ "\n Nulla in porttitor elit. \n Vivamus nulla lacus, consectetur in risus vel, \n fringilla laoreet ligula. Phasellus et odio vitae purus bibendum congue. \n Curabitur at elit nec arcu semper tristique at sed elit. Praesent sed efficitur sem. Duis gravida tellus mauris, nec dapibus felis mollis auctor. Ut sollicitudin lobortis libero sit amet mattis. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; In accumsan, lacus vitae maximus mollis, massa tellus facilisis augue, ut congue diam mi quis orci. Suspendisse sit amet turpis rhoncus, vestibulum ex id, \n mollis quam. Donec leo velit, \n tincidunt non finibus eget, \n sollicitudin a nisi. Phasellus semper, mi at mollis cursus, enim est tincidunt nibh, sit amet dictum eros mi et dolor. \n Aenean egestas pretium turpis ut \n porttitor."

				+ "\n Ut condimentum a dolor vitae elementum. \n Maecenas erat lacus, \n feugiat eget ante nec, \n feugiat pellentesque leo. Sed ipsum ipsum, sollicitudin \n  vitae pulvinar eu, sagittis non odio. Etiam vitae odio arcu. In hac habitasse platea dictumst. Etiam laoreet ante in arcu fermentum, sit amet malesuada sapien pulvinar. Sed elit lorem, porttitor quis ante et, egestas maximus tortor. Sed mattis massa eu sem fermentum, vel vehicula augue ornare. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Duis aliquam felis varius leo consectetur fringilla. Aenean \n placerat, lacus ultricies pellentesque porttitor, eros \n lectus elementum turpis, ac dictum enim turpis vitae sapien."

				+ "\n Cras molestie ligula sed mi iaculis venenatis. \n Quisque imperdiet \n vestibulum elit, sit amet ultricies tellus mattis non. Fusce quis interdum eros.\n  Etiam elit metus, varius in risus eget, placerat blandit mauris. Proin diam nisi, pharetra id nulla eu, volutpat rhoncus purus. Fusce turpis ex, elementum quis scelerisque ac, dapibus nec est. Proin diam nisl, condimentum a finibus ac, rhoncus id lectus. Quisque volutpat suscipit odio eu bibendum. Cras quis sollicitudin lectus, suscipit porta metus. Etiam vel diam massa. Quisque cursus maximus orci quis varius. Vivamus accumsan tortor id \n neque feugiat dignissim. Cras vestibulum, tellus \n sed ullamcorper sagittis, turpis nunc sollicitudin lacus, et placerat magna erat nec nisi. \n Mauris aliquam justo sed porttitor tincidunt.";

		private static String[] loremLines = randomLoremText.split("\n");
	}

}
