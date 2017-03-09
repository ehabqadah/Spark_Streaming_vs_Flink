package kafka.datacorn;

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import de.kdml.bigdatalab.spark_and_flink.common_utils.Configs;
import de.kdml.bigdatalab.spark_and_flink.common_utils.data.StreamRecord;

/**
 * ADS-B messages stream source
 * 
 * @author Ehab Qadah
 * 
 *         Mar 5, 2017
 */
public class AircraftPositionsStreamGenerator implements SourceFunction<String> {

	private static final int LINE_SLIDE_TIME_MS = 100;

	private static final long serialVersionUID = 2174904787118597072L;
	private static Configs configs = Configs.getInstance();
	boolean running = true;
	int i = 0;

	@Override
	public void run(SourceContext<String> ctx) throws Exception {

		while (running) {

			String[] adsbFiles = configs.getStringProp("adsbFiles").split(",");
			for (String file : adsbFiles) {

				System.out.println("streaming of " + file);

				try (BufferedReader br = new BufferedReader(new FileReader(file))) {
					String messageLine;
					while ((messageLine = br.readLine()) != null) {
						i++;

						// append streaming time
						messageLine = new StreamRecord(messageLine).toString();

						ctx.collect(messageLine);

						// Thread.sleep(LINE_SLIDE_TIME_MS);
					}
				} catch (Exception e) {

					System.out.println("exp" + e);
				}
			}
			// running = false;

		}
	}

	@Override
	public void cancel() {
		running = false;
	}

}