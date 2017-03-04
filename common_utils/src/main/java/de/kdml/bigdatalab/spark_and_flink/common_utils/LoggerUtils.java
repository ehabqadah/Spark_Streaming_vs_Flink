package de.kdml.bigdatalab.spark_and_flink.common_utils;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class LoggerUtils {

	static {

		warmUp();
	}

	private static Logger logger;

	public static void logMessage(String message) {
		logger.info(message);

	}

	private static void warmUp() {

		logger = Logger.getLogger("app-logger");

		// configure the logger with handler and formatter
		try {
			FileHandler fileHandler = new FileHandler(Configs.getInstance().getStringProp("logFilePath"));
			logger.addHandler(fileHandler);
			fileHandler.setFormatter(new Formatter() {

				@Override
				public String format(LogRecord record) {
					// only log the message
					return record.getMessage() + "\n";
				}
			});
		} catch (SecurityException | IOException e) {
			System.out.println(e);
		}
	}
}
