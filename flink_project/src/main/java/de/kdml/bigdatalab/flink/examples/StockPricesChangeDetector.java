package de.kdml.bigdatalab.flink.examples;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Price Changes detector in flink
 * 
 * @author Ehab Qadah
 * 
 *         Dec 9, 2016
 */
public class StockPricesChangeDetector {

	private static final double PRICE_CHANGE_THRESHOLD = 2.0;

	public static void main(String[] args) throws Exception {

		// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// disable system logs
		env.getConfig().disableSysoutLogging();

		// get input data by connecting to the socket
		DataStream<String> lines = env.socketTextStream("localhost", 9999, "\n");

		// parse the data, group it, and aggregate the counts
		DataStream<StockWithPrice> prices = lines.map(line -> {
			String[] priceData = line.split(",");// stock,price
			if (priceData.length == 2) {

				String stockId = priceData[0];
				Double price = Double.valueOf(priceData[1]);
				StockWithPrice stockTuple = new StockWithPrice(stockId, price, price);
				return stockTuple;
			}
			return null;
		}).keyBy("stock").reduce((oldPrice, newPrice) -> {
			System.out.println(oldPrice +"\n newPrice="+ newPrice);
			
			return new StockWithPrice(oldPrice.stock, newPrice.price, oldPrice.price);
		});
		
	
		DataStream<StockWithPrice> filterPrices=prices.filter(stockPrice -> {
			return Math.abs(stockPrice.price - stockPrice.lastPrice) > PRICE_CHANGE_THRESHOLD;

		});
		// print the results with a single thread, rather than in parallel
		filterPrices.print().setParallelism(1);

		env.execute("price change detection in flink");
	}

	/**
	 * Data type for stock
	 */
	public static class StockWithPrice {

		public String stock;
		public double price, lastPrice;

		public StockWithPrice() {
		}

		public StockWithPrice(String stock, double price, double lastPrice) {
			this.stock = stock;
			this.price = price;
			this.lastPrice = lastPrice;
		}

		@Override
		public String toString() {
			return stock + ": price=" + price + " last price=" + lastPrice;
		}
	}

}
