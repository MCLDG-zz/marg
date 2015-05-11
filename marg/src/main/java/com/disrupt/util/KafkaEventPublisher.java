package com.disrupt.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaEventPublisher {
	public static long producerCount = 100000;
	public static String inputMessage = new String(
			"SELL,USDHKD,2,USD,1,HKD,7.5");
	public static String inputMessageBUY = new String(
			"BUY,USDHKD,2,USD,1,HKD,7.5");
	private static String[] currencies = new String[] { "USD", "HKD", "NZD",
			"AUD", "ZAR", "GBP", "EUR", "CNY", "VND", "THB", "TAD" };

	public static void main(String[] args) throws Exception {
		Producer<String, String> producer;
		Properties props = new Properties();

		props.put("metadata.broker.list",
				"localhost:9092,localhost:9093,localhost:9094");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// props.put("partitioner.class", "com.disrupt.deal.SimplePartitioner");
		// set this property to get a sync response. Otherwise the Kafka write
		// is async
		// props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);

		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		Date dateStart = new Date();
		System.out.println("Kafka Publisher complete at: "
				+ dateFormat.format(dateStart)); // 2014/08/06 15:59:48

		for (long l = 0; l <= producerCount; l++) {
			long currl = (long) (Math.random() * 100);
			long lRand = (long) ((Math.random() * 100)) + 1;
			String msg = null;
			if (l % 10 == 0) {
				msg = new String(l + "," + currl + ",SELL,"
						+ currencies[(int) (currl / 10)]
						+ currencies[(int) (lRand / 10)] + "," + lRand + ","
						+ currencies[(int) (currl / 10)] + ",1,"
						+ currencies[(int) (currl / 10 + 1)] + ",1," + 7.75);
			} else {
				msg = new String(l + "," + currl + ",BUY,"
						+ currencies[(int) (currl / 10)]
						+ currencies[(int) (lRand / 10)] + "," + lRand + ","
						+ currencies[(int) (currl / 10)] + ",1,"
						+ currencies[(int) (currl / 10 + 1)] + ",1," + 7.75);
			}
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(
					"edge.test", "" + l, msg);
			producer.send(data);

			if (l % (producerCount / 10) == 0)
				System.out.println("Producer: " + l);
		}

		dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		Date dateEnd = new Date();
		System.out.println("Kafka Publisher complete at: "
				+ dateFormat.format(dateEnd)); // 2014/08/06 15:59:48

	}
}