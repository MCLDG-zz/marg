package com.disrupt.deal;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.lmax.disruptor.EventHandler;

/*
 * Persists the event to the file system
 */
public class JournalToKafkaEventHandler implements EventHandler<DealEvent> {
	Producer<String, String> producer;

	public JournalToKafkaEventHandler() {
		Properties props = new Properties();

		props.put("metadata.broker.list",
				"localhost:9092,localhost:9093,localhost:9094");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
//		props.put("partitioner.class", "com.disrupt.deal.SimplePartitioner");
		
		//set this property to get a sync response. Otherwise the Kafka write is async
//		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);

		producer = new Producer<String, String>(config);
	}

	public void onEvent(DealEvent event, long sequence, boolean endOfBatch) {
		String JSON = event.getDealJSON();
		KeyedMessage<String, String> data = new KeyedMessage<String, String>("edge.test", ""
				+ sequence, JSON);
		producer.send(data);
		if (sequence == EventMain.producerCount) {
			System.out.println("Kafka consumer complete: " + EventMain.producerCount);
	        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
	        Date dateEnd = new Date();
	        System.out.println(dateFormat.format(dateEnd)); //2014/08/06 15:59:48
		}

	}
}