package com.disrupt.deal;

import com.disrupt.deal.DealEvent;
import com.disrupt.deal.DealEventFactory;
import com.disrupt.deal.DealEventProducerWithTranslator;
import com.disrupt.deal.JournalToFileEventHandler;
import com.disrupt.deal.businesslogic.BusinessLogicEventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

public class EventMain extends Thread {
	public static long producerCount = 1000000;
	public static String inputMessage = new String(
			"SELL,USDHKD,2,USD,1,HKD,7.5");
	public static String inputMessageBUY = new String(
			"BUY,USDHKD,2,USD,1,HKD,7.5");
	private static String[] currencies = new String[] { "USD", "HKD", "NZD",
			"AUD", "ZAR", "GBP", "EUR", "CNY", "VND", "THB", "TAD" };
	final static String clientId = "SimpleConsumerDemoClient";
	final static String TOPIC = "edge.test";
	ConsumerConnector consumerConnector;

	public EventMain() {
		Properties properties = new Properties();
		properties.put("zookeeper.connect", "localhost:2181");
		properties.put("group.id", "test-group");
		ConsumerConfig consumerConfig = new ConsumerConfig(properties);
		consumerConnector = Consumer
				.createJavaConsumerConnector(consumerConfig);
	}

	public void run() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(TOPIC, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector
				.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(TOPIC).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		System.out.println("Kafka - connection to topic made: " + TOPIC);
		while (it.hasNext())
			System.out.println(new String(it.next().message()));
		
	}

	public static void main(String[] args) throws Exception {
		// Executor that will be used to construct new threads for consumers
		Executor executor = Executors.newCachedThreadPool();

		// The factory for the event
		DealEventFactory factory = new DealEventFactory();

		// Specify the size of the ring buffer, must be power of 2.
		int bufferSize = (int) Math.pow(2, 16);

		// Construct the Disruptor
		Disruptor<DealEvent> disruptor = new Disruptor<>(factory, bufferSize,
				executor, ProducerType.SINGLE,
				new com.lmax.disruptor.BlockingWaitStrategy());

		EventHandler<DealEvent> eh1 = new JournalToFileEventHandler();
		// EventHandler<DealEvent> eh2 = new JournalToMongoEventHandler();
		// EventHandler<DealEvent> eh3 = new BulkJournalToMongoEventHandler();
		EventHandler<DealEvent> eh4 = new DealParserEventHandler();
		// EventHandler<DealEvent> eh5 = new DoNothingEventHandler();
		// EventHandler<DealEvent> eh6 = new
		// JournalToFileBufferedEventHandler();
		// EventHandler<DealEvent> eh7 = new JournalToCassandraEventHandler();
		// EventHandler<DealEvent> eh8 = new
		// BatchJournalToCassandraEventHandler();
		//EventHandler<DealEvent> eh9 = new ReplicatorEventHandler();
		EventHandler<DealEvent> eh10 = new BusinessLogicEventHandler();
		// EventHandler<DealEvent> eh11 = new JournalToKafkaEventHandler();
		//EventHandler<DealEvent> eh12 = new MatchedToMongoEventHandler();

		// Connect the handler
		// disruptor.handleEventsWith(eh1);
		// disruptor.handleEventsWith(eh2);
		// disruptor.handleEventsWith(eh3);
		// disruptor.handleEventsWith(eh4);
		// disruptor.handleEventsWith(eh5);
		// disruptor.handleEventsWith(eh6);
		// disruptor.handleEventsWith(eh7);
		// disruptor.handleEventsWith(eh8);
		// disruptor.handleEventsWith(eh9);
		// disruptor.handleEventsWith(eh10);
		// disruptor.handleEventsWith(eh1,eh4);
		disruptor.handleEventsWith(eh1, eh4).then(eh10);
		// disruptor.handleEventsWith(eh11);

		// Start the Disruptor, starts all threads running
		disruptor.start();

		// Get the ring buffer from the Disruptor to be used for publishing.
		RingBuffer<DealEvent> ringBuffer = disruptor.getRingBuffer();

		DealEventProducerWithTranslator producer = new DealEventProducerWithTranslator(
				ringBuffer);
		System.out.println("Ringbuffer size: " + disruptor.getBufferSize());
		System.out.println("Barrier: " + disruptor.getBarrierFor(eh4));

		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		Date dateStart = new Date();
		System.out.println(dateFormat.format(dateStart)); // 2014/08/06 15:59:48

		// Pass the main method an argument of 'kafka' in order to use
		// Kafka as the event producer. Otherwise the 'for' loop
		// below will produce the events
		if (args.length > 0 && args[0].equals("kafka")) {
			EventMain ev = new EventMain();
			ev.start();

		} else {

			for (long l = 0; l <= producerCount; l++) {
				long currl = (long) (Math.random() * 100);
				long lRand = (long) ((Math.random() * 100)) + 1;
				if (l % 10 == 0) {
					producer.onData(l + "," + currl + ",SELL,"
							+ currencies[(int) (currl / 10)]
							+ currencies[(int) (lRand / 10)] + "," + lRand
							+ "," + currencies[(int) (currl / 10)] + ",1,"
							+ currencies[(int) (currl / 10 + 1)] + ",1," + 7.75);
				} else {
					producer.onData(l + "," + currl + ",BUY,"
							+ currencies[(int) (currl / 10)]
							+ currencies[(int) (lRand / 10)] + "," + lRand
							+ "," + currencies[(int) (currl / 10)] + ",1,"
							+ currencies[(int) (currl / 10 + 1)] + ",1," + 7.75);
				}
				// producer.onData();
				if (l % (producerCount / 10) == 0)
					System.out.println("Producer: " + l);
				// Thread.sleep(100);
			}
		}
		disruptor.shutdown();
		dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		Date dateEnd = new Date();
		System.out.println(dateFormat.format(dateEnd)); // 2014/08/06 15:59:48

	}
}