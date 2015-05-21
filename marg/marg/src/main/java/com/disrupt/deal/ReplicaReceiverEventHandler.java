package com.disrupt.deal;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.disrupt.deal.businesslogic.BusinessLogicEventHandler;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * Pubsub envelope subscriber
 * 
 * I noticed that this subscriber loses messages arbitrarily. Reading the ZeroMQ
 * docs it seems that subs can lose messages when they cannot keep up.
 * 
 * The resolution is to use
 */

public class ReplicaReceiverEventHandler {

	static long counter = 0;
	static DealEventProducerWithTranslator producer;

	public static void main(String[] args) {

		// Executor that will be used to construct new threads for consumers
		Executor executor = Executors.newCachedThreadPool();

		// The factory for the event
		DealEventFactory factory = new DealEventFactory();

		// Specify the size of the ring buffer, must be power of 2.
		int bufferSize = (int) Math.pow(2, 16);

		// Construct the Disruptor
		Disruptor<DealEvent> disruptor = new Disruptor<>(factory, bufferSize,
				executor);

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
		// EventHandler<DealEvent> eh9 = new ReplicatorEventHandler();
		EventHandler<DealEvent> eh10 = new BusinessLogicEventHandler();
		// EventHandler<DealEvent> eh11 = new JournalToKafkaEventHandler();
		EventHandler<DealEvent> eh12 = new MatchedToMongoEventHandler();

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
		// disruptor.handleEventsWith(eh4,eh6,eh9).then(eh10);
		//disruptor.handleEventsWith(eh1, eh4).then(eh10);
        disruptor.handleEventsWith(eh1, eh4).then(eh10).then(eh12);
		// disruptor.handleEventsWith(eh11);

		// Start the Disruptor, starts all threads running
		disruptor.start();

		// Get the ring buffer from the Disruptor to be used for publishing.
		RingBuffer<DealEvent> ringBuffer = disruptor.getRingBuffer();

		producer = new DealEventProducerWithTranslator(ringBuffer);

		// Prepare our context and subscriber
		Context context = ZMQ.context(1);
		Socket subscriber = context.socket(ZMQ.SUB);

		subscriber.connect("epgm://127.0.0.1:5500");
		subscriber.subscribe("A".getBytes());
		System.out.println("Subscriber config. Backlog: "
				+ subscriber.getBacklog() + " FD: " + subscriber.getFD()
				+ " HWM: " + subscriber.getHWM() + " Rate: "
				+ subscriber.getRate() + " Rcv HWM: " + subscriber.getRcvHWM()
				+ " Recovery Interval: " + subscriber.getRecoveryInterval()
				+ " receive time out " + subscriber.getReceiveTimeOut()
				+ " reconnect ivl " + subscriber.getReconnectIVL());

		// subscriber.setRate(1000000);
		subscriber.setHWM(10000900);
		subscriber.setBacklog(10000000);
		subscriber.setRcvHWM(10000000);
		subscriber.setReconnectIVL(10000);

		System.out.println("Subscriber config. Backlog: "
				+ subscriber.getBacklog() + " FD: " + subscriber.getFD()
				+ " HWM: " + subscriber.getHWM() + " Rate: "
				+ subscriber.getRate() + " Rcv HWM: " + subscriber.getRcvHWM()
				+ " Recovery Interval: " + subscriber.getRecoveryInterval()
				+ " receive time out " + subscriber.getReceiveTimeOut()
				+ " reconnect ivl " + subscriber.getReconnectIVL());

		System.out.println("Subscriber starting to listen: ");
		long prevSeq = 0;
		while (!Thread.currentThread().isInterrupted()) {
			// Read envelope with address
			String address = subscriber.recvStr();
			String contents = subscriber.recvStr();
			// Long l = Long.valueOf(contents);

			// if (l.longValue() > 0 && EventMain.producerCount % l.longValue()
			// == 0) {
			// System.out.println("Replica Receiver received: contents " + " " +
			// contents + ", expected: " + prevSeq);
			// }
			// if (prevSeq != l.longValue()) {
			// System.out.println("Replica Receiver out of sync. Received: contents "
			// + " " + contents + " but expected: " + prevSeq);
			// break;
			// }
			prevSeq++;
			if (EventMain.producerCount % 100 == 0) {
				System.out.println("Replica Receiver received: contents " + " "
						+ contents + ", expected: " + prevSeq);
			}
			publish(contents);
		}
	}

	public static void publish(String msg) {
		producer.onData(msg);
	}
}
