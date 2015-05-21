package com.disrupt.deal;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.lmax.disruptor.EventHandler;

public class ReplicatorEventHandler implements EventHandler<DealEvent> {

	Context context;
	Socket publisher1;
	Socket publisher2;

	public ReplicatorEventHandler() {
		context = ZMQ.context(1);
		publisher1 = context.socket(ZMQ.PUB);
		publisher1.bind("epgm://127.0.0.1:5500");
		publisher1.setIdentity("A".getBytes());
		
		//subscriber.setRate(1000000);
		publisher1.setHWM(1000000);
		publisher1.setBacklog(1000000);


		// binding takes a while so sleep
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void onEvent(DealEvent event, long sequence, boolean endOfBatch) {
		publisher1.sendMore("A");
		boolean isSent = publisher1.send(event.getDealMsg());
//		if (sequence % 2 == 0) {
//			publisher1.sendMore("A");
//			boolean isSent = publisher1.send("" + sequence);
//		} else {
//			publisher2.sendMore("B");
//			boolean isSent = publisher2.send("" + sequence);
//
//		}
		if (sequence % 100 == 0) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		
		if (sequence == EventMain.producerCount) {
			System.out.println("ReplicatorEventHandler complete: "
					+ EventMain.producerCount + " sequence: " + sequence
					+ " end: " + endOfBatch);
			DateFormat dateFormat = new SimpleDateFormat(
					"yyyy/MM/dd HH:mm:ss.SSS");
			Date dateEnd = new Date();
			System.out.println(dateFormat.format(dateEnd)); // 2014/08/06
			publisher1.close();
			//publisher2.close();
			context.term();
			System.out.println("Socket closed");
			dateEnd = new Date();
			System.out.println(dateFormat.format(dateEnd)); // 2014/08/06
		}

	}
}