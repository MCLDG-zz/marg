package com.disrupt.deal;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

/**
 * Pubsub envelope subscriber
 * 
 * I noticed that this subscriber loses messages arbitrarily. Reading the ZeroMQ
 * docs it seems that subs can lose messages when they cannot keep up.
 * 
 * The resolution is to use
 */

public class EventSub {

	static long counter = 0;

	public static void main(String[] args) {

		// Prepare our context and subscriber
		Context context = ZMQ.context(1);
		Socket subscriber = context.socket(ZMQ.SUB);

		subscriber.connect("epgm://127.0.0.1:5500");
		subscriber.subscribe("A".getBytes());
		System.out.println("Subscriber starting to listen: ");
		ArrayList<Long> al = new ArrayList((int) EventMain.producerCount);
		while (!Thread.currentThread().isInterrupted()) {
			// Read envelope with address
			String address = subscriber.recvStr();
			String contents = subscriber.recvStr();
			Long l = new Long(contents);
			System.out.println("Replicator Consumer read: " + counter
					+ " contents: " + contents);
			al.add(l);
			// if (counter != l) {
			// System.out.println("Replicator Sub is OUT OF SYNC: " + counter
			// + " contents: " + contents);
			// DateFormat dateFormat = new SimpleDateFormat(
			// "yyyy/MM/dd HH:mm:ss.SSS");
			// Date dateStart = new Date();
			// System.out.println(dateFormat.format(dateStart)); // 2014/08/06
			// // 15:59:48
			//
			// break;
			// }

			if (counter % 100000 == 0) {
				System.out.println("Replicator Consumer read: " + counter
						+ " contents: " + contents);
			}
			if (counter == EventMain.producerCount || l == EventMain.producerCount) {
				System.out.println("Subscriber closing. Read: " + counter);
				subscriber.close();
				context.term();
				DateFormat dateFormat = new SimpleDateFormat(
						"yyyy/MM/dd HH:mm:ss.SSS");
				Date dateStart = new Date();
				System.out.println(dateFormat.format(dateStart));
				// 2014/08/06 15:59:48

				// Now let's try and discover what is missing
				int i = 0;
				Long c = null;
				for (Iterator<Long> iterator = al.iterator(); iterator
						.hasNext();) {
					c = iterator.next();
					if (i == c) {
						// System.out.println("Match found: " + c);
					} else {
						System.out.println("Mismatch found. loop counter is: "
								+ i);
						System.out.println("but array contains " + c);
						System.out.println("Previous item: " + al.get(i - 1));
						System.out.println("Next item: " + al.get(i + 1));
						break;
					}
					i++;

				}

				break;
			}

			counter++;
		}
	}
}
