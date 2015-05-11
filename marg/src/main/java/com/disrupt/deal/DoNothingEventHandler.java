package com.disrupt.deal;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.lmax.disruptor.EventHandler;

public class DoNothingEventHandler implements EventHandler<DealEvent> {
	public void onEvent(DealEvent event, long sequence, boolean endOfBatch) {
		// try {
		// Thread.sleep(4);
		// } catch (InterruptedException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
		if (sequence == EventMain.producerCount) {
			System.out.println("Consumer complete: " + EventMain.producerCount);
			DateFormat dateFormat = new SimpleDateFormat(
					"yyyy/MM/dd HH:mm:ss.SSS");
			Date dateEnd = new Date();
			System.out.println(dateFormat.format(dateEnd)); // 2014/08/06
															// 15:59:48
		}

	}
}