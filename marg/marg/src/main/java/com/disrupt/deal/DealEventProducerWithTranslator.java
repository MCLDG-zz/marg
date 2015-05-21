package com.disrupt.deal;

import java.nio.ByteBuffer;

import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.EventTranslatorOneArg;

public class DealEventProducerWithTranslator {
	private final RingBuffer<DealEvent> ringBuffer;

	public DealEventProducerWithTranslator(RingBuffer<DealEvent> ringBuffer) {
		this.ringBuffer = ringBuffer;
	}

	// private static final EventTranslator<DealEvent> TRANSLATOR = new
	// EventTranslator<DealEvent>() {
	// public void translateTo(DealEvent event, long sequence) {
	// event.setDealJSON("JSON String with Deal Info");
	// }
	// };
	private static final EventTranslatorOneArg<DealEvent, String> TRANSLATORO = new EventTranslatorOneArg<DealEvent, String>() {
		public void translateTo(DealEvent event, long sequence, String msg) {
			event.setDealMsg(msg);
			event.setDealJSON(msg);
//			if (sequence > 65530)
//				System.out.println("Publisher Sequence: " + sequence);
		}
	};

	// public void onData() {
	// ringBuffer.publishEvent(TRANSLATOR);
	// }

	public void onData(String msg) {
		ringBuffer.publishEvent(TRANSLATORO, msg);
//		if (ringBuffer.getCursor() > 65530)
//		System.out.println("Buffer cursor: " + ringBuffer.getCursor()
//				+ " Remain Capacity: " + ringBuffer.remainingCapacity());

	}
}