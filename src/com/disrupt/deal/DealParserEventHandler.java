package com.disrupt.deal;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.lmax.disruptor.EventHandler;

/*
 * To cut a long story short - the ringBuffer is initialised with a size that is
 * usually less than the number of events to be processed. For instance, I initialise
 * it with 65536 and process 10M events. In this case the buffer is reused, and the events
 * in the buffer are reused. This means that any instance variables in the event will
 * retain their existing values. 
 * 
 * In my case I create the event by passing a String containing a deal. However,
 * if the event is being reused there will already be a DealEntity object in the
 * event which will refer to a different deal. This means that the event should
 * really be reset before being reused. If the event contains any variables, such
 * as 'boolean matched', and this is set by some later event handler, it may contain
 * a value of true for this new event (since the event is reused)
 * 
 * See the use of the reset method below
 */
public class DealParserEventHandler implements EventHandler<DealEvent> {
	public void onEvent(DealEvent event, long sequence, boolean endOfBatch) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		Date dateEnd = new Date();
		if (event.getDealEntity() != null) {
			if (event.getDealEntity().getProcessed()) {

//				System.out
//						.println("Deal Entity already processed. Sequence expected: "
//								+ sequence
//								+ ". Actual sequence: "
//								+ event.getDealEntity().getSequence() + ". Resetting event");
				event.resetEvent();
			}
		}
		if (!event.handlersVisited.contains("DealParserEventHandler")) {
			String CSV = event.getDealMsg();
			event.setDealEntity(this.parseCSVToObject(CSV));
			event.getDealEntity().setProcessed(true);

			if (sequence == EventMain.producerCount) {
				System.out.println("DealParserEventHandler Consumer complete: "
						+ EventMain.producerCount);
				// DateFormat dateFormat = new
				// SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
				// Date dateEnd = new Date();
				System.out.println(dateFormat.format(dateEnd)); // 2014/08/06
																// 15:59:48
			}
		}
		event.handlersVisited.add("DealParserEventHandler");
		event.handlersVisited.add(dateFormat.format(dateEnd));
		// if (event.getMatched() && sequence > 69000 && sequence < 69020) {
		// System.out.println("DealParser processing event number: "
		// + sequence + " with matched flag: " + event.getMatched());
		// System.out.println("DealParser processing event hash: " +
		// event.hashCode());
		// }
//		if (sequence > 69000 && sequence < 69020) {
//			System.out.println("DealParser processing event number: "
//					+ sequence + " with matched flag: " + event.getMatched()
//					+ " and deal seq: " + event.getDealEntity().getSequence());
//			System.out.println("DealParser processing event hash: "
//					+ event.hashCode());
//			System.out.println("DealParser - the event has visited: "
//					+ event.handlersVisited.toString());
//		}
	}

	/*
	 * Parse the CSV string and convert to DealEntity object. DealEntity is
	 * created as follows:
	 * 
	 * this.dealID = dealID; this.sequence = sequence; this.transactionType =
	 * transactionType; this.contractName = contractName; this.lots = lots;
	 * this.CCY1 = CCY1; this.CCY2 = CCY2; this.CCY1Amount = CCY1Amount;
	 * this.CCY2Amount = CCY2Amount;
	 */
	public DealEntity parseCSVToObject(String CSV) {

		String[] tokens = CSV.split(",");
		DealEntity dealEntity = new DealEntity((long) new Long(tokens[0]),
				(long) new Long(tokens[1]), tokens[2], tokens[3],
				(float) new Float(tokens[4]), tokens[5], (double) new Double(
						tokens[6]), tokens[7], (double) new Double(tokens[8]));
		return dealEntity;

	}
}