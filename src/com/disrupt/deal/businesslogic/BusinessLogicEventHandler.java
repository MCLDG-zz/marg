package com.disrupt.deal.businesslogic;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.disrupt.deal.DealEntity;
import com.disrupt.deal.DealEvent;
import com.disrupt.deal.EventMain;
import com.lmax.disruptor.EventHandler;

public class BusinessLogicEventHandler implements EventHandler<DealEvent> {

	private ArrayList<DealEntity> dealMatched = null;
	private long numMatches = 0;
	
	public BusinessLogicEventHandler() {
		dealMatched = new ArrayList<DealEntity>();
	}

	public void onEvent(DealEvent event, long sequence, boolean endOfBatch) {

		if (!event.handlersVisited.contains("BusinessLogicEventHandler")) {

			// On receiving a new deal we need to firstly check if it can be
			// matched immediately
			// If not, we add it to the queue where it waits for a matching deal
			long startNano = System.nanoTime();
			int matched = MatchingEngine.match(event.getDealEntity(),
					dealMatched);
			long endNano = System.nanoTime();
			if (matched != MatchingEngine.MatchType.NOTMATCHED.getValue()) {
				if (sequence > 8000 && sequence < 8100) {
					System.out.println("Matching elapsed time: " + (endNano - startNano));
				}
				if (sequence > 4000000 && sequence < 4000100) {
					System.out.println("Matching elapsed time: " + (endNano - startNano));
				}
				numMatches++;
				// Clone the the ArrayList. We cannot just use the reference to dealMatched as it will be
				// reused for the next Event, overwriting the matched deals, so we have to Clone
				ArrayList<DealEntity> tempDealMatched = new ArrayList<DealEntity>();
				tempDealMatched.addAll(dealMatched);

				event.setDealMatched(tempDealMatched);
				event.setMatched(matched);
				dealMatched.clear();
			}
		} else {
			System.out.println("Matching. Seen this event before " + sequence);
			
		}
		long totalDeals = 0;
		if (sequence == EventMain.producerCount) {

			System.out.println("Matching. Received " + sequence
					+ " deals. Number of deals matched: " + numMatches);
			System.out.println("Matching. Size of matching map: "
					+ MatchingEngine.allDealsMap.size());
			Collection<Map<Integer, ArrayList<DealEntity>>> currencyMapColl = (Collection<Map<Integer, ArrayList<DealEntity>>>) MatchingEngine.allDealsMap
					.values();
			for (Iterator iterator = currencyMapColl.iterator(); iterator
					.hasNext();) {
				HashMap hm = (HashMap) iterator.next();
				// System.out.println("Matching. Lot size: " + hm.size());
				Set set = hm.keySet();
				for (Iterator iterator2 = set.iterator(); iterator2.hasNext();) {
					Integer i = (Integer) iterator2.next();
					ArrayList al = (ArrayList) hm.get(i);
					// System.out.println("Matching. Size of deals array for this lot is: "
					// + al.size());
					totalDeals += al.size();
				}
			}
			System.out
					.println("Matching. Total number of deals waiting to match: "
							+ totalDeals);
		}

		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		Date dateEnd = new Date();
		event.handlersVisited.add("BusinessLogicEventHandler");
		event.handlersVisited.add(dateFormat.format(dateEnd));

//		if (sequence > 69000 && sequence < 69020) {
//			System.out.println("BusinessLogic processing event number: "
//					+ sequence + " with matched flag: " + event.getMatched());
//			System.out.println("BusinessLogic processing event hash: "
//					+ event.hashCode());
//			System.out
//					.println("BusinessLogicEventHandler - the event has visited: "
//							+ event.handlersVisited.toString());
//		}

	}
}