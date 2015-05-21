package com.disrupt.deal.businesslogic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.disrupt.deal.DealEntity;

public class MatchingEngine {

	// private static Map<String, List<DealEntity>> dealsMap = new
	// HashMap<String, List<DealEntity>>();
	public static Map<String, Map<Integer, ArrayList<DealEntity>>> allDealsMap = new HashMap<String, Map<Integer, ArrayList<DealEntity>>>();
	private static Map<Integer, ArrayList<DealEntity>> currencyMap;
	private static ArrayList<DealEntity> currencyList;

	public static enum MatchType {
		NOTMATCHED(0), FULLMATCH(1), PARTIALMATCH(2);

		private final int match;

		private MatchType(int match) {
			this.match = match;
		}

		public int getValue() {
			return match;
		}
	}

	/*
	 * matchedDealEntity: method should be called with this empty. This method
	 * will populate it with the matching deals. During a full match, where a
	 * deal matches on currency pair and lot size there will be one deal in this
	 * ArrayList. In the case of a partial match there will be > 1 deal in this
	 * ArrayList.  
	 * 
	 * Returns: NOTMATCHED(0), FULLMATCH(1), PARTIALMATCH(2);
	 * 
	 * TODO: partial match
	 */
	public static int match(DealEntity dealEntity,
			ArrayList<DealEntity> matchedDealEntity) {

		boolean matchFound = false; 
		boolean partialMatchFound = false;

		// Construct the key to lookup matching deals
		String matchDealKey = dealEntity.getContractName()
				+ reverseTransType(dealEntity.getTransactionType());
		String lookupDealKey = dealEntity.getContractName()
				+ dealEntity.getTransactionType();

		int lots = (int) dealEntity.getLots(); 

		// Use they key from the event to see if we have an match in our deal
		// cache. If we find a match look for matching lots
		currencyMap = allDealsMap.get(matchDealKey);
		if (currencyMap != null) {
			// Now we have the deals associated with the currency pair. Let's
			// look for a match on Lots (Lots is the key of currencyMap)
			if (currencyMap.containsKey(lots)) {
				currencyList = currencyMap.get(lots);
				// Since we are operating on FIFO principles, always get the
				// first item
				// We have a match so remove the original deal to ensure it
				// isn't
				// matched again
				if (currencyList.size() > 0) {
					matchedDealEntity.add((DealEntity) currencyList.remove(0));
					matchFound = true;
				}
			} else {
				// Means we have no exact match on Lot, so let's try a partial
				// match
				// We should do a partial match on a FIFO basis. The problem is
				// that
				// our currencyMap is keyed by Lots. The value for each Lot is
				// an
				// ArrayList in sequential time order, but what we need is an
				// array
				// of deals in sequential time order for all Lots

				// hmmmmm, how are we going to do this???
				int ind = lots;
				int lotsToAssign = lots;
				// System.out
				// .println("Matching engine found partial match. Need to match: "
				// + lots);
				while (ind > 0) {
					if (currencyMap.containsKey(ind)) {
						currencyList = currencyMap.get(ind);
						if (currencyList.size() > 0) {
							matchedDealEntity.add((DealEntity) currencyList
									.remove(0));
							matchFound = true;
							lotsToAssign -= ind;
							if (lotsToAssign < ind) {
								ind = lotsToAssign;
							}
						} else {
							ind--;
						}
					} else {
						ind--;
					}
				}
				// If a partial match where the full quota of lots could not be
				// filled, store the deal with the remaining number of lots to
				// be
				// filled
				if (lots - lotsToAssign > 0) {
					partialMatchFound = true;
					dealEntity.setLots(lotsToAssign);

//					System.out
//							.println("Matching engine partial match. Original lots: " + lots + ". Matched: "
//									+ (lots - lotsToAssign)
//									+ ". Lots outstanding: " + lotsToAssign + ". Deal sequence: " + dealEntity.getSequence());

				}
			}
		}
		// If no match, or partial match, add to the array
		if (!matchFound || (matchFound && partialMatchFound)) {
			currencyMap = allDealsMap.get(lookupDealKey);
			if (currencyMap != null) {
				// Now we have the deals associated with the currency pair.
				// Let's
				// look for a match on Lots (Lots is the key of currencyMap)
				if (currencyMap.containsKey(lots)) {
					currencyList = currencyMap.get(lots);
					currencyList.add(dealEntity);
				} else {
					currencyList = new ArrayList();
					currencyList.add(dealEntity);
					currencyMap.put((int) dealEntity.getLots(), currencyList);
				}
			} else {
				currencyList = new ArrayList();
				currencyList.add(dealEntity);
				currencyMap = new HashMap<Integer, ArrayList<DealEntity>>();
				currencyMap.put((int) dealEntity.getLots(), currencyList);
				allDealsMap.put(lookupDealKey, currencyMap);
			}
		}
		int ret = 0;
		if (partialMatchFound)
			ret = MatchType.PARTIALMATCH.getValue();
		else if (matchFound)
			ret = MatchType.FULLMATCH.getValue();
		else
			ret = MatchType.NOTMATCHED.getValue();
		return ret;
	}

	private static String reverseTransType(String transactionType) {
		if (transactionType.equals("BUY"))
			return "SELL";
		else
			return "BUY";
	}
}