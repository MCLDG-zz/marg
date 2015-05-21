package com.disrupt.deal;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.json.JSONObject;

import static java.nio.file.StandardOpenOption.*;

import com.disrupt.deal.businesslogic.MatchingEngine;
import com.lmax.disruptor.EventHandler;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;

/*
 * Persists the event to the file system
 */
public class MatchedToMongoEventHandler implements EventHandler<DealEvent> {
	Path fileP;
	OpenOption[] options;
	DBCollection dealColl;

	long numMatches = 0;

	public MatchedToMongoEventHandler() {
		MongoClient mongoClient = null;
		try {
			mongoClient = new MongoClient();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		DB db = mongoClient.getDB("mydb");
		dealColl = db.getCollection("dealEvent");
		System.out.println("Connection to Mongo created: "
				+ dealColl.hashCode());

	}

	public void onEvent(DealEvent event, long sequence, boolean endOfBatch) {

		// if (event.getMatched() && sequence > 69000 && sequence < 69020) {
		// System.out.println("MatchedMongo processing event number: "
		// + sequence + " with matched flag: " + event.getMatched());
		// System.out.println("MatchedMongo processing event hash: "
		// + event.hashCode());
		// }

		if (!event.handlersVisited.contains("MatchedToMongoEventHandler")) {

			if (event.getMatched() != MatchingEngine.MatchType.NOTMATCHED.getValue()) {
				numMatches++;
				DealEntity originalDeal = event.getDealEntity();
				ArrayList<DealEntity> matchedDeals = event.getDealMatched();
				ArrayList<String> deals = new ArrayList<String>();

				deals.add(dealEntityToJSON(originalDeal));
				for (Iterator iterator = matchedDeals.iterator(); iterator
						.hasNext();) {
					DealEntity matchedDeal = (DealEntity) iterator.next();
					deals.add(dealEntityToJSON(matchedDeal));
				}

				this.journalToMongo(deals);

				if (sequence == EventMain.producerCount) {
					System.out
							.println("MatchedMongo consumer complete. Dealt with "
									+ EventMain.producerCount
									+ " events, and "
									+ numMatches + " matched events");
					DateFormat dateFormat = new SimpleDateFormat(
							"yyyy/MM/dd HH:mm:ss.SSS");
					Date dateEnd = new Date();
					System.out.println(dateFormat.format(dateEnd)); // 2014/08/06
																	// 15:59:48
				}
			}
		}
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		Date dateEnd = new Date();
		event.handlersVisited.add("MatchedToMongoEventHandler");
		event.handlersVisited.add(dateFormat.format(dateEnd));

	}

	/**
	 * Write the event to MongoDB
	 */
	private void journalToMongo(ArrayList<String> deals) {
		BasicDBObject doc = new BasicDBObject("dealType", "matchedDeals");
		for (int i = 0; i < deals.size(); i++) {
			DBObject dbObject = (DBObject) JSON.parse(deals.get(i));
			if (i == 0) {
				doc.append("original", dbObject);
			} else
				doc.append("matched", dbObject);
		}
		dealColl.insert(doc, WriteConcern.ACKNOWLEDGED);
	}

	private String dealEntityToJSON(DealEntity dealEntity) {
		JSONObject jsonObj = new JSONObject(dealEntity);
		return jsonObj.toString();
	}

}