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
import java.util.Date;

import static java.nio.file.StandardOpenOption.*;

import com.lmax.disruptor.EventHandler;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

/*
 * Persists the event to the file system
 */
public class BulkJournalToMongoEventHandler implements EventHandler<DealEvent> {
	Path fileP;
	OpenOption[] options;
	DB db;
	DBCollection dealColl;
	BulkWriteOperation bulk;
	int bulkCounter = 0;

	public BulkJournalToMongoEventHandler() {
		MongoClient mongoClient = null;
		try {
			mongoClient = new MongoClient();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		db = mongoClient.getDB("mydb");
		dealColl = db.getCollection("dealEvent");
		System.out.println("Connection to Mongo created: "
				+ dealColl.hashCode());

	}

	public void onEvent(DealEvent event, long sequence, boolean endOfBatch) {
		String JSON = event.getDealJSON();
		// Convert the JSON to a deal entity instance. In this case - take a
		// short cut
		this.journalToMongo("Sequence: " + sequence + " " + JSON);

		if (sequence == EventMain.producerCount) {
			System.out.println("Consumer complete: " + EventMain.producerCount);
	        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
	        Date dateEnd = new Date();
	        System.out.println(dateFormat.format(dateEnd)); //2014/08/06 15:59:48
		}
	}

	/**
	 * Write the event to MongoDB
	 */
	private void journalToMongo(String content) {
		BasicDBObject doc = new BasicDBObject("name", "MongoDB").append(
				"content", content);
		if (bulkCounter == 0) {
			bulk = dealColl.initializeUnorderedBulkOperation();
		}
		bulk.insert(doc);
		bulkCounter++;
		if (bulkCounter > 999) {
			bulk.execute();
			bulkCounter = 0;
		}

	}
}