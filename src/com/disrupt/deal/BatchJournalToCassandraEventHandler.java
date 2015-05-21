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
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static java.nio.file.StandardOpenOption.*;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.lmax.disruptor.EventHandler;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

/*
 * Persists the event to the file system
 */
public class BatchJournalToCassandraEventHandler implements
		EventHandler<DealEvent> {
	private Cluster cluster;
	private Session session;
	PreparedStatement stmt;
	BoundStatement boundStatement;
	BatchStatement batch;
	int bulkCounter = 0;

	public BatchJournalToCassandraEventHandler() {
		this.connect("127.0.0.1");
		this.createSchema();
		this.createStmt();
	}

	public void onEvent(DealEvent event, long sequence, boolean endOfBatch) {
		String JSON = event.getDealJSON();
		// Convert the JSON to a deal entity instance. In this case - take a
		// short cut
		this.journalToCassandra(sequence, "" + JSON, "" + sequence);
		if (sequence == EventMain.producerCount) {
			System.out.println("Consumer complete: " + EventMain.producerCount + " end-batch: " + endOfBatch + " sequence: " + sequence);
			DateFormat dateFormat = new SimpleDateFormat(
					"yyyy/MM/dd HH:mm:ss.SSS");
			Date dateEnd = new Date();
			System.out.println(dateFormat.format(dateEnd)); // 2014/08/06
															// 15:59:48
		}
	}

	public void connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
		session = cluster.connect();
	}

	public void close() {
		cluster.close();

	}

	public void createSchema() {
		session.execute("CREATE KEYSPACE IF NOT EXISTS simplex WITH replication "
				+ "= {'class':'SimpleStrategy', 'replication_factor':1};");
		session.execute("CREATE TABLE IF NOT EXISTS simplex.event2 ("
				+ "id bigint PRIMARY KEY," + "JSON text," + "data text" + ");");
	}

	public void createStmt() {
		stmt = session.prepare("INSERT INTO simplex.event2 (id) VALUES (?)");
		batch = new BatchStatement();

	}

	public void journalToCassandra(long id, String JSON, String data) {
		batch.add(stmt.bind(id));
		bulkCounter++;
		if (bulkCounter > 999) {
			Collection<Statement> al = batch.getStatements();
			session.executeAsync(batch);
			bulkCounter = 0;
			batch.clear();
		}

	}
}