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

/*
 * Persists the event to the file system
 */
public class JournalToFileEventHandler implements EventHandler<DealEvent> {
	Path fileP;
	OpenOption[] options;

	public JournalToFileEventHandler() {
		String filePath = "/home/ubuntu/lmax/journal" + this.hashCode();
		fileP = Paths.get(filePath);
		options = new OpenOption[] { WRITE, CREATE, APPEND };
	}

	public void onEvent(DealEvent event, long sequence, boolean endOfBatch) {
		String JSON = event.getDealJSON();
		String msg = event.getDealMsg();
		// Convert the JSON to a deal entity instance. In this case - take a
		// short cut
		this.journalToFile("Sequence: " + sequence + " JSON: " + JSON + " Msg: " + msg + "\n");

		if (sequence == EventMain.producerCount) {
			System.out.println("JournalToFileEventHandler Consumer complete: " + EventMain.producerCount);
	        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
	        Date dateEnd = new Date();
	        System.out.println(dateFormat.format(dateEnd)); //2014/08/06 15:59:48
		}
	}

	/** TODO: can we have multiple file journalers?
	 * 
	 */
	/**
	 * Write a small string to a File - Use a FileWriter
	 */
	private void journalToFile(String content) {
		try {
			Files.write(fileP, content.getBytes("utf-8"), options);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}