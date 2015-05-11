package com.disrupt;

import com.disrupt.deal.DealEvent;
import com.disrupt.deal.DealEventFactory;
import com.disrupt.deal.DealEventProducerWithTranslator;
import com.disrupt.deal.JournalToFileEventHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class EventMain
{
    public static void main(String[] args) throws Exception
    {
        // Executor that will be used to construct new threads for consumers
        Executor executor = Executors.newCachedThreadPool();

        // The factory for the event
        DealEventFactory factory = new DealEventFactory();

        // Specify the size of the ring buffer, must be power of 2.
        int bufferSize = (int) Math.pow(2,11);

        // Construct the Disruptor
        Disruptor<DealEvent> disruptor = new Disruptor<>(factory, bufferSize, executor);
        
        EventHandler<DealEvent> eh1 = new JournalToFileEventHandler();
        //EventHandler<DealEvent> eh2 = new DealParserEventHandler();

        // Connect the handler
        disruptor.handleEventsWith(eh1);
        //disruptor.after(eh1).handleEventsWith(eh2);

        // Start the Disruptor, starts all threads running
        disruptor.start();

        // Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<DealEvent> ringBuffer = disruptor.getRingBuffer();

        DealEventProducerWithTranslator producer = new DealEventProducerWithTranslator(ringBuffer);

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date dateStart = new Date();
        System.out.println(dateFormat.format(dateStart)); //2014/08/06 15:59:48
        
        for (long l = 0; l < 10000000; l++)
        {
            //producer.onData();
            if (l % 100000 == 0)
            	System.out.println("Producer: " + l);
            //Thread.sleep(100);
        }
        Date dateEnd = new Date();
        System.out.println(dateFormat.format(dateEnd)); //2014/08/06 15:59:48
    }
}