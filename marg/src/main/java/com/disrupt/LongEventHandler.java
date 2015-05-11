package com.disrupt;

import com.lmax.disruptor.EventHandler;

public class LongEventHandler implements EventHandler<LongEvent>
{
    public void onEvent(LongEvent event, long sequence, boolean endOfBatch)
    {
    	long l = event.get();
    	try {
			Thread.sleep(4);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	if ( l % 1000 == 0) {
    		System.out.println("Consumer ID: " + this.toString() + " Consumer event: " + event + " value: " + l);
    	}
        
    }
}