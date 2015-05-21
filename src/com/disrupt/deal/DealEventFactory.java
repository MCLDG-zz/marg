package com.disrupt.deal;

import com.lmax.disruptor.EventFactory;

public class DealEventFactory implements EventFactory<DealEvent>
{
    public DealEvent newInstance()
    {
        return new DealEvent();
    }
}