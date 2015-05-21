package com.disrupt;

import com.lmax.disruptor.EventFactory;

public class StringEventFactory implements EventFactory<StringEvent>
{
    public StringEvent newInstance()
    {
        return new StringEvent();
    }
}