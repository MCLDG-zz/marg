package com.disrupt;

import java.nio.ByteBuffer;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.EventTranslatorOneArg;

public class StringEventProducerWithTranslator
{
    private final RingBuffer<StringEvent> ringBuffer;

    public StringEventProducerWithTranslator(RingBuffer<StringEvent> ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }

    private static final EventTranslatorOneArg<StringEvent, String> TRANSLATOR =
        new EventTranslatorOneArg<StringEvent, String>()
        {
            public void translateTo(StringEvent event, long sequence, String s)
            {
                event.set(s);
            }
        };

    public void onData(String s)
    {
        ringBuffer.publishEvent(TRANSLATOR, s);
    }
}