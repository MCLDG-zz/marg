package com.disrupt.deal;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;
 
public class SimplePartitioner implements Partitioner {
    public SimplePartitioner (VerifiableProperties props) {
 
    }
 
    public int partition(Object key, int a_numPartitions) {
        int partition = 0;
        Long longKey = (Long) key;
        if (longKey < EventMain.producerCount / 2) {
           partition = 1;
        } else {
            partition = 2;
        }
       return partition;
  }
 
}