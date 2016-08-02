package com.ery.estorm.client.push.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import com.ery.base.support.utils.Convert;

public class ObjectPartitioner implements Partitioner {

	public ObjectPartitioner(VerifiableProperties verifiableProperties) {
	}

	@Override
	public int partition(Object fieldVal, int i) {
		if (fieldVal instanceof Integer) {
			return (int) ((Integer) fieldVal % i);
		} else if (fieldVal instanceof Long) {
			return (int) ((Long) fieldVal % i);
		} else {
			long l = Convert.toLong(fieldVal, -1);
			if (l == -1) {
				return (int) (fieldVal.hashCode() % i);
			}
			return (int) (l % i);
		}
	}
}