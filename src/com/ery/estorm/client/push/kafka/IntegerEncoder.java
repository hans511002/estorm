package com.ery.estorm.client.push.kafka;

import java.io.Serializable;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import org.apache.hadoop.hbase.util.Bytes;

public class IntegerEncoder implements Encoder<Integer>, Serializable {

	public IntegerEncoder(VerifiableProperties verifiableProperties) {
	}

	@Override
	public byte[] toBytes(Integer l) {
		return Bytes.toBytes(l);
	}
}