package com.ery.estorm.client.push.kafka;

import java.io.Serializable;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

import org.apache.hadoop.hbase.util.Bytes;

public class LongEncoder implements Encoder<Long>, Serializable {

	public LongEncoder(VerifiableProperties verifiableProperties) {
	}

	@Override
	public byte[] toBytes(Long l) {
		return Bytes.toBytes(l);
	}
}