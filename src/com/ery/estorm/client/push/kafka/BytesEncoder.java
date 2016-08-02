package com.ery.estorm.client.push.kafka;

import java.io.Serializable;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

public class BytesEncoder implements Encoder<byte[]>, Serializable {

	public BytesEncoder(VerifiableProperties verifiableProperties) {
	}

	@Override
	public byte[] toBytes(byte[] bytes) {
		return bytes;
	}
}