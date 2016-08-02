package com.ery.estorm.client.push.kafka;

import kafka.utils.VerifiableProperties;

public class LongPartitioner extends ObjectPartitioner {

	public LongPartitioner(VerifiableProperties verifiableProperties) {
		super(verifiableProperties);
	}

}