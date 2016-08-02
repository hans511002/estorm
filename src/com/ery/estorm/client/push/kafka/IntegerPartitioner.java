package com.ery.estorm.client.push.kafka;

import kafka.utils.VerifiableProperties;

public class IntegerPartitioner extends ObjectPartitioner {

	public IntegerPartitioner(VerifiableProperties verifiableProperties) {
		super(verifiableProperties);
	}

}