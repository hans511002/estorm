package com.ery.estorm.client.push.kafka;

import kafka.utils.VerifiableProperties;

public class StringPartitioner extends ObjectPartitioner {

	public StringPartitioner(VerifiableProperties verifiableProperties) {
		super(verifiableProperties);
	}

}