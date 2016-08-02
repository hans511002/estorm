package storm.kafka;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.spout.MultiScheme;

public class KafkaConfig implements Serializable {

	public final BrokerHosts hosts;
	public final String topic;
	public final String clientId;

	public int fetchSizeBytes = 1024 * 1024;
	public int socketTimeoutMs = 10000;
	public int bufferSizeBytes = 1024 * 1024;
	/**
	 * KafkaMsgScheme
	 */
	public MultiScheme scheme;
	public boolean forceFromStart = false;
	public long startOffsetTime = kafka.api.OffsetRequest.EarliestTime();

	// public KafkaConfig(BrokerHosts hosts, String topic) {
	// this(hosts, topic, kafka.api.OffsetRequest.DefaultClientId());
	// }

	public KafkaConfig(BrokerHosts hosts, String topic, String clientId) {
		this.hosts = hosts;
		this.topic = topic;
		this.clientId = clientId;
	}

	public void forceStartOffsetTime(long millis) {
		startOffsetTime = millis;
		forceFromStart = true;
	}

	public static List<HostPort> convertHosts(List<String> hosts) {
		List<HostPort> ret = new ArrayList<HostPort>();
		for (String s : hosts) {
			HostPort hp;
			String[] spec = s.split(":");
			if (spec.length == 1) {
				hp = new HostPort(spec[0]);
			} else if (spec.length == 2) {
				hp = new HostPort(spec[0], Integer.parseInt(spec[1]));
			} else {
				throw new IllegalArgumentException("Invalid host specification: " + s);
			}
			ret.add(hp);
		}
		return ret;
	}
}
