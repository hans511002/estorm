package storm.kafka;

import storm.kafka.trident.GlobalPartitionInformation;

/**
 * Date: 11/05/2013 Time: 14:43
 */
public class StaticHosts implements BrokerHosts {
	private static final long serialVersionUID = -6927792105256012827L;
	private GlobalPartitionInformation partitionInformation;

	public StaticHosts(GlobalPartitionInformation partitionInformation) {
		this.partitionInformation = partitionInformation;
	}

	public GlobalPartitionInformation getPartitionInformation() {
		return partitionInformation;
	}
}
