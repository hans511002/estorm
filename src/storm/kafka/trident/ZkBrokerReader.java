package storm.kafka.trident;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import storm.kafka.DynamicBrokersReader;
import storm.kafka.ZkHosts;
import backtype.storm.utils.Utils;

public class ZkBrokerReader implements IBrokerReader {

	public static final Log LOG = LogFactory.getLog(ZkBrokerReader.class);

	GlobalPartitionInformation cachedBrokers;
	DynamicBrokersReader reader;
	long lastRefreshTimeMs;
	String topic;
	long refreshMillis;

	public ZkBrokerReader(Map conf, String topic, ZkHosts hosts) {
		reader = new DynamicBrokersReader(conf, hosts.brokerZkStr, hosts.brokerZkPath, topic);
		this.topic = topic;
		// cachedBrokers = reader.getBrokerInfo();
		// lastRefreshTimeMs = System.currentTimeMillis();
		refreshMillis = hosts.refreshFreqSecs * 1000L;
	}

	@Override
	public GlobalPartitionInformation getCurrentBrokers() {
		long currTime = System.currentTimeMillis();
		if (currTime > lastRefreshTimeMs + refreshMillis) {
			LOG.info("brokers need refreshing because " + refreshMillis + "ms have expired");
			while (true) {
				try {
					cachedBrokers = reader.getBrokerInfo();
					break;
				} catch (Exception e) {
					LOG.error("刷新Kafka消息[" + topic + "]管理分区错误，睡眠2秒重试", e);
					Utils.sleep(2000);
				}
			}
			lastRefreshTimeMs = currTime;
		}
		return cachedBrokers;
	}

	@Override
	public void close() {
		reader.close();
	}
}
