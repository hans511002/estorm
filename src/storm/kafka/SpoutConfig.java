package storm.kafka;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.ery.estorm.util.StringUtils;

public class SpoutConfig extends KafkaConfig implements Serializable {
	private static final long serialVersionUID = -9035761468622911506L;
	public List<String> zkServers = null;
	public Integer zkPort = null;
	public String zkRoot = null;
	// public String id = null;
	public long stateUpdateIntervalMs = 2000;
	public String streamId = null;
	public boolean isPause = false;// 暂停不再取消息

	public SpoutConfig(String zkQuorum, String kafkaBrokerZkPath, String topic, String zkRoot, String id) {
		super(new storm.kafka.ZkHosts(zkQuorum, kafkaBrokerZkPath), topic, id);
		this.zkRoot = zkRoot;
		// this.id = id;// msg_id spoutId
		streamId = id;
		setZk(zkQuorum);
	}

	void setZk(String zkQuorum) {
		zkServers = new ArrayList<String>();

		for (String h : zkQuorum.split(",")) {
			if (h.indexOf(':') > 0) {
				if (zkPort == null)
					zkPort = Integer.parseInt(h.substring(h.indexOf(':') + 1));
				zkServers.add(h.substring(0, h.indexOf(':')));
			} else {
				zkServers.add(h);
			}
		}
		if (zkPort == null)
			zkPort = 2181;
	}

	public void reSetZK() {
		if (hosts instanceof ZkHosts) {
			ZkHosts zh = (ZkHosts) hosts;
			setZk(zh.brokerZkStr);
		}
	}

	public static BrokerHosts convertZkServers(List<String> zkServers, int zkPort) {
		String str = StringUtils.join(":" + zkPort + ",", zkServers);
		str += ":" + zkPort;
		return new ZkHosts(str);
	}

}
