package storm.kafka;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.admin.AdminUtils;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.curator.framework.CuratorFramework;
import org.apache.storm.curator.framework.CuratorFrameworkFactory;
import org.apache.storm.curator.retry.RetryNTimes;
import org.apache.storm.json.simple.JSONValue;

import storm.kafka.trident.GlobalPartitionInformation;
import backtype.storm.Config;
import backtype.storm.utils.Utils;

import com.ery.estorm.zk.ZKUtil;

public class DynamicBrokersReader {

	public static final Log LOG = LogFactory.getLog(DynamicBrokersReader.class);

	// private ZkClient _curator;
	private CuratorFramework _curator;
	private String _zkQuorum;
	private String _zkPath;
	private String _topic;

	public DynamicBrokersReader(Map conf, String zkStr, String zkPath, String topic) {
		_zkQuorum = zkStr;
		_zkPath = zkPath;
		_topic = topic;
		try {
			// _curator = new ZkClient(
			// zkStr,
			// Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)),
			// Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT)),
			// new ZkSerializer() {
			// @Override
			// public byte[] serialize(Object o) throws ZkMarshallingError {
			// return o.toString().getBytes();
			// }
			//
			// @Override
			// public Object deserialize(byte[] bytes) throws ZkMarshallingError
			// {
			// return new String(bytes);
			// }
			// }
			// );
			_curator = CuratorFrameworkFactory.newClient(
					zkStr,
					Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)),
					15000,
					new RetryNTimes(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES)), Utils.getInt(conf
							.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL))));
			_curator.start();
		} catch (Exception ex) {
			LOG.error("can't connect to zookeeper");
		}
	}

	public boolean createKafkaTopic(int partitions, int replications) {
		return createKafkaTopic(partitions, replications, 30000, 20000);
	}

	public boolean createKafkaTopic(int partitions, int replications, int connTimeOut, int zkSessiontimeOut) {
		String kafkaRoot = ZKUtil.getParent(_zkPath);
		ZkClient zkClient = new ZkClient(_zkQuorum + kafkaRoot, zkSessiontimeOut, connTimeOut, new StringZkSerializer());
		String topicBrokersPath = partitionPath();
		String topicPartitionPath = topicBrokersPath.substring(kafkaRoot.length());
		if (zkClient.exists(topicPartitionPath)) {
			// 存在主题,读取主题分区信息，看是否涉及添加分区（不可删除分区，不可添加备份数）
			List<String> strings = zkClient.getChildren(topicPartitionPath);
			if (strings.size() < partitions) {// 需要添加分区
				AdminUtils.addPartitions(zkClient, _topic, partitions - strings.size(), "");
				LOG.info("消息主题[" + _topic + "]添加" + (partitions - strings.size()) + "个分区 OK!");
			} else if (strings.size() > partitions) {
				LOG.warn("消息主题[" + _topic + "]已建立分区[" + strings.size() + "],不可减少为[" + partitions + "]");
			}
			zkClient.close();
			return false;
		} else {
			// 不存在，执行命令新建
			AdminUtils.createTopic(zkClient, _topic, partitions, replications, new Properties());
			zkClient.close();
			return true;
		}

	}

	public HostPort[] getAllBorkers() {
		try {
			String brokerInfoPath = brokerPath();
			List<String> children = _curator.getChildren().forPath(brokerInfoPath);
			if (children.size() == 0)
				return null;
			HostPort[] hosts = new HostPort[children.size()];
			int len = 0;
			for (String id : children) {
				String path = brokerInfoPath + "/" + id;
				byte[] hostPortData = _curator.getData().forPath(path);
				hosts[len++] = getBrokerHost(hostPortData);
			}
			return hosts;
		} catch (Exception e) {
			LOG.error("获取Kafka Borker失败", e);
			throw new RuntimeException(e);
		}
	}

	public static class StringZkSerializer implements ZkSerializer {

		private String charset;

		public StringZkSerializer() {
		}

		public StringZkSerializer(String charset) {
			this.charset = charset;
		}

		@Override
		public byte[] serialize(Object o) throws ZkMarshallingError {
			if (charset != null && !"".equals(charset)) {
				try {
					o.toString().getBytes(charset);
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
			}
			return o.toString().getBytes();
		}

		@Override
		public Object deserialize(byte[] bytes) throws ZkMarshallingError {
			if (charset != null && !"".equals(charset)) {
				try {
					return new String(bytes, charset);
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
			}
			return new String(bytes);
		}
	}

	/**
	 * Get all partitions with their current leaders
	 */
	public GlobalPartitionInformation getBrokerInfo() {
		GlobalPartitionInformation globalPartitionInformation = new GlobalPartitionInformation();
		try {
			int numPartitionsForTopic = getNumPartitions();
			String brokerInfoPath = brokerPath();
			for (int partition = 0; partition < numPartitionsForTopic; partition++) {
				int leader = getLeaderFor(partition);
				String path = brokerInfoPath + "/" + leader;
				byte[] hostPortData = _curator.getData().forPath(path);
				// byte[] hostPortData =
				// _curator.readData(path).toString().getBytes();
				HostPort hp = getBrokerHost(hostPortData);
				globalPartitionInformation.addPartition(partition, hp);
			}
		} catch (Exception e) {
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			} else {
				throw new RuntimeException(e);
			}
		}
		LOG.info("Read partition info from zookeeper: " + globalPartitionInformation);
		return globalPartitionInformation;
	}

	private int getNumPartitions() {
		try {
			String topicBrokersPath = partitionPath();
			List<String> children = _curator.getChildren().forPath(topicBrokersPath);
			// List<String> children = _curator.getChildren(topicBrokersPath);
			return children.size();
		} catch (Exception e) {
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			} else {
				throw new RuntimeException(e);
			}
		}
	}

	public String partitionPath() {
		return _zkPath + "/topics/" + _topic + "/partitions";
	}

	public String brokerPath() {
		return _zkPath + "/ids";
	}

	/**
	 * get /brokers/topics/distributedTopic/partitions/1/state {
	 * "controller_epoch":4, "isr":[ 1, 0 ], "leader":1, "leader_epoch":1,
	 * "version":1 }
	 * 
	 * @param partition
	 * @return
	 */
	private int getLeaderFor(long partition) {
		try {
			String topicBrokersPath = partitionPath();
			byte[] hostPortData = _curator.getData().forPath(topicBrokersPath + "/" + partition + "/state");
			// byte[] hostPortData = _curator.readData(topicBrokersPath + "/" +
			// partition + "/state" ).toString().getBytes();
			Map<Object, Object> value = (Map<Object, Object>) JSONValue.parse(new String(hostPortData, "UTF-8"));
			Integer leader = ((Number) value.get("leader")).intValue();
			return leader;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void close() {
		_curator.close();
	}

	/**
	 * 
	 * [zk: localhost:2181(CONNECTED) 56] get /brokers/ids/0 {
	 * "host":"localhost", "jmx_port":9999, "port":9092, "version":1 }
	 * 
	 * @param contents
	 * @return
	 */
	private HostPort getBrokerHost(byte[] contents) {
		try {
			Map<Object, Object> value = (Map<Object, Object>) JSONValue.parse(new String(contents, "UTF-8"));
			String host = (String) value.get("host");
			Integer port = ((Long) value.get("port")).intValue();
			return new HostPort(host, port);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

}
