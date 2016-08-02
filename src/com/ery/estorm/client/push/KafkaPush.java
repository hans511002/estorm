package com.ery.estorm.client.push;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import storm.kafka.DynamicBrokersReader;
import storm.kafka.HostPort;
import backtype.storm.Config;

import com.ery.estorm.client.node.KafkaMsgScheme;
import com.ery.estorm.client.node.StormNodeConfig;
import com.ery.estorm.client.push.PushManage.BufferData;
import com.ery.estorm.client.push.PushManage.PushConfig;
import com.ery.estorm.client.push.kafka.BytesEncoder;
import com.ery.estorm.client.push.kafka.IntegerEncoder;
import com.ery.estorm.client.push.kafka.IntegerPartitioner;
import com.ery.estorm.client.push.kafka.LongEncoder;
import com.ery.estorm.client.push.kafka.LongPartitioner;
import com.ery.estorm.client.push.kafka.StringPartitioner;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.topology.NodeInfo.MsgPO;
import com.ery.estorm.exceptions.PushException;
import com.ery.estorm.util.ToolUtil;
import com.ery.base.support.utils.Convert;

/**
 * @author hans
 * 
 */
public class KafkaPush extends PushConnection {
	private static final Log LOG = LogFactory.getLog(KafkaPush.class);

	boolean initFlag = false;
	DynamicBrokersReader dyBrokerReader;
	Producer producer;
	ProducerConfig prodConfig;
	int keyType = 0;// 0：Int 1Long 2String
	int partitions = 3, replications = 1, connTimeOut = 20000, zkSessiontimeOut = 30000;
	String topic;
	String zkQuorum;
	String zkRoot = "";
	int partitionType = 0, requiredAcks = 0;
	String fileds, producerType, compression = "none";
	int keyFiledIndex[] = null;
	boolean isNodeTmpMsg = false;
	// kafka 参数:
	// kafka.zk.quorum=hadoop01,hadoop02,hadoop03,hadoop04,hadoop5
	// kafka.zk.root=/kafka
	// kafka.zk.connect.timeout=30000
	// kafka.zk.session.timeout=20000
	// kafka.msg.topic=push_{id}
	// kafka.msg.partitions=3 #3个分区
	// kafka.msg.replication.factor=2 #2个备份
	// kafka.msg.partitions.fileds= #分区字段 一个或多个输出字段，一个时根据字段对应类型判断，多个使用字符串
	// kafka.msg.partitions.type= #分区类型 1：数字 2字符串，4：HashCode
	// kafka.msg.producer.type=async # sync producer.type
	// kafka.msg.request.required.acks=0 #可用性级别 1 0 -1 * ##
	// 0：服务端领导者接受到数据后立即返回客户端标示（此时未完全持久化到磁盘，节点死了会造成部分数据丢失）
	// * ## 1：服务领导者完全持久化到磁盘后返回客户端标示（备份者未完全备份，尚未备份的数据可能丢失）
	// * ## -1：服务端等待所有备份完全同步后返回客户端标示（完全备份，只要保证服务端任意一个isr存活，则数据不会丢失）
	// * ##三种级别性能梯度：10->5>1
	// * ##————————如果ISR列表全挂，客户端将收到异常，客户端设立失败重发机制
	Properties prop = new Properties();

	static final String kafkaZkQuorum = "kafka.zk.quorum";
	static final String kafkaZkRoot = "kafka.zk.root";
	static final String kafkaZkCtms = "kafka.zk.connect.timeout";
	static final String kafkaZkStms = "kafka.zk.session.timeout";
	static final String kafkaMsgTopic = "kafka.msg.topic";
	static final String kafkaMsgPartitions = "kafka.msg.partitions";
	static final String kafkaMsgReplications = "kafka.msg.replication.factor";
	static final String kafkaMsgPartitionFileds = "kafka.msg.partitions.fileds";
	static final String kafkaMsgPartitionType = "kafka.msg.partitions.type";
	static final String kafkaMsgProducerType = "kafka.msg.producer.type";
	static final String kafkaMsgRequestRequiredAcks = "kafka.msg.request.required.acks";
	static final String kafkaMsgCompressionCodec = "kafka.msg.compression.codec";// =gzip
																					// snappy
	static final String kafkaMsgIsNodeMsg = "kafka.msg.isnodemsg";// =gzip
																	// snappy

	public boolean parseParams() {
		String[] params = pc.ppo.PUSH_PARAM.replaceAll("\r", "").split("\n");
		for (String par : params) {
			if (par == null || par.trim().equals("")) {
				continue;
			}
			String key = par.split("=")[0];
			String val = par.substring(key.length() + 1);
			if (key.equals(kafkaZkQuorum)) {
				zkQuorum = val;
			} else if (key.equals(kafkaZkRoot)) {
				zkRoot = val;
			} else if (key.equals(kafkaMsgTopic)) {
				topic = val;
			} else if (key.equals(kafkaMsgPartitionType)) {
				partitionType = Integer.parseInt(val);
			} else if (key.equals(kafkaMsgPartitionFileds)) {
				fileds = (val);
			} else if (key.equals(kafkaMsgProducerType)) {
				producerType = val;
			} else if (key.equals(kafkaMsgRequestRequiredAcks)) {
				requiredAcks = Integer.parseInt(val);
				if (requiredAcks != 1 && requiredAcks != -1) {
					requiredAcks = 0;
				}
			} else if (key.equals(kafkaMsgPartitions)) {
				partitions = Integer.parseInt(val);
			} else if (key.equals(kafkaMsgReplications)) {
				replications = Integer.parseInt(val);
			} else if (key.equals(kafkaZkCtms)) {
				connTimeOut = Integer.parseInt(val);
			} else if (key.equals(kafkaZkStms)) {
				zkSessiontimeOut = Integer.parseInt(val);
			} else if (key.equals(kafkaMsgIsNodeMsg)) {
				isNodeTmpMsg = Boolean.parseBoolean(val);
			} else if (key.equals(kafkaMsgCompressionCodec)) {
				compression = val;
				if (!compression.equals("gzip") && !compression.equals("snappy")) {
					compression = "none";
				}
			} else {
				prop.put(key, val);
			}
		}
		if (isNodeTmpMsg) {
			zkQuorum = StormNodeConfig.conf.get(EStormConstant.ZOOKEEPER_QUORUM);
			zkRoot = StormNodeConfig.conf.get(EStormConstant.STORM_MSG_KAFKA_ROOT_ZNODE, "");
		}
		if (zkQuorum == null || zkQuorum.equals("")) {
			LOG.error("节点[" + this.pc.ppo.NODE_ID + "]订阅[" + this.pc.ppo.PUSH_ID + "]配置错误，不进行订阅推送");
			initFlag = false;
			return false;
		}
		if (topic == null || topic.equals("")) {
			LOG.error("节点[" + this.pc.ppo.NODE_ID + "]订阅[" + this.pc.ppo.PUSH_ID + "]配置错误，TOPIC不能为空，不进行订阅推送 ");
			initFlag = false;
			return false;
		}
		if (isNodeTmpMsg) {
			this.keyType = 0;
		} else {
			if (fileds == null || fileds.trim().equals("")) {
				keyType = 0;
			} else {
				String filed[] = fileds.split(",");
				keyFiledIndex = new int[filed.length];
				for (int i = 0; i < filed.length; i++) {
					Integer index = this.pc.ppo.fieldNameRel.get(filed[i]);
					if (index == null) {
						LOG.error("节点[" + this.pc.ppo.NODE_ID + "]订阅[" + this.pc.ppo.PUSH_ID +
								"]分区字段配置错误，在输出中不存在，请注意大小写，不进行订阅推送");
						initFlag = false;
						return false;
					}
					keyFiledIndex[i] = index;
				}
				if (filed.length == 1 &&
						(this.pc.ppo.nodeOutFields[keyFiledIndex[0]].filedType == 2 ||
								this.pc.ppo.nodeOutFields[keyFiledIndex[0]].filedType == 4 || this.pc.ppo.nodeOutFields[keyFiledIndex[0]].filedType == 8)) {
					keyType = 1;
				} else {
					keyType = 2;
				}
			}
			if (partitionType == 1) {
				keyType = 1;
			} else if (partitionType == 2 || partitionType == 4) {
				keyType = 2;
			} else {
				partitionType = keyType;
			}
		}
		if (dyBrokerReader != null) {
			dyBrokerReader.close();
		}
		Map conf = new HashMap();
		conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, this.zkSessiontimeOut);
		conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 10);
		conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 1000);
		dyBrokerReader = new DynamicBrokersReader(conf, zkQuorum, zkRoot + "/brokers", topic);
		dyBrokerReader.createKafkaTopic(partitions, replications, connTimeOut, zkSessiontimeOut);
		return true;
	}

	String converToString(HostPort[] hosts) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < hosts.length; i++) {
			sb.append(hosts[i].host + ":" + hosts[i].port + ",");
		}
		return sb.substring(0, sb.length() - 1);
	}

	public ProducerConfig getKafkaConfig() {
		prop.put("serializer.class", BytesEncoder.class.getCanonicalName());// "com.ery.estorm.push.kafka.BytesEncoder");
		prop.put("metadata.broker.list", converToString(this.dyBrokerReader.getAllBorkers()));
		prop.put("producer.type", producerType);
		prop.put("compression.codec", compression);
		prop.put("request.required.acks", this.requiredAcks + "");

		if (this.keyType == 0) {// 分区规则
			prop.put("key.serializer.class", IntegerEncoder.class.getCanonicalName());// "com.ery.estorm.push.KafkaPush.IntegerEncoder");
			prop.put("partitioner.class", IntegerPartitioner.class.getCanonicalName());// "com.ery.estorm.push.kafka.IntegerPartitioner");
		} else if (this.keyType == 1) {
			prop.put("key.serializer.class", LongEncoder.class.getCanonicalName());// "com.ery.estorm.push.kafka.LongEncoder");
			prop.put("partitioner.class", LongPartitioner.class.getCanonicalName());// "com.ery.estorm.push.kafka.LongPartitioner");
		} else if (this.keyType == 2) {
			prop.put("key.serializer.class", "kafka.serializer.StringEncoder");
			prop.put("partitioner.class", StringPartitioner.class.getCanonicalName());// "com.ery.estorm.push.kafka.StringPartitioner");
		}
		if (producerType.equals("async")) {
			if (this.pc.ppo.PUSH_BUFFER_SIZE > 0) {
				prop.put("batch.num.messages", this.pc.ppo.PUSH_BUFFER_SIZE + "");
			}
		} else {
			prop.put("producer.type", "sync");
		}
		return new ProducerConfig(prop);
	}

	public KafkaPush(PushConfig pc) {
		super(pc);
		open();
		msgScheme = new KafkaMsgScheme(pc.npo.nodeOutFields);
	}

	public void createTopic() {
		if (initFlag && dyBrokerReader != null)
			dyBrokerReader.createKafkaTopic(partitions, replications);
	}

	@Override
	public void open() {
		if (this.producer == null) {
			parseParams();
			if (prodConfig == null) {
				prodConfig = getKafkaConfig();
			}
			if (prodConfig != null) {
				if (keyType == 0)
					producer = new Producer<Integer, byte[]>(prodConfig);
				else if (keyType == 1)
					producer = new Producer<Long, byte[]>(prodConfig);
				else
					producer = new Producer<String, byte[]>(prodConfig);
			}
			isConnected = true;
		}
	}

	@Override
	public void close() {
		try {
			if (this.producer != null) {
				producer.close();
			}
			if (dyBrokerReader != null)
				dyBrokerReader.close();
		} catch (Exception e) {
			LOG.error("闭关Kafka订阅连接错误", e);
		} finally {
			dyBrokerReader = null;
			producer = null;
			prodConfig = null;
			isConnected = false;
		}
	}

	@Override
	public void commit() {
	}

	// 获取发送对象
	public KeyedMessage buildMessage(MsgPO po, byte[] key, byte[] content) {
		if (content == null)
			return null;
		if (key == null) {
			return new KeyedMessage<Integer, byte[]>(this.topic, content);
		} else {
			return new KeyedMessage<String, byte[]>(this.topic, Convert.toLong(key) + "", content);
		}
	}

	KafkaMsgScheme msgScheme;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void putData(List<String> keys) throws PushException {
		List<KeyedMessage> kms = new ArrayList<KeyedMessage>();
		if (isNodeTmpMsg) {
			for (String key : keys) {
				BufferData rdata = pc.bufferData.get(key);
				if (rdata != null) {
					byte[] data = msgScheme.serialize(pc.ppo.pushMsgType == 0 ? rdata.data : rdata.incData);
					if (data == null) {
						LOG.error("推送订阅数据序列化异常：key=" + key + "   Data:" +
								(pc.ppo.pushMsgType == 0 ? rdata.data : rdata.incData));
					} else {
						kms.add(new KeyedMessage<Integer, byte[]>(this.topic, key.hashCode(), data));
					}
				}

			}
		} else {
			// 判断是否有KEY
			if (this.keyType == 0) {
				for (String key : keys) {
					BufferData rdata = pc.bufferData.get(key);
					if (rdata != null) {
						byte[] data = msgScheme.serialize(pc.ppo.pushMsgType == 0 ? rdata.data : rdata.incData);
						if (data == null) {
							LOG.error("推送订阅数据序列化异常：key=" + key + "   Data:" +
									(pc.ppo.pushMsgType == 0 ? rdata.data : rdata.incData));
						} else {
							kms.add(new KeyedMessage<Integer, byte[]>(this.topic, key.hashCode(), data));
						}
					}
				}
			} else if (this.keyType == 1) {
				for (String key : keys) {
					BufferData rdata = pc.bufferData.get(key);
					if (rdata != null) {
						byte[] data = msgScheme.serialize(pc.ppo.pushMsgType == 0 ? rdata.data : rdata.incData);
						if (data == null) {
							LOG.error("推送订阅数据序列化异常：key=" + key + "   Data:" +
									(pc.ppo.pushMsgType == 0 ? rdata.data : rdata.incData));
						} else {
							Long l = null;
							if (rdata.data[this.keyFiledIndex[0]] instanceof Long) {
								l = (Long) rdata.data[this.keyFiledIndex[0]];
							} else {
								l = ToolUtil.toLong(rdata.data[this.keyFiledIndex[0]].toString());
							}
							kms.add(new KeyedMessage<Long, byte[]>(this.topic, l, data));
						}
					}
				}
			} else if (this.keyType == 1) {
				for (String key : keys) {
					BufferData rdata = pc.bufferData.get(key);
					if (rdata != null) {
						byte[] data = msgScheme.serialize(pc.ppo.pushMsgType == 0 ? rdata.data : rdata.incData);
						if (data == null) {
							LOG.error("推送订阅数据序列化异常：key=" + key + "   Data:" +
									(pc.ppo.pushMsgType == 0 ? rdata.data : rdata.incData));
						} else {
							StringBuffer sb = new StringBuffer();
							for (int j = 0; j < this.keyFiledIndex.length; j++) {
								sb.append(rdata.data[this.keyFiledIndex[j]]);
								if (j > 0)
									sb.append(',');
							}
							kms.add(new KeyedMessage<String, byte[]>(this.topic, sb.toString(), data));
						}
					}
				}
			}
		}
		try {
			producer.send(kms);
		} catch (Exception e) {
			throw new PushException(e);
		}
	}
}
