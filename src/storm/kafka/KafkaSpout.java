package storm.kafka;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import kafka.message.Message;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import storm.kafka.trident.KafkaUtils;
import backtype.storm.Config;
import backtype.storm.metric.api.IMetric;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.utils.Utils;

// TODO: need to add blacklisting
// TODO: need to make a best effort to not re-emit messages if don't have to
public class KafkaSpout extends BaseRichSpout {
	private static final long serialVersionUID = -5550059212747681786L;

	public static class MessageAndRealOffset {
		public Message msg;
		public long offset;

		public MessageAndRealOffset(Message msg, long offset) {
			this.msg = msg;
			this.offset = offset;
		}
	}

	static enum EmitState {
		EMITTED_MORE_LEFT, EMITTED_END, NO_EMITTED
	}

	public static final Log LOG = LogFactory.getLog(KafkaSpout.class);

	String _uuid = UUID.randomUUID().toString();
	public SpoutConfig _spoutConfig;
	public SpoutOutputCollector _collector;
	public PartitionCoordinator _coordinator;
	DynamicPartitionConnections _connections;
	public ZkState _state;
	public long _lastUpdateMs = 0;
	public int _currPartitionIndex = 0;
	public boolean isPause = false;// 暂停不再取消息
	boolean hasBuffer = true;

	long llen = 0;
	long nlen = 0;
	long _ltime = 0;
	double _5slen = 100;

	public KafkaSpout(SpoutConfig spoutConf) {
		_spoutConfig = spoutConf;
	}

	@Override
	public void open(Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
		_collector = collector;

		Map<String, Object> stateConf = new HashMap<String, Object>(conf);
		_spoutConfig.reSetZK();
		List<String> zkServers = _spoutConfig.zkServers;
		if (zkServers == null)
			zkServers = (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
		Integer zkPort = _spoutConfig.zkPort;
		if (zkPort == null)
			zkPort = ((Number) conf.get(Config.STORM_ZOOKEEPER_PORT)).intValue();
		stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS, zkServers);
		stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_PORT, zkPort);
		stateConf.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, _spoutConfig.zkRoot);

		_state = new ZkState(stateConf);

		_connections = new DynamicPartitionConnections(_spoutConfig, KafkaUtils.makeBrokerReader(conf, _spoutConfig));

		// using TransactionalState like this is a hack
		int totalTasks = context.getComponentTasks(context.getThisComponentId()).size();
		if (_spoutConfig.hosts instanceof StaticHosts) {
			_coordinator = new StaticCoordinator(_connections, conf, _spoutConfig, _state, context.getThisTaskIndex(), totalTasks, _uuid);
		} else {
			_coordinator = new ZkCoordinator(_connections, conf, _spoutConfig, _state, context.getThisTaskIndex(), totalTasks, _uuid);
		}

		_5slen = _ltime = System.currentTimeMillis();

		context.registerMetric("kafkaOffset", new IMetric() {
			KafkaUtils.KafkaOffsetMetric _kafkaOffsetMetric = new KafkaUtils.KafkaOffsetMetric(_spoutConfig.topic, _connections);

			@Override
			public Object getValueAndReset() {
				List<PartitionManager> pms = _coordinator.getMyManagedPartitions();
				Set<Partition> latestPartitions = new HashSet();
				for (PartitionManager pm : pms) {
					latestPartitions.add(pm.getPartition());
				}
				_kafkaOffsetMetric.refreshPartitions(latestPartitions);
				for (PartitionManager pm : pms) {
					_kafkaOffsetMetric.setLatestEmittedOffset(pm.getPartition(), pm.lastCompletedOffset());
				}
				return _kafkaOffsetMetric.getValueAndReset();
			}
		}, 60);

		context.registerMetric("kafkaPartition", new IMetric() {
			@Override
			public Object getValueAndReset() {
				List<PartitionManager> pms = _coordinator.getMyManagedPartitions();
				Map concatMetricsDataMaps = new HashMap();
				for (PartitionManager pm : pms) {
					concatMetricsDataMaps.putAll(pm.getMetricsDataMap());
				}
				return concatMetricsDataMaps;
			}
		}, 60);
	}

	@Override
	public void close() {
		_state.close();
	}

	@Override
	public void nextTuple() {
		try {
			List<PartitionManager> managers = _coordinator.getMyManagedPartitions();
			boolean hasMsg = false;
			if (_spoutConfig.isPause) {
				if (hasBuffer == false || managers == null || managers.size() == 0)
					return;
				for (int i = 0; i < managers.size(); i++) {
					_currPartitionIndex = _currPartitionIndex % managers.size();
					PartitionManager _partition = managers.get(_currPartitionIndex);
					if (_partition._waitingToEmit.isEmpty()) {
						_currPartitionIndex = (_currPartitionIndex + 1) % managers.size();
						continue;
					}
					EmitState state = EmitState.EMITTED_MORE_LEFT;
					while (state == EmitState.EMITTED_MORE_LEFT) {// 存在缓存就提交
						hasMsg = true;
						state = managers.get(_currPartitionIndex).next(_collector);
					}
					_currPartitionIndex = (_currPartitionIndex + 1) % managers.size();
					// if (state != EmitState.NO_EMITTED) {
					// break;
					// }
				}
				hasBuffer = false;
				if (hasMsg)
					commit();
			} else {
				for (int i = 0; i < managers.size(); i++) {
					// in case the number of managers decreased
					_currPartitionIndex = _currPartitionIndex % managers.size();
					EmitState state = managers.get(_currPartitionIndex).next(_collector);
					if (state != EmitState.EMITTED_MORE_LEFT) {
						_currPartitionIndex = (_currPartitionIndex + 1) % managers.size();
					}
					// EmitState state = EmitState.EMITTED_MORE_LEFT;
					// while (state == EmitState.EMITTED_MORE_LEFT) {// 存在缓存就提交
					// state = managers.get(_currPartitionIndex).next(_collector);
					// }
					// _currPartitionIndex = (_currPartitionIndex + 1) % managers.size();
					if (state != EmitState.NO_EMITTED) {// 一定是下一轮才会返回 NO_EMITTED
						nlen++;
						hasMsg = true;
						break;
					}
				}
				long now = System.currentTimeMillis();
				if (hasMsg && (now - _lastUpdateMs) > _spoutConfig.stateUpdateIntervalMs) {
					commit();
				}
				if (now - _ltime > 5000) {
					_ltime = now;
					_5slen = (nlen - llen) / 5.0;
					llen = nlen;
					LOG.info("当前速率：" + _5slen + " 提交记录：" + nlen);

					if (_5slen < 1) {
						Utils.sleep(500);
					} else if (_5slen < 2) {
						Utils.sleep(350);
					} else if (_5slen < 3) {
						Utils.sleep(250);
					} else if (_5slen < 5) {
						Utils.sleep(150);
					} else if (_5slen < 10) {
						Utils.sleep(70);
					} else if (_5slen < 20) {
						Utils.sleep(30);
					} else if (_5slen < 50) {
						Utils.sleep(10);
					}
				}
			}
		} catch (Throwable e) {
			e.printStackTrace();
			LOG.error("", e);
			// ExceptionBolt.ExceptionPO po = SimpleUtils.toExceptionPO(e);
			// _collector.emit(SSConstant.EXCEPTION_STREAM_ID, ExceptionBolt.getExceptionValues(po));
		}
	}

	@Override
	public void ack(Object msgId) {
		PartitionManager.KafkaMessageId id = (PartitionManager.KafkaMessageId) msgId;
		PartitionManager m = _coordinator.getManager(id.partition);
		if (m != null) {
			m.ack(id.offset);
		}
	}

	@Override
	public void fail(Object msgId) {
		PartitionManager.KafkaMessageId id = (PartitionManager.KafkaMessageId) msgId;
		PartitionManager m = _coordinator.getManager(id.partition);
		if (m != null) {
			m.fail(id.offset);
		}
	}

	@Override
	public void deactivate() {
		commit();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(_spoutConfig.streamId, _spoutConfig.scheme.getOutputFields());
	}

	private void commit() {
		_lastUpdateMs = System.currentTimeMillis();
		for (PartitionManager manager : _coordinator.getMyManagedPartitions()) {
			manager.commit();
		}
	}

	// public static void main(String[] args) {
	// List<String> zkServers = new ArrayList<String>();
	// zkServers.add("192.168.10.101");
	// zkServers.add("192.168.10.102");
	// zkServers.add("192.168.10.103");
	// zkServers.add("192.168.10.104");
	// zkServers.add("192.168.10.105");
	//
	// // 初始kafka配置
	// SpoutConfig kafkaConfig = new SpoutConfig(SpoutConfig.convertZkServers(zkServers, 2181), "TEST_SYS_LOG", "/brokers",
	// "testsimplestorm");
	// kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	//
	// // 定义拓扑
	// TopologyBuilder builder = new TopologyBuilder();
	// builder.setSpout("words", new KafkaSpout(kafkaConfig), 3);
	//
	// builder.setBolt("split", new SplitSysLogBolt(2), 10).shuffleGrouping("words", SSConstant.WORK_STREAM_ID);
	// builder.setBolt("ex_split", new ExceptionBolt()).globalGrouping("words", SSConstant.EXCEPTION_STREAM_ID);
	//
	// builder.setBolt("print", new PrinterBolt()).shuffleGrouping("split", SSConstant.WORK_STREAM_ID);
	// builder.setBolt("ex_print", new ExceptionBolt()).globalGrouping("split", SSConstant.EXCEPTION_STREAM_ID);
	//
	// // 集群配置
	// Config config = new Config();
	// config.put(Config.STORM_ZOOKEEPER_SERVERS, zkServers);
	// config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
	// config.put(Config.STORM_ZOOKEEPER_ROOT, "simple_storm");
	// config.put(Config.TRANSACTIONAL_ZOOKEEPER_ROOT, "simple_tr_storm");
	//
	// LocalCluster cluster = new LocalCluster();
	// cluster.submitTopology("kafka-test", config, builder.createTopology());
	//
	// // TopologyBuilder builder = new TopologyBuilder();
	// // List<String> hosts = new ArrayList<String>();
	// // hosts.add("localhost");
	// // SpoutConfig spoutConf = SpoutConfig.fromHostStrings(hosts, 8, "clicks", "/kafkastorm", "id");
	// // spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
	// // spoutConf.forceStartOffsetTime(-2);
	// //
	// // // spoutConf.zkServers = new ArrayList<String>() {{
	// // // add("localhost");
	// // // }};
	// // // spoutConf.zkPort = 2181;
	// //
	// // builder.setSpout("spout", new KafkaSpout(spoutConf), 3);
	// //
	// // Config conf = new Config();
	// // //conf.setDebug(true);
	// //
	// // LocalCluster cluster = new LocalCluster();
	// // cluster.submitTopology("kafka-test", conf, builder.createTopology());
	// //
	// // Utils.sleep(600000);
	// }
}
