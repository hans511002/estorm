package com.ery.estorm.client.node;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;

import com.ery.estorm.config.Configuration;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.topology.NodeInfo;
import com.ery.estorm.daemon.topology.NodeInfo.MsgPO;
import com.ery.estorm.zk.ZKAssign;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperWatcher;
import com.ery.base.support.utils.Convert;

public class SpoutNode extends KafkaSpout implements StormNodeProcess {
	private static final long serialVersionUID = 8344461096756585675L;
	public static final Log LOG = LogFactory.getLog(SpoutNode.class);

	public ZooKeeperWatcher zkw = null;// 不能序列化

	public String nodeZkPath = null;
	public NodeInfo nodeInfo = null;// 提交TOP 时初始化,
	public Configuration conf = null;
	public int sleepMillis = 0;
	TopologyContext context = null;
	Map comConf;

	public SpoutNode(Configuration conf, SpoutConfig spoutConf, NodeInfo node) {
		super(spoutConf);
		this.conf = conf;
		this.nodeInfo = node;
	}

	public int getTaskId() {
		return context.getThisTaskId();
	}

	@Override
	public void nextTuple() {
		if (_spoutConfig.isPause)// 暂停不再取消息
			return;
		super.nextTuple();
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		super.open(conf, context, collector);
		comConf = conf;
		this.context = context;
		// StormNodeConfig.addStorm(context.getStormId());
		StormNodeConfig.init(this.conf, context.getThisWorkerPort());
		zkw = StormNodeConfig.zkw;
		nodeZkPath = ZKAssign.getNodePath(zkw, this.nodeInfo.nodeName);
		StormNodeConfig.listen.register(nodeInfo.nodeName, this);
		StormNodeConfig.nodeConfig.register(nodeZkPath, this);
	}

	@Override
	public void close() {
		super.close();
		LOG.info("SPOUT节点[" + this.nodeInfo.getNodeId() + "] 关闭");
		// StormNodeConfig.listen.remove(nodeInfo.nodeName, this);
		// StormNodeConfig.nodeConfig.remove(this.nodeZkPath, this);
	}

	@Override
	public void nodeConfigChanage(NodeInfo oldN, NodeInfo newN) {
		this.nodeInfo = newN;
		// 比对属性 执行命令

	}

	public static SpoutConfig createSpoutConfig(Configuration conf, MsgPO mpo, String spoutId) {
		String baseZNode = conf.get(EStormConstant.ZOOKEEPER_BASE_ZNODE, "/estorm");
		String kafkaMsgZNode = ZKUtil.joinZNode(baseZNode, conf.get(EStormConstant.STORM_MSG_KAFKA_ZNODE, "kafka"));
		String kafkaRootNode = conf.get(EStormConstant.STORM_MSG_KAFKA_ROOT_ZNODE, "");
		String kafkaBrokerZkPath = ZKUtil.joinZNode(kafkaRootNode, "brokers");

		SpoutConfig spoutConf = new SpoutConfig(conf.get(EStormConstant.ZOOKEEPER_QUORUM), kafkaBrokerZkPath,
				mpo.MSG_TAG_NAME, kafkaMsgZNode, spoutId);
		spoutConf.scheme = new SchemeAsMultiScheme(new KafkaMsgScheme(mpo.nodeOutFields));
		spoutConf.stateUpdateIntervalMs = conf.getInt(EStormConstant.STORM_MSG_KAFKA_STATE_UPDATE_INTERVAL, 2000);
		if (mpo.READ_OPS_PARAMS != null && !mpo.READ_OPS_PARAMS.equals("")) {
			String[] pars = mpo.READ_OPS_PARAMS.split("\n");
			for (String par : pars) {
				String tmp[] = par.split("=");
				String val = par.substring(tmp[0].length() + 1);
				if (tmp[0].equals(EStormConstant.STORM_MSG_KAFKA_STATE_UPDATE_INTERVAL)) {
					try {
						spoutConf.stateUpdateIntervalMs = Integer.parseInt(val);
					} catch (Exception e) {
						LOG.warn("特定消息[" + spoutId + "]状态时间配置错误，使用默认配置值" + spoutConf.stateUpdateIntervalMs + "，配置信息：" +
								par);
					}
					break;
				} else if (tmp[0].equals(EStormConstant.STORM_MSG_KAFKA_FORCE_START)) {
					spoutConf.forceFromStart = Convert.toBool(val, false);
				} else if (tmp[0].equals(EStormConstant.STORM_MSG_KAFKA_REFRESH_INTERVAL)) {
					ZkHosts brokerConf = (ZkHosts) spoutConf.hosts;
					brokerConf.refreshFreqSecs = Convert.toInt(val, 60);
				} else if (tmp[0].equals(EStormConstant.STORM_MSG_KAFKA_SOCKET_TIMEOUT)) {
					spoutConf.socketTimeoutMs = Convert.toInt(val, 10000);// Integer.parseInt(tmp[1]);
				}
			}
		}
		return spoutConf;
	}

	public boolean stop() {
		_spoutConfig.isPause = true;
		super.nextTuple();
		// StormNodeConfig.listen.remove(nodeInfo.nodeName, this);
		// StormNodeConfig.nodeConfig.remove(nodeZkPath, this);
		return true;
	}

	public boolean isStoped() {
		return _spoutConfig.isPause;
	}

	public boolean pause() {
		_spoutConfig.isPause = true;
		return true;
	}

	public void recover() {
		_spoutConfig.isPause = false;
		StormNodeConfig.listen.register(nodeInfo.nodeName, this);
		StormNodeConfig.nodeConfig.register(this.nodeZkPath, this);
	}

	@Override
	public NodeInfo getNodeInfo() {
		return this.nodeInfo;
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 40000);
		conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, 10);
		conf.put(Config.STORM_ZOOKEEPER_RETRY_INTERVAL, 1000);
		return conf;
	}
}
