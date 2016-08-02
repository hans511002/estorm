package com.ery.estorm.client.node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import storm.kafka.SpoutConfig;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

import com.ery.estorm.config.Configuration;
import com.ery.estorm.daemon.topology.NodeInfo;
import com.ery.estorm.daemon.topology.NodeInfo.FieldPO;
import com.ery.estorm.daemon.topology.NodeInfo.MsgPO;
import com.ery.estorm.zk.ZKAssign;
import com.ery.estorm.zk.ZooKeeperWatcher;

public class SpoutQuery extends BaseRichSpout implements StormNodeProcess {
	private static final long serialVersionUID = 8344461096756585675L;
	private static final Log LOG = LogFactory.getLog(SpoutQuery.class);
	public static String ARGS_STREAMID = "args";
	public static String SEQ_STREAMID = "seq";

	public ZooKeeperWatcher zkw = null;// 不能序列化

	public String nodeZkPath = null;
	public NodeInfo nodeInfo = null;// 提交TOP 时初始化, 需要构造一个NODE做查询参数
	public boolean isPause = false;// 暂停不再取消息
	public Configuration conf = null;
	TopologyContext context = null;
	Map comConf;
	SpoutOutputCollector collector;

	public SpoutQuery(Configuration conf, SpoutConfig spoutConf, NodeInfo node) {
		this.conf = conf;
		this.nodeInfo = node;
	}

	public int getTaskId() {
		return context.getThisTaskId();
	}

	@Override
	public void nextTuple() {

	}

	public SpoutOutputCollector getCollector() {
		if (isPause)// 暂停不再取消息
			return null;
		return collector;
	}

	public String getStreamId() {
		return nodeInfo.nodeName;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		comConf = conf;
		this.context = context;
		// StormNodeConfig.addStorm(context.getStormId());
		StormNodeConfig.init(this.conf, context.getThisWorkerPort());
		zkw = StormNodeConfig.zkw;
		nodeZkPath = ZKAssign.getNodePath(zkw, this.nodeInfo.nodeName.toString());
		StormNodeConfig.listen.register(nodeInfo.nodeName, this);
		StormNodeConfig.nodeConfig.register(nodeZkPath, this);
		StormNodeConfig.spoutQuery = this;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		MsgPO mpo = (MsgPO) nodeInfo.node;
		List<String> fields = new ArrayList<String>();
		// Fields fds=new Fields(List<String> fields)
		for (FieldPO f : mpo.fields) {
			fields.add(f.FIELD_NAME);
		}
		declarer.declareStream(nodeInfo.nodeName, true, new Fields(fields));
	}

	@Override
	public void close() {
		super.close();
		// StormNodeConfig.listen.remove(nodeInfo.nodeName, this);
		// StormNodeConfig.nodeConfig.remove(this.nodeZkPath, this);
	}

	@Override
	public void nodeConfigChanage(NodeInfo oldN, NodeInfo newN) {
		this.nodeInfo = newN;
		// 比对属性 执行命令

	}

	public boolean isStoped() {
		return this.isPause;
	}

	public boolean stop() {
		this.isPause = true;
		return true;
	}

	public boolean pause() {
		this.isPause = true;
		return true;
	}

	public void recover() {
		this.isPause = false;
	}

	@Override
	public NodeInfo getNodeInfo() {
		return this.nodeInfo;
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return comConf;
	}
}
