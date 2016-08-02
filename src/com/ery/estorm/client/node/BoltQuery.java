package com.ery.estorm.client.node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.ery.estorm.config.Configuration;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.topology.NodeInfo;
import com.ery.estorm.daemon.topology.NodeInfo.FieldPO;
import com.ery.estorm.daemon.topology.NodeInfo.NodePO;
import com.ery.estorm.zk.ZKAssign;
import com.ery.estorm.zk.ZooKeeperWatcher;

public class BoltQuery extends BaseBasicBolt implements StormNodeProcess {
	private static final long serialVersionUID = -5638131391587595127L;
	private static final Log LOG = LogFactory.getLog(BoltQuery.class);

	public ZooKeeperWatcher zkw = null;// 不能序列化
	public String nodeZkPath = null;
	public NodeInfo nodeInfo = null;// 提交TOP 时初始化,
	public boolean isPause = false;// 暂停不再取消息
	public Configuration conf = null;
	public int minWaitMillis = 0;
	TopologyContext context = null;
	Map comConf;

	public BoltQuery(Configuration conf, NodeInfo node) {
		this.conf = conf;
		this.nodeInfo = node;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		comConf = stormConf;
		this.context = context;
		StormNodeConfig.init(this.conf, context.getThisWorkerPort());
		zkw = StormNodeConfig.zkw;
		nodeZkPath = ZKAssign.getNodePath(zkw, this.nodeInfo.nodeName.toString());
		minWaitMillis = this.conf.getInt(EStormConstant.STORM_STOP_MINWAIT_CONFIRM_MILLIS, 5000);
		StormNodeConfig.listen.register(nodeInfo.nodeName, this);
		StormNodeConfig.nodeConfig.register(nodeZkPath, this);
		// StormNodeConfig.addStorm(context.getStormId());
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		String srcComId = input.getSourceComponent();
		String srcStreamId = input.getSourceStreamId();

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		NodePO npo = (NodePO) nodeInfo.node;
		List<String> fields = new ArrayList<String>();
		// Fields fds=new Fields(List<String> fields)
		for (FieldPO f : npo.fields) {
			fields.add(f.FIELD_NAME);
		}
		declarer.declareStream(nodeInfo.nodeName, true, new Fields(fields));
	}

	@Override
	public void cleanup() {
		// StormNodeConfig.listen.remove(nodeInfo.nodeName, this);
		// StormNodeConfig.nodeConfig.remove(this.nodeZkPath, this);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return comConf;
	}

	@Override
	public NodeInfo getNodeInfo() {
		return this.nodeInfo;
	}

	@Override
	public int getTaskId() {
		return context.getThisTaskId();
	}

	@Override
	public void nodeConfigChanage(NodeInfo oldN, NodeInfo newN) {
		this.nodeInfo = newN;
		// 比对属性 执行命令
		// TODO Auto-generated method stub

	}

	@Override
	public boolean pause() {
		this.isPause = true;
		return true;
	}

	@Override
	public void recover() {
		this.isPause = false;
	}

	@Override
	public boolean stop() {
		this.isPause = true;
		return true;
	}

	public boolean isStoped() {
		return this.isPause;
	}
}
