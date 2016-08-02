package com.ery.estorm.daemon.topology;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;

import backtype.storm.generated.Bolt;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;

import com.ery.estorm.client.node.BoltNode;
import com.ery.estorm.client.node.SpoutNode;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.topology.NodeInfo.NodeInput;
import com.ery.estorm.daemon.topology.NodeInfo.NodePO;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperWatcher;

public class TopologyInfo implements Serializable {
	private static final long serialVersionUID = 3872744126119962232L;

	public static class TopologyName implements Serializable {
		private static final long serialVersionUID = -9213794683303970659L;
		public String topName;
		public long stime;
		public int batchID = 1;

		public TopologyName(String str) {
			topName = str;
			stime = System.currentTimeMillis();
		}

		public TopologyName(String str, long l) {
			topName = str;
			stime = l;
		}

		@Override
		public boolean equals(Object other) {
			if (other == null)
				return false;
			if (other instanceof TopologyName) {
				TopologyName tn = (TopologyName) other;
				return this.stime == tn.stime && this.topName.equals(tn.topName);
			} else {
				return false;
			}

		}

		@Override
		public int hashCode() {
			return toString().hashCode();
		}

		public static TopologyName parseServerName(final String str) {
			String tmp[] = str.split("-");
			TopologyName tn = new TopologyName(tmp[0]);
			if (tmp.length > 1)
				tn.batchID = Integer.parseInt(tmp[1]);
			if (tmp.length > 2)
				tn.stime = Long.parseLong(tmp[2]);
			return tn;
		}

		public String toString() {
			return topName + "-" + batchID + "-" + stime;
		}
	}

	public StormTopology st = null;
	public int workerNum = 1;
	// public Config topConf = new Config();
	// public List<NodeInfo> spouts;//
	// public List<NodeInfo> bolts;

	public volatile TopologyName assignTopName;// estorm分配的名称

	public TopologyInfo() {

	}

	public TopologyInfo(StormTopology st) {
		this.st = st;

		// this.spouts = spouts;
		// this.bolts = bolts;
	}

	public TopologyName getTopName() {
		return assignTopName;
	}

	// 是否包含节点
	public boolean containsNode(String nodeName) {// nameName;// 名称前缀区分// msg
													// join node
		Map<String, SpoutSpec> spouts = st.get_spouts();
		for (String ss : spouts.keySet()) {
			if (ss.equals(nodeName)) {
				return true;
			}
		}
		Map<String, Bolt> bolts = st.get_bolts();
		for (String ss : bolts.keySet()) {
			if (ss.equals(nodeName)) {
				return true;
			}
		}
		return false;
	}

	public List<Long> getcontainsNodes() {
		List<Long> nodeIds = new ArrayList<Long>();
		Map<String, Bolt> bolts = st.get_bolts();
		for (String ss : bolts.keySet()) {
			nodeIds.add(Long.parseLong(ss.substring(5)));
		}
		return nodeIds;
	}

	// 是否依赖此节点
	public boolean leanNode(NodePO node) {
		Map<String, SpoutSpec> spouts = st.get_spouts();
		for (String ss : spouts.keySet()) {// 输入
			if (("msg_-" + node.NODE_ID).equals(ss)) {
				return true;
			}
		}
		List<NodeInfo> bolts = new ArrayList<NodeInfo>();
		deserializeNodeInfos(st, null, bolts);
		for (NodeInfo bs : bolts) {
			NodePO _node = (NodePO) bs.node;
			for (NodeInput input : _node.inputs) {
				if (input.INPUT_TYPE == 1 && input.INPUT_SRC_ID == node.NODE_ID) {
					return true;
				}
			}
		}
		return false;
	}

	public String toString() {
		List<NodeInfo> spouts = new ArrayList<NodeInfo>();
		List<NodeInfo> bolts = new ArrayList<NodeInfo>();
		deserializeNodeInfos(st, spouts, bolts);
		return "assignTopName:" + assignTopName + ",\n{spouts:" + spouts.toString().replaceAll("\n", "\t\n") +
				",bolts:" + bolts.toString().replaceAll("\n", "\t\n") + "}";
	}

	public static void deserializeNodeInfos(StormTopology st, List<NodeInfo> spouts, List<NodeInfo> bolts) {
		if (spouts != null) {
			spouts.clear();
			Map<String, SpoutSpec> _spouts = st.get_spouts();
			for (String nm : _spouts.keySet()) {
				SpoutSpec ss = _spouts.get(nm);
				ss.get_spout_object();
				SpoutNode obj = Utils.deserialize(ss.get_spout_object().buffer_for_serialized_java().array(),
						SpoutNode.class);
				SpoutNode sn = (SpoutNode) obj;
				spouts.add(sn.nodeInfo);
			}
		}
		if (bolts != null) {
			bolts.clear();
			Map<String, Bolt> _bolts = st.get_bolts();
			for (String nm : _bolts.keySet()) {
				Bolt bt = _bolts.get(nm);
				BoltNode obj = Utils.deserialize(bt.get_bolt_object().buffer_for_serialized_java().array(),
						BoltNode.class);
				BoltNode bn = (BoltNode) obj;
				bolts.add(bn.nodeInfo);
			}
		}
	}

	public void updateToZk(ZooKeeperWatcher watcher) throws IOException, KeeperException {
		String path = ZKUtil.joinZNode(watcher.estormTopologyZNode, this.assignTopName.toString());
		byte[] data = EStormConstant.Serialize(this);
		ZKUtil.createSetData(watcher, path, data);
	}

	public void deleteToZk(ZooKeeperWatcher watcher) {
		String path = ZKUtil.joinZNode(watcher.estormTopologyZNode, this.assignTopName.toString());
		try {
			ZKUtil.deleteNode(watcher, path);
		} catch (Exception e) {
		}
	}
}
