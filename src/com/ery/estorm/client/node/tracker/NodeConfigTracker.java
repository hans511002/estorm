package com.ery.estorm.client.node.tracker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.zookeeper.KeeperException;

import com.ery.estorm.client.node.StormNodeProcess;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.topology.NodeInfo;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperListener;
import com.ery.estorm.zk.ZooKeeperWatcher;

public class NodeConfigTracker extends ZooKeeperListener {
	// 多处理任务task
	Map<String, List<StormNodeProcess>> spNodes = new HashMap<String, List<StormNodeProcess>>();
	Map<String, Long> nodeRels = new HashMap<String, Long>();
	public Map<Long, NodeInfo> onlineNodes = new HashMap<Long, NodeInfo>();

	public NodeConfigTracker(ZooKeeperWatcher zkw) {
		super(zkw);
		zkw.registerListener(this);
		nodeChildrenChanged(zkw.estormNodeZNode);
		// ZKUtil.watchAndCheckExists(zkw, zkw.nodeZNode);// 监听节点
	}

	@Override
	public void nodeChildrenChanged(String path) {
		if (path.equals(this.watcher.estormNodeZNode)) {
			try {
				List<String> servers = ZKUtil.listChildrenAndWatchThem(watcher, watcher.estormNodeZNode);
				add(servers);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void add(final List<String> servers) throws IOException, KeeperException {
		synchronized (this.onlineNodes) {
			for (String n : servers) {
				String nodeName = ZKUtil.getNodeName(n);
				if (!this.onlineNodes.containsKey(nodeName)) {
					String nodePath = ZKUtil.joinZNode(watcher.estormNodeZNode, n);
					byte[] data = getData(nodePath);
					if (data != null) {
						NodeInfo node = EStormConstant.castObject(data);
						this.onlineNodes.put(node.getNodeId(), node);
						nodeRels.put(nodeName, node.getNodeId());
					}
				}
			}
		}
	}

	public void register(String path, StormNodeProcess spoutNode) {
		synchronized (spNodes) {
			List<StormNodeProcess> sps = spNodes.get(path);
			if (sps == null) {
				sps = new ArrayList<StormNodeProcess>();
				spNodes.put(path, sps);
			}
			if (!sps.contains(spoutNode)) {
				try {
					ZKUtil.watchAndCheckExists(this.watcher, path);// 监听节点
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				sps.add(spoutNode);
			}
		}
	}

	@Override
	public synchronized void nodeDeleted(String path) {
		if (path.startsWith(watcher.estormNodeZNode + "/")) {
			try {
				String nodeName = ZKUtil.getNodeName(path);
				if (ZKUtil.watchAndCheckExists(watcher, path)) {
					handler(path);
				} else {
					this.onlineNodes.remove(nodeRels.remove(nodeName));
				}
			} catch (KeeperException e) {
			} catch (IOException e) {
			}
		}
	}

	@Override
	public synchronized void nodeDataChanged(String path) {
		if (path.startsWith(watcher.estormNodeZNode + "/")) {
			try {
				handler(path);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void handler(String path) throws IOException {
		byte[] data = getData(path);
		NodeInfo node = EStormConstant.castObject(data);
		NodeInfo oldN = this.onlineNodes.put(node.getNodeId(), node);
		if (spNodes.containsKey(path)) {
			if (node != null) {
				List<StormNodeProcess> pns = spNodes.get(path);
				for (StormNodeProcess sp : pns) {
					if (sp != null)
						sp.nodeConfigChanage(oldN, node);
				}
			}
		}
	}

	// // 同一虚拟机中只能是同一个TOP的节点，不需要删除
	// public boolean remove(String path, StormNodeProcess spoutNode) {
	// boolean res = false;
	// synchronized (spNodes) {
	// List<StormNodeProcess> sps = spNodes.get(path);
	// if (sps != null) {
	// res = sps.remove(spoutNode);
	// if (sps.size() == 0) {
	// spNodes.remove(path);
	// }
	// }
	// }
	// return res;
	// }

}
