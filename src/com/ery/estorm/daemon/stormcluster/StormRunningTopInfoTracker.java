package com.ery.estorm.daemon.stormcluster;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import backtype.storm.generated.Assignment;
import backtype.storm.generated.StormBase;
import clojure.lang.PersistentArrayMap;

import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.Abortable;
import com.ery.estorm.daemon.topology.TopologyInfo.TopologyName;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperListener;
import com.ery.estorm.zk.ZooKeeperWatcher;

/**
 * storms watcher.stormTopologys
 * 
 * @author hans
 * 
 */
public class StormRunningTopInfoTracker extends ZooKeeperListener {
	private static final Log LOG = LogFactory.getLog(StormRunningTopInfoTracker.class);

	// storm-name launch-time-secs status num-workers component->executors

	public StormBase stormBase;// /storm0.9/storms/sstpout-5-1389322743
	// time-secs storm-id executors port
	// backtype.storm.generated.ClusterWorkerHeartbeat
	// backtype.storm.generated.TopologySummary
	public Map<String, clojure.lang.PersistentArrayMap> summary; // /storm0.9/workerbeats/sstpout-5-1389322743/11cbb075-19e9-469e-8f4b-2db9480bf19f-6801
	// master-code-dir node->host executor->node+port executor->start-time-secs
	public backtype.storm.generated.Assignment am;// /storm0.9/assignments/sstpout-5-1389322743
	public final String stormZKStormPath;
	public final String stormZKWorkerbeatsPath;
	public final String stormZKAssignPath;
	public final String stormId;// 运行的ID
	public TopologyName runningTopName;
	Abortable abortable;

	public StormRunningTopInfoTracker(ZooKeeperWatcher watcher, Abortable abortable, TopologyName topName) {
		super(watcher);
		summary = new HashMap<String, PersistentArrayMap>();
		this.runningTopName = topName;
		stormId = topName.toString();
		stormZKStormPath = ZKUtil.joinZNode(watcher.stormBaseZNode, "storms/" + stormId);
		stormZKWorkerbeatsPath = ZKUtil.joinZNode(watcher.stormBaseZNode, "workerbeats/" + stormId);
		stormZKAssignPath = ZKUtil.joinZNode(watcher.stormBaseZNode, "assignments/" + stormId);
		// watcher.registerListener(this);
		this.abortable = abortable;
	}

	public void updateInfos() {
		nodeDataChanged(stormZKStormPath);
		nodeDataChanged(stormZKAssignPath);
		getChildren(stormZKWorkerbeatsPath);
	}

	public void getChildren(String path) {
		try {
			List<String> servers = ZKUtil.listChildrenAndWatchThem(watcher, path);
			for (String host : servers) {
				byte[] data = ZKUtil.getDataAndWatch(watcher, ZKUtil.joinZNode(path, host));
				if (data != null) {
					PersistentArrayMap map = EStormConstant.castObject(data);
					synchronized (summary) {
						summary.put(ZKUtil.getNodeName(path), map);
					}
				}
			}
		} catch (KeeperException e) {
			LOG.warn("初始化流实例主机运行监控信息" + stormId, e);
		} catch (IOException e) {
			LOG.warn("", e);
		}
	}

	public void nodeDataChanged(String path) {
		try {
			if (path.equals(stormZKStormPath)) {// TOP任务节点数据变更
				byte[] data = ZKUtil.getDataAndWatch(watcher, stormZKStormPath);
				if (data != null) {
					stormBase = StormServerManage.stormMetaSerializer.deserialize(data, StormBase.class);
					// stormBase = EStormConstant.castObject(data);
				}
			} else if (path.equals(stormZKAssignPath)) {// STORM任务分配信息变更
				byte[] data = ZKUtil.getDataAndWatch(watcher, stormZKAssignPath);
				if (data != null) {
					am = StormServerManage.stormMetaSerializer.deserialize(data, Assignment.class);
					// am = EStormConstant.castObject(data);
				}
			} else if (path.startsWith(stormZKWorkerbeatsPath + "/")) {// TOP心跳信息
				byte[] data = ZKUtil.getDataAndWatch(watcher, path);
				if (data != null) {
					PersistentArrayMap map = EStormConstant.castObject(data);
					synchronized (summary) {
						summary.put(ZKUtil.getNodeName(path), map);
					}
				}
			}
		} catch (KeeperException e) {
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void nodeCreated(String path) {
		if (path.equals(stormZKStormPath) || path.equals(stormZKAssignPath) ||
				path.startsWith(stormZKWorkerbeatsPath + "/")) {
			nodeDataChanged(path);
		}
	}

	public void nodeDeleted(String path) {
		if (path.startsWith(stormZKWorkerbeatsPath + "/")) {
			synchronized (summary) {
				summary.remove(ZKUtil.getNodeName(path));
			}
		} else if (path.equals(stormZKStormPath)) {
			LOG.info("TOP " + stormId + " 停止 ，路径被删除：" + path);
		}
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("TopName:");
		sb.append(runningTopName);

		return sb.toString();
	}
}
