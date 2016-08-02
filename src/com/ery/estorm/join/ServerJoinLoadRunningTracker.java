package com.ery.estorm.join;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import com.ery.estorm.ServerInfo;
import com.ery.estorm.cluster.tracker.BackMasterTracker;
import com.ery.estorm.config.Configuration;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.DaemonMaster;
import com.ery.estorm.daemon.topology.NodeInfo.JoinPO.JoinPartition;
import com.ery.estorm.util.KeyLocker;
import com.ery.estorm.util.StringUtils;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperListener;
import com.ery.estorm.zk.ZooKeeperWatcher;

/**
 * 只在主节点运行
 * 
 * @author hans
 * 
 */
public class ServerJoinLoadRunningTracker extends ZooKeeperListener {
	String priPath;
	DaemonMaster master;
	public final Map<String, List<JoinPartition>> runningJoinPartitions = new HashMap<String, List<JoinPartition>>();
	final Configuration conf;
	final JoinDataManage joinDataManage;
	public final KeyLocker<String> locker = new KeyLocker<String>();
	final BackMasterTracker backMasterTracker;
	Lock lock = null;
	Map<String, Integer> runProcess = new HashMap<String, Integer>();

	public ServerJoinLoadRunningTracker(ZooKeeperWatcher watcher, DaemonMaster master) {
		super(watcher);
		this.master = master;
		this.backMasterTracker = master.getBackMasterTracker();
		this.conf = master.getConfiguration();
		this.joinDataManage = master.getJoinDataManage();
		this.priPath = watcher.estormServerJoinStateZNode + ZKUtil.ZNODE_PATH_SEPARATOR;
		String[] serverProcess = this.conf.getStrings(EStormConstant.ESTORM_SERVER_JOINLOAD_PROCESSES);
		if (serverProcess != null) {
			for (String hostName : serverProcess) {
				String tmp[] = hostName.split(":");
				if (tmp.length == 2) {
					runProcess.put(tmp[0], StringUtils.stringToInt(tmp[1], EStormConstant.ESTORM_SERVER_JOINLOAD_PROCESSES_DEFAULT));
				}
			}
		}
	}

	public void start() {
		this.watcher.registerListener(this);
	}

	// 排除关联节点的子节点
	private boolean isServerStatusNode(String path) {
		return path.startsWith(priPath);
	}

	public void nodeDataChanged(String path) {
		nodeCreated(path);
	}

	public void nodeDeleted(String path) {
		if (!isServerStatusNode(path))
			return;
		String hostName = ZKUtil.getNodeName(path);
		synchronized (runningJoinPartitions) {
			this.runningJoinPartitions.remove(hostName);
		}
	}

	public Lock getLock() {
		return locker.acquireLock("running");
	}

	public void nodeCreated(String path) {
		if (!isServerStatusNode(path))
			return;
		try {
			byte[] data = ZKUtil.getDataAndWatch(watcher, path);
			if (data != null) {
				List<JoinPartition> runningJpt = EStormConstant.castObject(data);
				String hostName = ZKUtil.getNodeName(path);
				synchronized (runningJoinPartitions) {
					this.runningJoinPartitions.put(hostName, runningJpt);
					runningJoinPartitions.notifyAll();
				}
				List<ServerInfo> onlineServers = this.backMasterTracker.getOnlineServers();
				if (onlineServers.size() <= 2) {// 只有一二台备机，主机也加入数据加载
					onlineServers.add(master.getServerName());
				}
				boolean FullLoad = true;
				for (ServerInfo sn : onlineServers) {// 判断是否加载运行曹占用完
					Integer rnum = runProcess.get(sn.hostName);
					if (rnum == null)
						rnum = EStormConstant.ESTORM_SERVER_JOINLOAD_PROCESSES_DEFAULT;
					if (!this.runningJoinPartitions.containsKey(sn.hostName)
							|| this.runningJoinPartitions.get(sn.hostName).size() < rnum) {
						FullLoad = false;
					}
				}
				if (FullLoad && lock == null) {
					lock = locker.acquireLock("running");
				} else if (!FullLoad && lock != null) {
					lock.unlock();
				}
			}
		} catch (Exception e) {
			master.abort("Unexpected exception handling nodeCreated event", e);
		}
	}
}
