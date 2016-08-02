package com.ery.estorm.join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.zookeeper.KeeperException;

import com.ery.estorm.ServerInfo;
import com.ery.estorm.cluster.tracker.BackMasterTracker;
import com.ery.estorm.config.Configuration;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.DaemonMaster;
import com.ery.estorm.daemon.topology.NodeInfo.JoinPO;
import com.ery.estorm.daemon.topology.NodeInfo.JoinPO.JoinPartition;
import com.ery.estorm.daemon.topology.NodeInfo.JoinPO.JoinPartition.ServerLoadState;
import com.ery.estorm.daemon.topology.NodeInfo.JoinPO.LoadState;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperListener;
import com.ery.estorm.zk.ZooKeeperWatcher;

/**
 * 只在主节点运行
 * 
 * @author hans
 * 
 */
public class JoinPartitionStatusTracker extends ZooKeeperListener {
	// String node;
	// String nodePri;
	DaemonMaster master;
	// joinId,pars
	public final Map<Long, List<JoinPartition>> joins = new HashMap<Long, List<JoinPartition>>();
	final Configuration conf;
	final Pattern pattern;
	final JoinListTracker joinListTracker;
	final BackMasterTracker backMasterTracker;
	final JoinDataManage joinDataManage;

	public JoinPartitionStatusTracker(ZooKeeperWatcher watcher, DaemonMaster master) {
		super(watcher);
		this.master = master;
		this.conf = master.getConfiguration();
		this.joinDataManage = master.getJoinDataManage();
		this.joinListTracker = joinDataManage.joinListTracker;
		this.backMasterTracker = master.getBackMasterTracker();
		// this.node = watcher.joinZNode;
		// nodePri = this.node + "/join_";
		pattern = Pattern.compile(watcher.estormJoinZNode + "/join_(\\d+)/(p_[-\\d\\w]+)$");
	}

	public void start() {
		this.watcher.registerListener(this);
	}

	// 排除关联节点的子节点
	private boolean isJoinStatusNode(String path) {
		return pattern.matcher(path).find();
	}

	private long getJoinId(String path) {
		Matcher m = pattern.matcher(path);
		if (m.find())
			return Long.parseLong(m.group(1));
		else
			return 0;
	}

	public void nodeDataChanged(String path) {
		nodeCreated(path);
	}

	public void nodeDeleted(String path) {
		if (!isJoinStatusNode(path))
			return;
		long joinId = getJoinId(path);// Long.parseLong(ZKUtil.getNodeName(path).replace("join_",
										// ""));
		synchronized (joins) {
			this.joins.remove(joinId);
		}
	}

	public void nodeCreated(String path) {
		if (!isJoinStatusNode(path))
			return;
		try {
			byte[] data = ZKUtil.getDataAndWatch(watcher, path);
			if (data != null) {
				long joinId = getJoinId(path);// Long.parseLong(ZKUtil.getNodeName(path).replace("join_",
												// ""));
				JoinPO jpo = joinListTracker.joins.get(joinId);
				JoinPartition jpt = EStormConstant.castObject(data);
				List<JoinPartition> pars = joins.get(joinId);
				synchronized (joins) {
					if (pars == null) {
						pars = new ArrayList<JoinPartition>();
						joins.put(joinId, pars);
					}
					boolean finded = false;
					for (JoinPartition _jpo : pars) {
						if (_jpo.partion.equals(jpt.partion)) {
							finded = true;
							_jpo.state = jpt.state;
							_jpo.rnum = jpt.rnum;
							_jpo.loadServer = jpt.loadServer;
						}
					}
					if (!finded) {
						pars.add(jpt);
					}
				}
				if (jpo.INLOCAL_MEMORY != 0)// 内存不做处理
					return;
				if (jpt.state == LoadState.Failed) {// 判断 是否失败，失败 分配到其它主机加载
					List<ServerInfo> onlineServers = this.backMasterTracker.getOnlineServers();
					if (onlineServers.size() <= 2) {// 只有一台备机，主机也加入数据加载
						onlineServers.add(master.getServerName());
					}
					int oss = onlineServers.size();
					int retryCount = conf.getInt(EStormConstant.ESTORM_JOIN_LOAD_RETRY_TIMES, oss);
					retryCount = retryCount > onlineServers.size() ? oss : retryCount;
					if (jpt.loadServer.size() == retryCount) {// 确认加载失败
						jpo.loadState = LoadState.Failed;// 下一周期如果规则未变化，来源没变化则不在加载
						endLoad(jpo, pars, path);
					} else {
						if (retryCount == oss - 1) {// 只剩下一个服务器未试
							for (ServerInfo sn : onlineServers) {
								if (!jpt.loadServer.containsKey(sn.hostName)) {
									String thisJoinOrderNode = ZKUtil.joinZNode(watcher.estormJoinOrderZNode,
											sn.hostName);
									jpt.state = LoadState.Retry;
									jpt.loadServer.put(sn.hostName, new ServerLoadState(LoadState.Assign));
									jpt.st = System.currentTimeMillis();
									jpt.assginToServer(watcher, thisJoinOrderNode);
									break;
								}
							}
						} else {
							while (true) {
								int index = (int) (Math.random() * oss * Math.random() * oss) % oss;
								ServerInfo sn = onlineServers.get(index);
								if (!jpt.loadServer.containsKey(sn.hostName)) {
									String thisJoinOrderNode = ZKUtil.joinZNode(watcher.estormJoinOrderZNode,
											sn.hostName);
									jpt.state = LoadState.Retry;
									jpt.st = System.currentTimeMillis();
									jpt.loadServer.put(sn.hostName, new ServerLoadState(LoadState.Assign));
									jpt.assginToServer(watcher, thisJoinOrderNode);
									break;
								}
							}
						}
					}
				} else if (jpt.state == LoadState.Success && pars.size() == jpo.partions.size()) {
					boolean jpoNormal = true;
					for (JoinPartition _jpo : pars) {
						if (_jpo.state != LoadState.Success) {
							jpoNormal = false;
							break;
						}
					}
					if (jpoNormal) {// 完成所有加载
						jpo.loadState = LoadState.Success;
						endLoad(jpo, pars, path);
					}
				}
			}
		} catch (Exception e) {
			master.abort("Unexpected exception handling nodeCreated event", e);
		}
	}

	void endLoad(JoinPO jpo, List<JoinPartition> pars, String path) throws IOException, KeeperException {
		if (jpo.partitionLoadInfo == null)
			jpo.partitionLoadInfo = new HashMap<String, JoinPartition>();
		for (JoinPartition _jpo : pars) {
			jpo.partitionLoadInfo.put(_jpo.partion, _jpo);
		}
		jpo.updateToZk(watcher); // 更新jpo状态
		String priPath = ZKUtil.getParent(path);// 删除分区Zk节点 下次加载再使用
		for (JoinPartition _jpo : pars) {
			_jpo.deleteZk(watcher, priPath);
		}
	}
}
