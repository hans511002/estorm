package com.ery.estorm.join;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import backtype.storm.utils.Utils;

import com.ery.estorm.ServerInfo;
import com.ery.estorm.cluster.tracker.BackMasterTracker;
import com.ery.estorm.config.ConfigListenThread;
import com.ery.estorm.config.Configuration;
import com.ery.estorm.config.EStormConfigRead;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.DaemonMaster;
import com.ery.estorm.daemon.topology.NodeInfo.JoinPO;
import com.ery.estorm.daemon.topology.NodeInfo.JoinPO.JoinPartition;
import com.ery.estorm.daemon.topology.NodeInfo.JoinPO.JoinPartition.ServerLoadState;
import com.ery.estorm.daemon.topology.NodeInfo.JoinPO.LoadState;
import com.ery.estorm.log.LogMag;
import com.ery.estorm.log.LogMag.LogNodeWriter;
import com.ery.estorm.util.HasThread;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperWatcher;
import com.ery.base.support.jdbc.DataAccess;

//需要并发到多台master加载数据
public class JoinDataManage extends HasThread {
	protected static final Log LOG = LogFactory.getLog(JoinDataManage.class);
	final DaemonMaster master;
	public Map<Long, JoinPO> joins = new HashMap<Long, JoinPO>();
	ConfigListenThread configListenThread = null;
	final JoinOrderTracker joinOrderTracker;
	final JoinListTracker joinListTracker;
	JoinPartitionStatusTracker StatusTracker = null;
	final ZooKeeperWatcher watcher;
	final BackMasterTracker backMasterTracker;
	ServerJoinLoadRunningTracker serverJoinLoadRunningTracker = null;
	long joinConfigListenInterval = 60000;

	public JoinDataManage(DaemonMaster master) {
		this.master = master;
		watcher = master.getZooKeeper();
		this.backMasterTracker = master.getBackMasterTracker();
		String thisJoinOrderNode = ZKUtil.joinZNode(watcher.estormJoinOrderZNode, master.serverName.getHostname());
		joinOrderTracker = new JoinOrderTracker(watcher, thisJoinOrderNode, master);
		joinOrderTracker.start();// 监听关联数据加载执行命令
		joinListTracker = new JoinListTracker(watcher, master);
		joinListTracker.setJoinDataManage(this);
		joinListTracker.start();
		// 以上为所有master运行
	}

	@Override
	public void run() {
		// 主master运行
		Thread.currentThread().setName("JoinMag");
		// 主Master监听关联数据加载状态
		StatusTracker = new JoinPartitionStatusTracker(watcher, master);
		StatusTracker.start();
		serverJoinLoadRunningTracker = new ServerJoinLoadRunningTracker(watcher, master);
		serverJoinLoadRunningTracker.start();
		joinListTracker.runListen();
		LogMag.start(master.getConfiguration());
		LogNodeWriter logWriter = new LogNodeWriter(master, watcher, configListenThread);
		logWriter.start();

		while (!master.isAborted() && !master.isStopped()) {
			if (this.configListenThread != null) {// 扫描数据库，查找对比删除的JION表
				synchronized (configListenThread.access) {
					try {
						configListenThread.open();
						List<Long> jpos = EStormConfigRead.readJoins(configListenThread.access);
						for (Long l : joinListTracker.joins.keySet()) {
							if (!jpos.contains(l)) {// delete
								JoinPO jpo = joinListTracker.joins.get(l);
								if (jpo.dataSourcePo.DATA_SOURCE_TYPE >= 10 && jpo.dataSourcePo.DATA_SOURCE_TYPE < 20)
									ZKUtil.deleteNode(watcher, watcher.estormJoinZNode + "/join_" + l);// 删除对应ZK节点
							}
						}
					} catch (Exception e) {
						configListenThread.close();
					}
				}
			}
			Utils.sleep(joinConfigListenInterval);
		}
	}

	public void setListenThread(ConfigListenThread configListenThread) {
		synchronized (configListenThread.access) {
			this.configListenThread = configListenThread;
		}
	}

	int SurplusBolt(List<ServerInfo> onlineServers) {
		int needBolt = 0;
		for (ServerInfo sn : onlineServers) {// 判断是否加载运行曹占用完
			Integer rnum = serverJoinLoadRunningTracker.runProcess.get(sn.hostName);
			if (rnum == null)
				rnum = EStormConstant.ESTORM_SERVER_JOINLOAD_PROCESSES_DEFAULT;
			List<JoinPartition> runingPro = serverJoinLoadRunningTracker.runningJoinPartitions.get(sn.hostName);
			if (runingPro == null) {
				needBolt += rnum;
			} else if (runingPro.size() < rnum) {
				needBolt += runingPro.size() - rnum;
			}
		}
		return needBolt;
	}

	ServerInfo getMinHost(List<ServerInfo> onlineServers) {
		int needBolt = 0;
		ServerInfo host = null;
		for (ServerInfo sn : onlineServers) {// 判断是否加载运行曹占用完
			Integer rnum = serverJoinLoadRunningTracker.runProcess.get(sn.hostName);
			if (rnum == null)
				rnum = EStormConstant.ESTORM_SERVER_JOINLOAD_PROCESSES_DEFAULT;
			List<JoinPartition> runingPro = serverJoinLoadRunningTracker.runningJoinPartitions.get(sn.hostName);
			if (runingPro == null) {
				if (needBolt < rnum) {
					needBolt = rnum;
					host = sn;
				}
			} else if (runingPro.size() < rnum) {
				if (needBolt < rnum - runingPro.size()) {
					needBolt = rnum - runingPro.size();
					host = sn;
				}
			}
		}
		return host;
	}

	// 解析是否需要加载等
	public void parseJoinPO(JoinPO jpo) throws IOException {
		jpo.needLoad = true;
		Configuration conf = master.getConfiguration();
		String joinDataStoreType = conf.get(EStormConstant.ESTORM_JOINDATA_STORE_TYPE).toUpperCase();
		// 判断是否与来源 相同 hbase/oracle/mysql/ob/file 不支持从直接从HDFS文件加载关联数据
		/**
		 * 11,ORACLE 12,MYSQL 13,OB 23,HBASE 34,FTP 45,HDFS file则都需要加载
		 */
		if (jpo.dataSourcePo.DATA_SOURCE_TYPE == 23 // Hbase 直接不加载数据
				||
				(jpo.dataSourcePo.DATA_SOURCE_TYPE == 13 && joinDataStoreType.equals("OB")) ||
				(jpo.dataSourcePo.DATA_SOURCE_TYPE == 11 && joinDataStoreType.equals("ORACLE")) ||
				(jpo.dataSourcePo.DATA_SOURCE_TYPE == 12 && joinDataStoreType.equals("MYSQL"))) {
			jpo.needLoad = false;
			jpo.loadState = LoadState.Success;
			return;
		}
		if (jpo.dataSourcePo.DATA_SOURCE_TYPE >= 10 && jpo.dataSourcePo.DATA_SOURCE_TYPE < 20) {// rmdbs
			jpo.partions = null;
			DataAccess joinAccess = null;
			try {
				joinAccess = EStormConfigRead.getDataAccess(jpo.dataSourcePo);
				List<String> tables = new ArrayList<String>();
				if (jpo.TABLE_SQL != null && !jpo.TABLE_SQL.trim().equals("")) {
					String sql = jpo.TABLE_SQL;
					sql = EStormConstant.macroProcess(sql);
					LOG.info("关联数据加载[" + jpo.JOIN_ID + "]--执行关联表查询SQL=" + sql);
					Object[][] tabs = joinAccess.queryForArray(sql, false, new Object[] {});
					for (Object[] tab : tabs) {
						tables.add(tab[0].toString());
					}
				} else {
					String joinTabls = jpo.TABLE_NAME;
					joinTabls = EStormConstant.macroProcess(joinTabls);
					if (joinTabls != null && !joinTabls.trim().equals("")) {
						String[] js = joinTabls.split(",");
						for (String t : js)
							tables.add(t);
					}
				}
				if (tables.size() == 0) {
					throw new IOException("关联数据[" + jpo.JOIN_ID + "]配置异常，无关联表:" + jpo);
				}
				LOG.info("关联数据加载[" + jpo.JOIN_ID + "]--关联数据表" + tables);
				jpo.partions = tables;
			} catch (Exception e) {
				throw new IOException(e);
			} finally {
				try {
					if (joinAccess != null)
						joinAccess.getConnection().close();
				} catch (SQLException e) {
					LOG.warn("获取分区列表后关闭数据库连接失败", e);
				}
			}

		}
	}

	// 分配到各主机上加载
	public void AssignJoinDataOrder(JoinPO jpo) throws IOException, KeeperException {
		// 解析 joinPO 实例化源,更新ZK节点 数据
		parseJoinPO(jpo);
		jpo.updateToZk(watcher);
		if (jpo.needLoad == false)
			return;
		if (jpo.INLOCAL_MEMORY == 0) {// 非本地内存，不需要命令分布执行
			// 根据主机数计算分布加载任务
			List<ServerInfo> onlineServers = this.backMasterTracker.getOnlineServers();
			if (onlineServers.size() <= 2) {// 只有一二台备机，主机也加入数据加载
				onlineServers.add(master.getServerName());
			}
			// 识别各主机正在加载的任务
			Lock lock = serverJoinLoadRunningTracker.getLock();
			try {
				if (jpo.partions == null || jpo.partions.size() == 0) {
					throw new IOException("关联数据[" + jpo.JOIN_ID + "]配置异常，无关联表:" + jpo);
				}
				int needBolt = jpo.partions.size();
				int assignIndex = 0;
				while (needBolt > 0) {
					ServerInfo sn = null;
					synchronized (serverJoinLoadRunningTracker.runningJoinPartitions) {
						sn = getMinHost(onlineServers);// 获取一个服务器提交任务
						serverJoinLoadRunningTracker.runningJoinPartitions.notifyAll();
					}
					if (sn == null) {
						lock.unlock();
						while (serverJoinLoadRunningTracker.lock == null) {
							Utils.sleep(100);
						}
						lock = serverJoinLoadRunningTracker.getLock();
						continue;
					}
					String serverPath = ZKUtil.joinZNode(watcher.estormJoinOrderZNode, sn.hostName);
					JoinPartition jpt = new JoinPartition();
					jpt.partion = jpo.partions.get(assignIndex++);// 分区
					jpt.st = System.currentTimeMillis();
					jpt.state = LoadState.Assign;
					jpt.joinId = jpo.JOIN_ID;
					jpt.loadServer.put(sn.hostName, new ServerLoadState(LoadState.Assign));
					jpt.assginToServer(watcher, serverPath);
					// 写入命令
					needBolt--;
					Utils.sleep(1000);
				}
			} finally {
				lock.unlock();
			}

		}
	}
}
