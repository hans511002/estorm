package com.ery.estorm.daemon.stormcluster;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import backtype.storm.serialization.GzipThriftSerializationDelegate;
import backtype.storm.utils.Utils;

import com.ery.estorm.config.Configuration;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.AssignmentManager;
import com.ery.estorm.daemon.DaemonMaster;
import com.ery.estorm.daemon.stormcluster.StormNimbusInfoTracker.StormMasterInfo;
import com.ery.estorm.daemon.stormcluster.StormSupervisorTracker.StormSupervisorInfo;
import com.ery.estorm.daemon.topology.TopologyInfo;
import com.ery.estorm.daemon.topology.TopologyInfo.TopologyName;
import com.ery.estorm.util.ToolUtil;
import com.ery.estorm.zk.ZooKeeperWatcher;

//监听启动storm集群
public class StormServerManage {
	private static final Log LOG = LogFactory.getLog(StormServerManage.class);
	public static GzipThriftSerializationDelegate stormMetaSerializer = new backtype.storm.serialization.GzipThriftSerializationDelegate();

	// Config conf;
	String stormHome;
	String stormCommand;

	// storm rebalance sstpout

	public String getStormHome() {
		return stormHome;
	}

	public String getStormCommand() {
		return stormCommand;
	}

	public static final String masterProcessName = "nimbus";
	public static final String slaveProcessName = "supervisor";
	public static final String uiName = "core";
	final String hostUser;// 运行程序的用户名
	DaemonMaster master;
	private ZooKeeperWatcher watcher;
	Configuration conf;

	StormNimbusInfoTracker nimbusTracker;// Storm主节点监控
	StormSupervisorTracker supervisorTracker;// storm运行节点 监控

	public StormRunningTopsTracker stormsTracker;

	public boolean isRunning = false;

	public final boolean isNimbus;// 是否nimbus主机
	public final boolean isSupervisor;// 是否Supervisor主机

	public class ServiceProcess {
		public String startCmd;
		public String stopCmd;
	}

	public StormMasterInfo mInfo;
	// final ServiceProcess nimbusCmd;
	// final ServiceProcess coreUiCmd;
	// final ServiceProcess supervisorCmd;

	final ServiceProcess stormCluserCmd;
	public final String nimbusHost;

	boolean firstRun = true;

	// StormMasterTracker stormMasterTracker;

	public StormServerManage(DaemonMaster master) throws IOException, KeeperException {
		this.master = master;
		watcher = master.getZooKeeper();
		conf = master.getConfiguration();
		nimbusHost = conf.get(EStormConstant.STORM_MANAGE_NIMBUS_HOST);
		if (nimbusHost == null) {
			throw new IOException("storm config error:not config [" + EStormConstant.STORM_MANAGE_NIMBUS_HOST + "] ");
		}
		stormHome = conf.get(EStormConstant.STORM_HOME, System.getenv("STORM_HOME"));
		if (stormHome == null || stormHome.equals("")) {
			throw new IOException("storm config error:not config [" + EStormConstant.STORM_HOME +
					"] and env[STORM_HOME] ");
		}
		stormCommand = stormHome + "/bin/storm";
		String binHome = stormHome + "/sbin";
		hostUser = EStormConstant.getUserName();

		supervisorTracker = new StormSupervisorTracker(watcher, master, this);
		supervisorTracker.start();

		stormsTracker = new StormRunningTopsTracker(watcher, master);
		stormCluserCmd = new ServiceProcess();
		mInfo = new StormMasterInfo();

		mInfo.hostName = nimbusHost;
		if (master.serverName.getHostname().equals(nimbusHost)) {
			isNimbus = true;
		} else {
			isNimbus = false;
		}

		// nimbusCmd = new ServiceProcess();
		// coreUiCmd = new ServiceProcess();
		// supervisorCmd = new ServiceProcess();
		// nimbusCmd.startCmd = binHome + "/storm-daemon.sh nimbus start";
		// nimbusCmd.stopCmd = binHome + "/storm-daemon.sh nimbus stop";
		// coreUiCmd.startCmd = binHome + "/storm-daemon.sh ui start";
		// coreUiCmd.stopCmd = binHome + "/storm-daemon.sh ui stop";
		// supervisorCmd.startCmd = binHome +
		// "/storm-daemon.sh supervisor start";
		// supervisorCmd.stopCmd = binHome + "/storm-daemon.sh supervisor stop";

		stormCluserCmd.startCmd = binHome + "/start-storm.sh";
		stormCluserCmd.stopCmd = binHome + "/stop-storm.sh";
		if (isNimbus) {
			nimbusTracker = new StormNimbusInfoTracker(watcher, master, this);
			nimbusTracker.start();
		}
		if (supervisorTracker.supervisorServers.containsKey(master.serverName.getHostname())) {
			isSupervisor = true;
		} else {
			isSupervisor = false;
		}
	}

	public void start() throws KeeperException, IOException {
		if (isRunning)
			return;
		stormsTracker.start();
		// 先启动配置中与Estorm主机对应的Storm主机
		LOG.info("Start Storm server manage: isNimbus=" + isNimbus + " isSupervisor=" + isSupervisor);
		LOG.info("EStormConstant.DebugType=" + EStormConstant.DebugType);
		startCluser();
		isRunning = true;
	}

	public boolean startCluser() {
		if (firstRun) {
			firstRun = false;
			Utils.sleep(1000);
		}
		int retry = 0;
		while (retry++ < 3) {
			try {
				// ToolUtil.runProcess(stormCluserCmd.stopCmd);
				// try {
				// Thread.sleep(10000);
				// } catch (InterruptedException e) {
				// }
				ToolUtil.runProcess(stormCluserCmd.startCmd, LOG);
				return true;
			} catch (IOException e) {
				e.printStackTrace();
				LOG.warn("启动" + stormCluserCmd.startCmd + "失败", e);
			}
		}
		return false;
	}

	public boolean stopCluser() {
		if (!master.isActiveMaster())
			return false;
		int retry = 0;
		while (retry++ < 3) {
			try {
				startCmd(stormCluserCmd.stopCmd);
				return true;
			} catch (IOException e) {
				LOG.warn("启动" + stormCluserCmd.startCmd + "失败", e);
			}
		}
		return false;
	}

	// /重启集群
	public boolean restartSupervisor(StormSupervisorInfo sinfo) {
		LOG.warn(" Supervisor on " + sinfo.info.get_hostname() + " was stoped,restarting ...");
		if (startCluser()) {
			AssignmentManager assign = master.getAssignmentManager();
			if (assign != null && assign.assignTops.values().size() > 0) {
				try {
					assign.rebalance(new ArrayList<TopologyInfo>(assign.assignTops.values()), 10, null, null);
				} catch (IOException e) {
					LOG.warn("启动" + sinfo.info.get_hostname() + "成功，重做平衡失败", e);
				}
			}
			return true;
		} else {
			LOG.warn("重启" + sinfo.info.get_hostname() + "失败");
			return false;
		}
	}

	private int startCmd(String cmd) throws IOException {
		Process proc = ToolUtil.runProcess(cmd, LOG);
		try {
			InputStream in = proc.getInputStream();
			java.io.DataInputStream b = new DataInputStream(in);
			String line = null;
			while ((line = b.readLine()) != null) {
				LOG.info("外部命令输出:" + line);
			}
			proc.wait(30000);
			int res = proc.exitValue();
			return res;
		} catch (InterruptedException e) {
			proc.destroy();
			throw new IOException(e);
		}

	}

	public void stop() {
		watcher.unregisterListener(nimbusTracker);
		nimbusTracker.stop();
	}

	public StormRunningTopsTracker getStormsTracker() {
		return stormsTracker;
	}

	public StormNimbusInfoTracker getNimbusTracker() {
		return nimbusTracker;
	}

	public StormSupervisorTracker getSupervisorTracker() {
		return supervisorTracker;
	}

	// =======================topology
	// process============================================
	// Syntax: [storm rebalance topology-name [-w wait-time-secs] [-n
	// new-num-workers] [-e component=parallelism]*]
	// 只是发送rebalance 命令
	public void rebalance(String topName, int waitSes, int newWorkders, Map<String, Integer> parallelism) {
		if (!stormsTracker.stormInfos.containsKey(topName))
			return;
		String rebalanceCmd = stormCommand + " rebalance " + topName;
		if (waitSes > 0) {
			rebalanceCmd += " -w " + waitSes;
		}
		if (newWorkders > 0) {
			rebalanceCmd += " -n " + newWorkders;
		}
		if (parallelism != null) {
			for (String comName : parallelism.keySet()) {
				// 判断是否存在对应的组件
				if (master.getAssignmentManager().assignTops.containsKey(topName)) {
					TopologyInfo top = master.getAssignmentManager().assignTops.get(topName);
					if (top != null) {
						if (!top.containsNode(comName)) {
							break;
						}
					}
				}
				Integer parNum = parallelism.get(comName);
				rebalanceCmd += " -e " + comName + "=" + parNum;
			}
		}
		cmd(rebalanceCmd);
	}

	public void stopTopology(String topName, int waitSes) {
		if (!stormsTracker.stormInfos.containsKey(topName))
			return;
		String stopTopCmd = stormCommand + " kill " + topName + " -w " + waitSes;
		try {
			ToolUtil.runProcess(stopTopCmd, LOG);
		} catch (IOException e) {
			LOG.error("执行停止Topology的命令失败:" + stopTopCmd, e);
		}
		long waitTime = 60000;
		long l = System.currentTimeMillis();
		Utils.sleep(1000);
		while (true) {
			synchronized (stormsTracker.stormInfos) {
				if (!stormsTracker.stormInfos.containsKey(topName)) {
					break;
				}
			}
			if (System.currentTimeMillis() - l > Math.max(waitTime, 5000) + 5000) {
				try {
					ToolUtil.runProcess(stopTopCmd, LOG);
					l = System.currentTimeMillis();
				} catch (IOException e) {
					LOG.error("执行停止Topology的命令失败:" + stopTopCmd, e);
				}
			}
			Utils.sleep(100);
		}

		// 删除节点
		TopologyInfo tn = master.getAssignmentManager().assignTops.get(topName);
		if (tn == null) {
			return;
		}
		// TopologyLoad top =
		// master.getTopologyManager().onlineTopologys.get(tn);
		// String topPath = ZKAssign.getTopologyPath(this.watcher,
		// tn.toString());
		// top.topologyInfo.deleteToZk(watcher);
		// // 反向查找 TopologyName
		// try {
		// ZKUtil.deleteNode(watcher, topPath);
		// synchronized (top.nodes) {
		// for (String nn : top.nodes.keySet()) {
		// String nnPath = ZKAssign.getNodePath(this.watcher, nn);
		// ZKUtil.deleteNode(watcher, nnPath);
		// }
		// }
		// } catch (KeeperException e) {
		// LOG.error("停止Topology[" + topName + "]后删除分配管理节点失败", e);
		// } catch (IOException e) {
		// LOG.error("停止Topology[" + topName + "]后删除分配管理节点失败", e);
		// }
		l = System.currentTimeMillis();
		while (true) {
			synchronized (master.getAssignmentManager().assignTops) {
				if (master.getAssignmentManager().assignTops.containsKey(topName)) {
					if (System.currentTimeMillis() - l > 10000) {
						break;
					}
				}
			}
			if (System.currentTimeMillis() - l > 30000) {
				break;
			}
			Utils.sleep(500);
		}
	}

	public void activateTopology(TopologyInfo top) {
		activateTopology(top.getTopName().topName);
	}

	public void activateTopology(TopologyName top) {
		activateTopology(top.topName);
	}

	public void activateTopology(String topName) {
		String activateCmd = stormCommand + " activate " + topName;
		cmd(activateCmd);
	}

	public void cmd(String cmd) {
		long reTry = 0;
		while (reTry++ < 3) {
			try {
				ToolUtil.runProcess(cmd, LOG);
				break;
			} catch (IOException e) {
				LOG.error("调用系统命令失败:" + cmd, e);
			}
		}
	}

	public void deactivateTopology(TopologyInfo top) {
		deactivateTopology(top.assignTopName.topName);
	}

	public void deactivateTopology(TopologyName top) {
		deactivateTopology(top.topName);
	}

	public void deactivateTopology(String topName) {
		// 先暂停，再发送deactivate 命令
		String activateCmd = stormCommand + " deactivate " + topName;
		cmd(activateCmd);
	}
}
