package com.ery.estorm.log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import com.ery.estorm.config.ConfigListenThread;
import com.ery.estorm.config.ConfigListenThread.ConfigInfo;
import com.ery.estorm.config.Configuration;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.DaemonMaster;
import com.ery.estorm.daemon.Server;
import com.ery.estorm.log.NodeLog.LogPo;
import com.ery.estorm.log.NodeLog.NodeMsgLogPo;
import com.ery.estorm.log.NodeLog.NodeSummaryLogPo;
import com.ery.estorm.log.NodeLog.PushLogPo;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperListener;
import com.ery.estorm.zk.ZooKeeperWatcher;

//日志管理及异步写入
public class LogMag {
	private static final Log LOG = LogFactory.getLog(NodeLog.class.getName());
	// private static List<LogPo> nodeLogs = new ArrayList<LogPo>();
	public static List<LogPo> nodeMsgLogs = new ArrayList<LogPo>();
	public static List<LogPo> pushLogs = new ArrayList<LogPo>();
	private static int LogFlushSize = 10000;// 10000条更新一次
	private static int LogFlushInterval = 600 * 1000;//
	static DaemonMaster master;
	static Configuration conf;
	private static boolean isRunning = false;
	public static ConfigInfo configInfo = null;
	public static Map<Long, NodeSummaryLogPo> nodeSummarys = new HashMap<Long, NodeSummaryLogPo>();

	private LogMag() {
	}

	public static void setLogFlushSize(int LogFlushSize) {
		LogMag.LogFlushSize = LogFlushSize;
	}

	public static void setLogFlushInterval(int LogFlushInterval) {
		LogMag.LogFlushInterval = LogFlushInterval;
	}

	public static void addLog(NodeMsgLogPo log) {
		synchronized (nodeMsgLogs) {
			nodeMsgLogs.add(log);
		}
	}

	public static void addLog(PushLogPo log) {
		synchronized (pushLogs) {
			pushLogs.add(log);
		}
	}

	public static void start(Configuration conf) {
		if (!isRunning) {
			LogMag.conf = conf;
			LogFlushSize = conf.getInt(EStormConstant.NODE_LOG_FLUSH_SIZE, 10000);
			LogFlushInterval = conf.getInt(EStormConstant.NODE_LOG_FLUSH_INTERVAL, 600 * 1000);
			// // LogWriter nodeLogWriter = new LogWriter(master, nodeLogs, NodeSummaryLogPo.class);
			// LogWriter nodeMsgLogWriter = new LogWriter(master, nodeMsgLogs, NodeMsgLogPo.class);
			// LogWriter nodePushLogWriter = new LogWriter(master, pushLogs, PushLogPo.class);
			// // nodeLogWriter.start();
			// nodeMsgLogWriter.start();
			// nodePushLogWriter.start();

			isRunning = true;
		}
	}

	public static class LogNodeWriter extends Thread {
		Server master;
		ZooKeeperWatcher watcher;
		long flushTime;
		public NodeSummaryTracker nodeLogTracker;
		public static ConfigListenThread configListen = null;
		public LogNodeWriter logNodeWriter = null;

		public LogNodeWriter(Server master, ZooKeeperWatcher watcher, ConfigListenThread configListen) {
			logNodeWriter = this;
			LogNodeWriter.configListen = configListen;
			this.master = master;
			this.watcher = watcher;
			nodeLogTracker = new NodeSummaryTracker(watcher);
		}

		public static void writeLog() {
			// 写日志
			if (LogNodeWriter.configListen != null) {
				synchronized (configListen.access) {
					try {
						configListen.open();
						configListen.access.beginTransaction();
						for (Long id : nodeSummarys.keySet()) {
							nodeSummarys.get(id).writeLog(configListen.access);
							if (LOG.isDebugEnabled()) {
								LOG.debug("更新节点统计信息：" + nodeSummarys.get(id));
							}
						}
						configListen.access.commit();
					} catch (Exception e) {
						configListen.close();
					}
				}
			}
		}

		public void run() {
			Thread.currentThread().setName("NodeLogWriter");
			flushTime = System.currentTimeMillis();
			while (!master.isStopped() && !master.isAborted()) {
				try {
					if (nodeSummarys.size() > 0 && flushTime <= System.currentTimeMillis() - LogFlushInterval) {
						writeLog();
						flushTime = System.currentTimeMillis();
					}
					sleep(1000);
				} catch (InterruptedException e) {
				}
			}
			isRunning = false;
		}
	}

	public static class LogWriter extends Thread {
		List<LogPo> logs;
		Class<?> logClass;
		Server master;
		long flushTime;

		public LogWriter(Server master, List<LogPo> logs, Class<?> logClass) {
			this.logClass = logClass;
			this.logs = logs;
			this.master = master;
		}

		public void run() {
			Thread.currentThread().setName("LogWriter:" + logClass.getName());
			while (!master.isStopped() && !master.isAborted()) {
				try {
					List<LogPo> tmpLogs = new ArrayList<LogPo>();
					synchronized (logs) {
						if (logs.size() > 0 && (logs.size() > LogFlushSize || flushTime <= System.currentTimeMillis() - LogFlushInterval)) {
							tmpLogs.addAll(logs);
							logs.clear();
							flushTime = System.currentTimeMillis();
						}
					}
					if (tmpLogs.size() > 0)
						NodeLog.writeLog(tmpLogs, logClass);
					sleep(1000);
				} catch (InterruptedException e) {
				}
			}
			isRunning = false;
		}
	}

	public static class NodeSummaryTracker extends ZooKeeperListener {
		String priPath;
		public Map<String, Map<Long, NodeSummaryLogPo>> hostSummarys = new HashMap<String, Map<Long, NodeSummaryLogPo>>();

		public NodeSummaryTracker(ZooKeeperWatcher watcher) {
			super(watcher);
			priPath = watcher.estormLogZNode + "/";
			watcher.registerListener(this);
			nodeChildrenChanged(watcher.estormLogZNode);
			// try {
			// ZKUtil.watchAndCheckExists(watcher, watcher.logZNode);
			// } catch (KeeperException e) {
			// e.printStackTrace();
			// } catch (IOException e) {
			// e.printStackTrace();
			// }
		}

		boolean isLog(String path) {
			return path.startsWith(priPath);
		}

		public void nodeChildrenChanged(String path) {
			if (path.equals(watcher.estormLogZNode)) {
				try {
					List<String> servers = ZKUtil.listChildrenAndWatchThem(watcher, watcher.estormLogZNode);
					for (String host : servers)
						ZKUtil.watchAndCheckExists(watcher, ZKUtil.joinZNode(watcher.estormLogZNode, host));
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		public void nodeCreated(String path) {
			if (isLog(path)) {
				handler(path);
			}
		}

		@Override
		public synchronized void nodeDeleted(String path) {
			if (isLog(path)) {
				try {
					if (ZKUtil.watchAndCheckExists(watcher, path)) {
						handler(path);
					}
				} catch (KeeperException e) {
				} catch (IOException e) {
				}
			}
		}

		private void handler(String path) {
			byte[] data = getData(path);
			try {
				if (data != null) {
					String hostName = ZKUtil.getNodeName(path);
					HashMap<Long, NodeSummaryLogPo> updateLists = EStormConstant.castObject(data);
					if (updateLists.size() > 0) {
						Map<Long, NodeSummaryLogPo> hostLogs = hostSummarys.get(hostName);
						if (hostLogs == null) {
							hostLogs = new HashMap<Long, NodeSummaryLogPo>();
							hostSummarys.put(hostName, hostLogs);
						}
						for (Long nodeId : updateLists.keySet()) {
							NodeSummaryLogPo nlogPo = updateLists.get(nodeId);
							NodeSummaryLogPo hnlog = hostLogs.get(nlogPo.NODE_ID);
							NodeSummaryLogPo nlog = nodeSummarys.get(nlogPo.NODE_ID);
							if (nlog == null) {
								synchronized (LogMag.nodeSummarys) {
									LogMag.nodeSummarys.put(nlogPo.NODE_ID, nlogPo);
								}
								nlog = nlogPo;
							} else {
								nlog.add(nlogPo);
							}
							// nlog = EStormConstant.Clone(nlog);
							if (hnlog == null) {
								synchronized (hostLogs) {
									hostLogs.put(nlogPo.NODE_ID, nlogPo);
								}
							} else {
								hnlog.add(nlogPo);
							}
						}
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public synchronized void nodeDataChanged(String path) {
			if (isLog(path)) {
				handler(path);
			}
		}

		public static void updateLog(ZooKeeperWatcher watcher, String path, HashMap<Long, NodeSummaryLogPo> updateList) throws IOException,
				KeeperException {
			byte[] data = EStormConstant.Serialize(updateList);
			ZKUtil.createSetData(watcher, path, data);
		}
	}
}
