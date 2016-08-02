package com.ery.estorm.daemon.stormcluster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import backtype.storm.utils.Utils;

import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.DaemonMaster;
import com.ery.estorm.util.ToolUtil;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperNodeTracker;
import com.ery.estorm.zk.ZooKeeperWatcher;

/**
 * 当前Storm无主节点信息，无备Master，通过进程监听
 * 
 * @author hans
 * 
 */
public class StormNimbusInfoTracker extends ZooKeeperNodeTracker {
	protected static final Log LOG = LogFactory.getLog(StormNimbusInfoTracker.class);
	byte[] oldData;
	StormMasterInfo mInfo;
	StormServerManage server;
	DaemonMaster master;
	boolean started;

	/**
	 * 当前Storm无主节点信息，无备Master，通过进程监听构造MasterInfo
	 */
	public static class StormMasterInfo implements Serializable {
		private static final long serialVersionUID = 1988136721994714358L;
		public String hostName;
		public long startTime = System.currentTimeMillis();
		public long rptTime = System.currentTimeMillis();
		public long prId = 0;
		public long uiPid = 0;
		public int thriftPort;
		public int uiPort;

	}

	public StormNimbusInfoTracker(ZooKeeperWatcher watcher, DaemonMaster master, StormServerManage stServer) {
		super(watcher, watcher.estormNimbus, master);
		this.master = master;
		server = stServer;
		started = false;
	}

	public StormMasterInfo getMasterInfo() {
		return mInfo;
	}

	public String getProcessPort(String pid) {
		try {
			String corePortCmd = "/bin/netstat -ap ";// 不支持管道 netstat
														// -ap|/bin/grep LISTEN
														// |/bin/grep
			Process proc;
			proc = ToolUtil.runProcess(corePortCmd + pid);
			BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
			String text = null;
			String portStr = null;
			while ((text = in.readLine()) != null) {
				String tmp[] = text.split(" ");
				if (tmp.length > 1 && tmp[0].equals("tcp") && text.indexOf("LISTEN") > 0 && text.indexOf(pid) > 0) {
					portStr = text;
					break;
				}
			}
			in.close();
			proc.waitFor();
			proc.destroy();
			if (portStr != null) {
				return portStr.replaceAll(".*?:(\\d+).*", "$1");
			}
		} catch (Exception e) {
			LOG.error("获取对应进程ID监听的端口号失败", e);
		}
		return null;
	}

	public synchronized void start() {
		if (started)
			return;
		super.start();
		// 启动监听进程
		if (server.isNimbus && (EStormConstant.DebugType & 1) != 1) {//
			new Thread(new Runnable() {
				public void run() {
					String chkCmd = System.getenv("JAVA_HOME") + "/bin/jps";// |grep
																			// nimbus
																			// 管道不能获取输出流
					long runTimes = 0;
					Thread.currentThread().setName("NimbusListen");
					while (true) {
						try {
							synchronized (server) {
								mInfo = server.mInfo;
								if ((runTimes++ % 1000) == 0) {
									LOG.info("调用系统命令检查Storm服务守护进程：" + chkCmd);
									if (runTimes > (1 << 62))
										runTimes = 0;
								}
								Process proc = ToolUtil.runProcess(chkCmd);
								BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
								String NimBus = null, CoreUi = null;
								String text = null;
								while ((text = in.readLine()) != null) {
									String tmp[] = text.split(" ");
									if (tmp.length > 1 && tmp[1].equals("nimbus")) {
										NimBus = text;
									} else if (tmp.length > 1 && tmp[1].equals("core")) {
										CoreUi = text;
									}
									if (NimBus != null && CoreUi != null) {
										break;
									}
								}
								in.close();
								proc.waitFor();
								proc.destroy();

								if (NimBus != null && CoreUi != null) {
									String pid = NimBus.split(" ")[0];
									long olpid = mInfo.prId;
									mInfo.prId = Long.parseLong(pid);
									if (olpid != mInfo.prId || mInfo.thriftPort == 0) {
										String port = getProcessPort(mInfo.prId + "");
										mInfo.thriftPort = Integer.parseInt(port == null ? "0" : port);
									}
									olpid = mInfo.uiPid;
									mInfo.uiPid = Long.parseLong(CoreUi.split(" ")[0]);
									if (olpid != mInfo.uiPid || mInfo.uiPort == 0) {
										String port = getProcessPort(mInfo.uiPid + "");
										mInfo.uiPort = Integer.parseInt(port == null ? "0" : port);
									}
									mInfo.rptTime = System.currentTimeMillis();
									if (!ZKUtil.watchAndCheckExists(watcher, node)) {
										ZKUtil.createEphemeralNodeAndWatch(watcher, node,
												EStormConstant.Serialize(mInfo));
									} else {
										ZKUtil.setData(watcher, node, EStormConstant.Serialize(mInfo));
									}
								} else {
									if (ZKUtil.watchAndCheckExists(watcher, node)) {
										ZKUtil.deleteNode(watcher, node);
									}
								}

							}
						} catch (IOException e) {
							LOG.warn("", e);
						} catch (KeeperException e) {
							LOG.warn("", e);
						} catch (InterruptedException e) {
							LOG.warn("", e);
						} finally {
							Utils.sleep(5000);
						}
					}
				}
			}).start();
		}
	}

	@Override
	public synchronized void nodeCreated(String path) {
		if (!path.equals(node))
			return;
		try {
			byte[] data = ZKUtil.getDataAndWatch(watcher, node);
			if (data != null) {
				this.data = data;
				try {
					mInfo = EStormConstant.castObject(data);
				} catch (IOException e) {
					e.printStackTrace();
				}
				notifyAll();
			} else {
				nodeDeleted(path);
			}
		} catch (KeeperException e) {
			abortable.abort("Unexpected exception handling nodeCreated event", e);
		} catch (IOException e) {
			abortable.abort("Unexpected exception handling nodeCreated event", e);
		}
	}

	@Override
	public synchronized void nodeDeleted(String path) {
		if (path.equals(node)) {
			try {
				if (ZKUtil.watchAndCheckExists(watcher, node)) {
					nodeCreated(path);
				} else {
					this.data = null;
					server.startCluser();
				}
			} catch (KeeperException e) {
				abortable.abort("Unexpected exception handling nodeDeleted event", e);
			} catch (IOException e) {
				abortable.abort("Unexpected exception handling nodeCreated event", e);
			}
		}
	}
}
