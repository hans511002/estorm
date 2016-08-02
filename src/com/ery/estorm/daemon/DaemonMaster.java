package com.ery.estorm.daemon;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import backtype.storm.utils.Utils;

import com.ery.estorm.ServerInfo;
import com.ery.estorm.cluster.tracker.BackMasterTracker;
import com.ery.estorm.cluster.tracker.ClusterStatusTracker;
import com.ery.estorm.config.Configuration;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.stormcluster.StormServerManage;
import com.ery.estorm.executor.ExecutorService;
import com.ery.estorm.http.InfoServer;
import com.ery.estorm.join.JoinDataManage;
import com.ery.estorm.log.LogMag.LogNodeWriter;
import com.ery.estorm.log.NodeLog;
import com.ery.estorm.monitor.MonitoredTask;
import com.ery.estorm.monitor.TaskMonitor;
import com.ery.estorm.socket.OrderHeader;
import com.ery.estorm.socket.SCServer;
import com.ery.estorm.util.DNS;
import com.ery.estorm.util.HasThread;
import com.ery.estorm.util.JvmPauseMonitor;
import com.ery.estorm.util.Sleeper;
import com.ery.estorm.util.StringUtils;
import com.ery.estorm.util.Strings;
import com.ery.estorm.util.VersionInfo;
import com.ery.estorm.zk.ZooKeeperListener;
import com.ery.estorm.zk.ZooKeeperWatcher;

public class DaemonMaster extends HasThread implements MasterServices, Server {
	private static final Log LOG = LogFactory.getLog(DaemonMaster.class.getName());
	public static final String MASTER = "master";

	private StormServerManage stormServer;// storm服务管理

	// Our zk client.
	private ZooKeeperWatcher zooKeeper;
	// The configuration for the Master
	private final Configuration conf;
	// Manager and zk listener for master election
	private ActiveMasterManager activeMasterManager;// 主节点注册监控
	// master address manager and watcher
	private ClusterStatusTracker clusterStatusTracker;// 集群状态监控
	// Master server tracker
	private ServerManager serverManager;
	private BackMasterTracker backMasterTracker;// 备节点监控

	private Sleeper stopSleeper = new Sleeper(100, this);
	// private TopologyTracker topTracker;// topology监控
	// private TopologyManager topologyManager;// 详情

	// private NodeTracker nodeTracker;// 任务节点监控
	// private NodeManager nodeManager;
	private AssignmentManager assignmentManager;// topology 生成及分配
	private JoinDataManage joinDataManage;// 数据关联定时加载

	private InfoServer infoServer;// http服务
	private JvmPauseMonitor pauseMonitor;

	// This flag is for stopping this Master instance. Its set when we are
	// stopping or aborting
	private volatile boolean stopped = false;
	// Set on abort -- usually failure of our zk session.
	private volatile boolean abort = false;
	// flag set after we complete initialization once active,
	// it is not private since it's used in unit tests
	volatile boolean initialized = false;

	// flag set after we complete assignMeta.
	private volatile boolean serverShutdownHandlerEnabled = false;

	// Instance of the executor service.
	ExecutorService executorService;
	// 本服务地址信息
	public final ServerInfo serverName;
	// Time stamps for when a hmaster was started and when it became active
	private long masterStartTime;// 运行开始时间
	private long masterActiveTime;// 活动开始时间，成为主Master时间

	/** time interval for emitting metrics values */
	private final int msgInterval;
	private List<ZooKeeperListener> registeredZKListenersBeforeRecovery;
	private SCServer scServer;

	public DaemonMaster(final Configuration conf) throws IOException, KeeperException, InterruptedException {
		if (EStormConstant.DebugType > 0 && (EStormConstant.DebugType & 2) != 2) {
			Utils.sleep(5000);
		}
		this.conf = new Configuration(conf);
		// Disable the block cache on the master
		// Server to handle client requests.
		String hostName = Strings
				.domainNamePointerToHostName(DNS.getDefaultHost(conf.get("estorm.master.dns.interface", "default"),
						conf.get("estorm.master.dns.nameserver", "default")));
		this.scServer = new SCServer(this);
		int port = conf.getInt(EStormConstant.MASTER_PORT_KEY, 3000);
		this.zooKeeper = new ZooKeeperWatcher(conf, MASTER + ":" + port, this, true);
		// Set our address.
		// We don't want to pass isa's hostname here since it could be 0.0.0.0
		this.serverName = new ServerInfo(hostName, port, System.currentTimeMillis());
		OrderHeader.hostName = serverName;
		// set the thread name now we have an address
		setName(MASTER + ":" + this.serverName.getHostPort());
		this.serverManager = new ServerManager(this, this.zooKeeper);

		this.scServer.startThreads();
		this.pauseMonitor = new JvmPauseMonitor(conf);
		this.pauseMonitor.start();
		// metrics interval: using the same property as region server.
		this.msgInterval = conf.getInt("estorm.server.msginterval", 3 * 1000);
		joinDataManage = new JoinDataManage(this);
		NodeLog.init(conf);
	}

	/**
	 * Try becoming active master.
	 * 
	 * @param startupStatus
	 * @return True if we could successfully become the active master.
	 * @throws InterruptedException
	 */
	private boolean becomeActiveMaster(MonitoredTask startupStatus) throws InterruptedException {
		this.activeMasterManager = new ActiveMasterManager(zooKeeper, this.serverName, this);
		this.zooKeeper.registerListenerFirst(activeMasterManager);
		// The ClusterStatusTracker is setup before the other
		// ZKBasedSystemTrackers because it's needed by the activeMasterManager
		// to check if the cluster should be shutdown.
		return this.activeMasterManager.blockUntilBecomingActiveMaster(startupStatus);
	}

	private void loop() {
		long lastMsgTs = 0l;
		long now = 0l;
		while (!this.stopped) {
			now = System.currentTimeMillis();
			if ((now - lastMsgTs) >= this.msgInterval) {
				doMetrics();
				lastMsgTs = System.currentTimeMillis();
			}
			stopSleeper.sleep();
		}
	}

	/**
	 * Emit the HMaster metrics, such as region in transition metrics.
	 * Surrounding in a try block just to be sure metrics doesn't abort HMaster.
	 */
	private void doMetrics() {
		try {
			// this.assignmentManager.updateNodeMetrics();
		} catch (Throwable e) {
			LOG.error("Couldn't update metrics: " + e.getMessage());
		}
	}

	@Override
	public void run() {
		MonitoredTask startupStatus = TaskMonitor.get().createStatus("Master startup");
		startupStatus.setDescription("Master startup on " + this.serverName.getHostPort());
		masterStartTime = System.currentTimeMillis();
		try {
			// Put up info server.
			int port = this.conf.getInt(EStormConstant.MASTER_INFO_PORT_KEY, 30010);
			if (port >= 0) {
				String a = this.conf.get(EStormConstant.MASTER_INFO_BINDADDRESS_KEY, "0.0.0.0");
				this.infoServer = new InfoServer(MASTER, a, port, false, this.conf);
				this.infoServer.addServlet("status", "/master-status", MasterStatusServlet.class);
				this.infoServer.addServlet("dump", "/dump", MasterDumpServlet.class);
				this.infoServer.setAttribute(MASTER, this);
				this.infoServer.start();
			}
			startupStatus.setStatus("Initializing ZK system trackers");
			initializeZKBasedSystemTrackers();
			this.registeredZKListenersBeforeRecovery = this.zooKeeper.getListeners();
			if (!this.stopped)
				finishInitialization(startupStatus, false);
			// Block on becoming the active master.
			becomeActiveMaster(startupStatus);
			// 主节点监听配置信息
			this.stormServer = new StormServerManage(this);
			// this.topTracker.start();
			// this.nodeTracker.start();
			this.stormServer.start();
			this.stormServer.startCluser();
			// master assign
			this.assignmentManager = new AssignmentManager(this);
			zooKeeper.registerListenerFirst(assignmentManager);
			this.assignmentManager.start();
			joinDataManage.start();

			// We are either the active master or we were asked to shutdown
			if (!this.stopped)
				loop();
		} catch (Throwable t) {
			abort("Unhandled exception. Starting shutdown.", t);
		} finally {
			startupStatus.cleanup();
			// Wait for all the remaining region servers to report in IFF we
			// were
			// running a cluster shutdown AND we were NOT aborting.
			if (!this.abort && this.serverManager != null && this.serverManager.isClusterShutdown()) {
				this.serverManager.expireServers();
			}
			stopServiceThreads();
			// Stop services started for both backup and active masters
			if (this.activeMasterManager != null)
				this.activeMasterManager.stop();

			if (this.serverManager != null)
				this.serverManager.stop();
			if (this.assignmentManager != null)
				this.assignmentManager.stop();
			this.zooKeeper.close();
		}
		LOG.info("HMaster main thread exiting");

	}

	private void stopServiceThreads() {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Stopping service threads");
		}
		if (this.scServer != null)
			this.scServer.stop();

		if (this.infoServer != null) {
			LOG.info("Stopping infoServer");
			try {
				this.infoServer.stop();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			LOG.info("Stoped infoServer");
		}
		if (this.executorService != null)
			this.executorService.shutdown();
		if (this.pauseMonitor != null) {
			this.pauseMonitor.stop();
		}
	}

	void initializeZKBasedSystemTrackers() throws IOException, InterruptedException, KeeperException {
		// this.topTracker = new TopologyTracker(this.zooKeeper, this);
		// nodeManager = new NodeManager(this, zooKeeper);
		// this.nodeTracker = new NodeTracker(zooKeeper, this, nodeManager);

		this.backMasterTracker = new BackMasterTracker(this.zooKeeper, this, this.serverManager);
		this.backMasterTracker.start();
		// Set the cluster as up. If new RSs, they'll be waiting on this before
		// going ahead with their startup.
		this.clusterStatusTracker = new ClusterStatusTracker(this.zooKeeper, this);
		boolean wasUp = this.clusterStatusTracker.isClusterUp();
		this.clusterStatusTracker.start();
		this.clusterStatusTracker.setClusterUp();

		LOG.info("Server active/primary master=" + this.serverName + ", sessionid=0x" +
				Long.toHexString(this.zooKeeper.getRecoverableZooKeeper().getSessionId()) +
				", setting cluster-up flag (Was=" + wasUp + ")");
	}

	/**
	 * Finish initialization of HMaster after becoming the primary master.
	 */
	private void finishInitialization(MonitoredTask status, boolean masterRecovery) throws IOException,
			InterruptedException, KeeperException {
		serverShutdownHandlerEnabled = true;

		this.masterActiveTime = System.currentTimeMillis();
		if (!masterRecovery) {
			this.executorService = new ExecutorService(this.serverName.getHostPort());
			// this.executorService.startExecutorService(ExecutorType.MASTER_OPEN_REGION,
			// conf.getInt(
			// "hbase.master.executor.openregion.threads", 5));
		}
		if (this.stopped)
			return;
		status.markComplete("Initialization successful");
		LOG.info("Master has completed initialization");
		initialized = true;
	}

	@Override
	public void abort(String why, Throwable t) {
		if (abortNow(why, t)) {
			if (t != null)
				LOG.fatal(why, t);
			else
				LOG.fatal(why);
			this.abort = true;
			stop("Aborting");
		}
	}

	private boolean abortNow(final String msg, final Throwable t) {
		if (this.stopped) {
			return true;
		}
		LogNodeWriter.writeLog();
		boolean failFast = conf.getBoolean("fail.fast.expired.active.master", false);
		if (t != null && t instanceof KeeperException.SessionExpiredException && !failFast) {
			try {
				LOG.info("Primary Master trying to recover from ZooKeeper session expiry.");
				return !tryRecoveringExpiredZKSession();
			} catch (Throwable newT) {
				LOG.error("Primary master encountered unexpected exception while "
						+ "trying to recover from ZooKeeper session" + " expiry. Proceeding with server abort.", newT);
			}
		}
		return true;
	}

	// ZK超时重启
	private boolean tryRecoveringExpiredZKSession() throws InterruptedException, IOException, KeeperException,
			ExecutionException {
		this.zooKeeper.unregisterAllListeners();
		this.zooKeeper.reconnectAfterExpiration();
		// add back listeners which were registered before master initialization
		// because they won't be added back in below Master re-initialization
		// code
		if (this.registeredZKListenersBeforeRecovery != null) {
			for (ZooKeeperListener curListener : this.registeredZKListenersBeforeRecovery) {
				this.zooKeeper.registerListener(curListener);
			}
		}
		Callable<Boolean> callable = new Callable<Boolean>() {
			@Override
			public Boolean call() throws InterruptedException, IOException, KeeperException {
				MonitoredTask status = TaskMonitor.get().createStatus("Recovering expired ZK session");
				try {
					if (!becomeActiveMaster(status)) {
						return Boolean.FALSE;
					}
					serverShutdownHandlerEnabled = false;
					initialized = false;
					finishInitialization(status, true);
					return true;
					// return !stopped;
				} finally {
					status.cleanup();
				}
			}
		};

		long timeout = conf.getLong("estorm.master.zksession.recover.timeout", 300000);
		java.util.concurrent.ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<Boolean> result = executor.submit(callable);
		executor.shutdown();
		if (executor.awaitTermination(timeout, TimeUnit.MILLISECONDS) && result.isDone()) {
			Boolean recovered = result.get();
			if (recovered != null) {
				return recovered.booleanValue();
			}
		}
		executor.shutdownNow();
		return false;
	}

	public ZooKeeperWatcher getZooKeeperWatcher() {
		return this.zooKeeper;
	}

	public boolean isActiveMaster() {
		return this.activeMasterManager.isActiveMaster();
	}

	public ActiveMasterManager getActiveMasterManager() {
		return this.activeMasterManager;
	}

	public ServerInfo getActiveMaster() {
		return this.activeMasterManager.getActiveMaster();
	}

	@Override
	public AssignmentManager getAssignmentManager() {
		return this.assignmentManager;
	}

	@Override
	public ExecutorService getExecutorService() {
		return this.executorService;
	}

	@Override
	public ServerManager getServerManager() {
		return this.serverManager;
	}

	@Override
	public boolean isInitialized() {
		return this.initialized;
	}

	@Override
	public boolean isServerShutdownHandlerEnabled() {
		return this.serverShutdownHandlerEnabled;
	}

	@Override
	public Configuration getConfiguration() {
		return this.conf;
	}

	// @Override
	// public TopologyTracker getTopTracker() {
	// return this.topTracker;
	// }

	@Override
	public ZooKeeperWatcher getZooKeeper() {
		return this.zooKeeper;
	}

	@Override
	public boolean isAborted() {
		return this.abort;
	}

	@Override
	public boolean isStopped() {
		return this.stopped;
	}

	// 停止本机服务
	@Override
	public void stop(final String why) {
		LOG.info(why);
		this.abort = true;
		this.stopped = true;
		// We wake up the stopSleeper to stop immediately
		stopSleeper.skipSleepCycle();
		// If we are a backup master, we need to interrupt wait
		if (this.activeMasterManager != null) {
			synchronized (this.activeMasterManager.clusterHasActiveMaster) {
				this.activeMasterManager.clusterHasActiveMaster.notifyAll();
			}
		}
		LogNodeWriter.writeLog();
		this.zooKeeper.close();
		LOG.info("stop estorm masters end");
	}

	// 停止集群
	public void stopMaster() {
		int retry = 0;
		while (retry++ < 10) {
			try {
				// 删除watcher.clusterStateZNode
				this.clusterStatusTracker.setClusterDown();
				break;
			} catch (KeeperException e) {
				LOG.error("stopMaster zookeeper delete clusterStatus fail", e);
				stopSleeper.sleep();
			} catch (IOException e) {
				LOG.error("stopMaster zookeeper delete clusterStatus fail", e);
				stopSleeper.sleep();
			}
		}
		LogNodeWriter.writeLog();
	}

	@Override
	public ServerInfo getServerName() {
		return this.serverName;
	}

	public long getMasterStartTime() {
		return masterStartTime;
	}

	public long getMasterActiveTime() {
		return masterActiveTime;
	}

	public ClusterStatusTracker getClusterStatusTracker() {
		return clusterStatusTracker;
	}

	public BackMasterTracker getBackMasterTracker() {
		return backMasterTracker;
	}

	// public NodeTracker getNodeTracker() {
	// return nodeTracker;
	// }
	// //
	// public NodeManager getNodeManager() {
	// return nodeManager;
	// }

	public InfoServer getInfoServer() {
		return infoServer;
	}

	public JvmPauseMonitor getPauseMonitor() {
		return pauseMonitor;
	}

	public SCServer getScServer() {
		return scServer;
	}

	public StormServerManage getStormServer() {
		return stormServer;
	}

	public JoinDataManage getJoinDataManage() {
		return joinDataManage;
	}

	public static DaemonMaster constructMaster(Class<? extends DaemonMaster> masterClass, final Configuration conf) {
		try {
			Constructor<? extends DaemonMaster> c = masterClass.getConstructor(Configuration.class);
			return c.newInstance(conf);
		} catch (InvocationTargetException ite) {
			Throwable target = ite.getTargetException() != null ? ite.getTargetException() : ite;
			if (target.getCause() != null)
				target = target.getCause();
			throw new RuntimeException("Failed construction of Master: " + masterClass.toString(), target);
		} catch (Exception e) {
			throw new RuntimeException("Failed construction of Master: " + masterClass.toString() +
					((e.getCause() != null) ? e.getCause().getMessage() : ""), e);
		}
	}

	public static void main(String[] args) throws InterruptedException {
		VersionInfo.logVersion();
		Thread.sleep(10000);
		StringUtils.startupShutdownMessage(DaemonMaster.class, args, LOG);
		new EMasterCommandLine(DaemonMaster.class).doMain(args);
	}
}
