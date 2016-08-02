package com.ery.estorm.daemon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ery.estorm.ServerInfo;
import com.ery.estorm.config.Configuration;
import com.ery.estorm.exceptions.ZooKeeperConnectionException;
import com.ery.estorm.zk.ZooKeeperWatcher;

// 管理服务器，发送集群控制命令
public class ServerManager {

	private static final Log LOG = LogFactory.getLog(ServerManager.class);

	// Set if we are to shutdown the cluster.
	private volatile boolean clusterShutdown = false;

	/** Map of registered servers to their current load */
	private final Map<String, ServerInfo> masterServers = new ConcurrentHashMap<String, ServerInfo>();
	private final DaemonMaster master;
	ZooKeeperWatcher watcher;
	Configuration conf;

	/**
	 * Constructor.
	 * 
	 * @param master
	 * @param services
	 * @throws ZooKeeperConnectionException
	 */
	public ServerManager(final DaemonMaster master, ZooKeeperWatcher watcher) throws IOException {
		this.master = master;
		this.watcher = watcher;
		conf = master.getConfiguration();
		// InputStream in = ClassLoader.getSystemResourceAsStream("masters");
		// if (in != null) {
		// BufferedReader bin = new BufferedReader(new InputStreamReader(in));
		// String text = null;
		// while ((text = bin.readLine()) != null) {
		// configMasters.put(text.replaceAll("\r|\n", ""), 0l);
		// }
		// in.close();
		// configMasters.put(master.serverName.hostName,
		// master.serverName.startTime);
		// }
	}

	public void recordNewServer(ServerInfo serverName) {
		if (findServerWithSameHostnamePort(serverName) == null) {
			LOG.info("Registering server=" + serverName);
			this.masterServers.put(serverName.getHostname(), serverName);
		}
	}

	/**
	 * @return ServerName with matching hostname and port.
	 */
	private ServerInfo findServerWithSameHostnamePort(final ServerInfo serverName) {
		for (ServerInfo sn : getOnlineServers()) {
			if (ServerInfo.isSameHostnameAndPort(serverName, sn))
				return sn;
		}
		return null;
	}

	public List<ServerInfo> getDeadServers() {
		List<ServerInfo> res = new ArrayList<ServerInfo>();
		for (String host : this.masterServers.keySet()) {
			if (this.masterServers.get(host).getDeadTime() > 0)
				res.add(this.masterServers.get(host));
		}
		return res;
	}

	public List<ServerInfo> getOnlineServers() {
		List<ServerInfo> res = new ArrayList<ServerInfo>();
		for (String host : this.masterServers.keySet()) {
			if (this.masterServers.get(host).getDeadTime() == 0)
				res.add(this.masterServers.get(host));
		}

		return res;
	}

	public synchronized void expireServers() {
		for (String serverName : this.masterServers.keySet()) {
			ServerInfo info = this.masterServers.get(serverName);
			info.setDeadTime(System.currentTimeMillis());
			if (this.clusterShutdown) {
				LOG.info("Cluster shutdown set; " + serverName + " expired; onlineMasterServers=" +
						this.masterServers.size());
				if (this.masterServers.isEmpty()) {
					master.stop("Cluster shutdown set; onlineMasterServer=0");
				}
				return;
			}
			LOG.debug("Added=" + serverName + " to dead servers, submitted shutdown handler to be executed");
		}
	}

	public synchronized void expireServer(final ServerInfo serverName) {
		if (!master.isServerShutdownHandlerEnabled()) {
			LOG.info("Master doesn't enable ServerShutdownHandler during initialization, " + "delay expiring server " +
					serverName);
			ServerInfo info = this.masterServers.get(serverName);
			info.setDeadTime(System.currentTimeMillis());
			return;
		}
		if (!this.masterServers.containsKey(serverName)) {
			LOG.warn("Expiration of " + serverName + " but server not online");
		}
		ServerInfo info = this.masterServers.get(serverName);
		info.setDeadTime(System.currentTimeMillis());
		synchronized (masterServers) {
			masterServers.notifyAll();
		}
		ServerInfo msn = master.getActiveMasterManager().getActiveMaster();// 主节点
		if (msn == null || !msn.equals(serverName)) {
			LOG.debug("Added=" + serverName + " to dead servers, submitted shutdown handler to be executed");
		}
		if (this.clusterShutdown) {
			LOG.info("Cluster shutdown set; " + serverName + " expired; onlineMasterServers=" +
					this.masterServers.size());
			if (this.masterServers.isEmpty()) {
				master.stop("Cluster shutdown set; onlineMasterServer=0");
			}
			return;
		}
	}

	public boolean isMasterServerOnline(ServerInfo serverName) {
		return serverName != null &&
				((masterServers.containsKey(serverName.getHostname()) && masterServers.get(serverName.getHostname()).deadTime == 0) || (serverName
						.equals(master.getActiveMasterManager().getActiveMaster())));
	}

	public synchronized boolean isServerDead(ServerInfo serverName) {
		return serverName == null || serverName.deadTime > 0;
	}

	public void shutdownCluster() {
		this.clusterShutdown = true;
		this.master.stopMaster();
		this.master.stop("Cluster shutdown requested");
	}

	public boolean isClusterShutdown() {
		return this.clusterShutdown;
	}

	/**
	 * Stop the ServerManager. Currently closes the connection to the master.
	 */
	public void stop() {

	}

}
