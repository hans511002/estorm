/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ery.estorm.cluster.tracker;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import com.ery.estorm.ServerInfo;
import com.ery.estorm.daemon.DaemonMaster;
import com.ery.estorm.daemon.ServerManager;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperListener;
import com.ery.estorm.zk.ZooKeeperWatcher;

//监控备Master
public class BackMasterTracker extends ZooKeeperListener {
	private static final Log LOG = LogFactory.getLog(BackMasterTracker.class);
	private final Map<String, ServerInfo> masterServers = new ConcurrentHashMap<String, ServerInfo>();
	private ServerManager serverManager;
	private DaemonMaster master;

	public BackMasterTracker(ZooKeeperWatcher watcher, DaemonMaster abortable, ServerManager serverManager) {
		super(watcher);
		this.master = abortable;
		this.serverManager = serverManager;
	}

	/**
	 * Starts the tracking of online RegionServers.
	 * 
	 * <p>
	 * All RSs will be tracked after this method is called.
	 * 
	 * @throws KeeperException
	 * @throws IOException
	 */
	public void start() throws KeeperException, IOException {
		watcher.registerListener(this);
		List<String> servers = ZKUtil.listChildrenAndWatchThem(watcher, watcher.estormBackupMasterAddressesZNode);
		add(servers);
	}

	private void add(final List<String> servers) throws IOException, KeeperException {
		synchronized (this.masterServers) {
			this.masterServers.clear();
			for (String n : servers) {
				byte[] data = ZKUtil.getDataAndWatch(watcher, watcher.estormBackupMasterAddressesZNode + "/" + n);
				ServerInfo master = ServerInfo.parseFrom(data);
				this.masterServers.put(master.getHostname(), master);
				serverManager.recordNewServer(master);
			}
		}
	}

	@Override
	public void nodeDeleted(String path) {
		if (path.startsWith(watcher.estormBackupMasterAddressesZNode + "/")) {
			String serverName = ZKUtil.getNodeName(path);
			LOG.info("MasterServer ephemeral node deleted, processing expiration [" + serverName + "]");
			ServerInfo sn = this.masterServers.remove(serverName);
			if (!serverManager.isMasterServerOnline(sn)) {
				LOG.warn(serverName.toString() + " is not online or isn't known to the master." +
						"The latter could be caused by a DNS misconfiguration.");
				return;
			}
			if (!sn.equals(master.getActiveMaster())) {
				this.serverManager.expireServer(sn);
			}
		}
	}

	@Override
	public void nodeChildrenChanged(String path) {
		if (path.equals(watcher.estormBackupMasterAddressesZNode)) {
			try {
				List<String> servers = ZKUtil.listChildrenAndWatchThem(watcher,
						watcher.estormBackupMasterAddressesZNode);
				add(servers);
			} catch (IOException e) {
				master.abort("Unexpected zk exception getting RS nodes", e);
			} catch (KeeperException e) {
				master.abort("Unexpected zk exception getting RS nodes", e);
			}
		}
	}

	/**
	 * Called when a new node has been created.
	 * 
	 * @param path
	 *            full path of the new node
	 */
	public void nodeCreated(String path) {
		// no-op
	}

	/**
	 * Gets the online servers.
	 * 
	 * @return list of online servers
	 */
	public List<ServerInfo> getOnlineServers() {
		return serverManager.getOnlineServers();
	}
}
