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
package com.ery.estorm.daemon.stormcluster;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import backtype.storm.generated.SupervisorInfo;

import com.ery.estorm.daemon.DaemonMaster;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperListener;
import com.ery.estorm.zk.ZooKeeperWatcher;

/**
 * <pre>
 * 监控Storm服务运行主机，自动重启storm服务
 * </pre>
 * 
 * @author .hans
 * 
 */
public class StormSupervisorTracker extends ZooKeeperListener {
	private static final Log LOG = LogFactory.getLog(StormSupervisorTracker.class);
	public Map<String, StormSupervisorInfo> supervisorServers = new HashMap<String, StormSupervisorInfo>();

	private DaemonMaster master;
	StormServerManage stormServer;
	boolean started;

	public static class StormSupervisorInfo {
		public String sid;
		public SupervisorInfo info;

		public StormSupervisorInfo(ZooKeeperWatcher watcher, String sid) throws KeeperException, IOException {
			byte[] data = ZKUtil.getDataAndWatch(watcher, watcher.stormServerZNode + "/" + sid);
			if (data != null) {
				info = StormServerManage.stormMetaSerializer.deserialize(data, SupervisorInfo.class);
			}
		}

	}

	public StormSupervisorTracker(ZooKeeperWatcher watcher, DaemonMaster master, StormServerManage stServer) {
		super(watcher);
		this.master = master;
		stormServer = stServer;
		started = false;
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
		if (started)
			return;
		watcher.registerListener(this);
		nodeChildrenChanged(watcher.stormServerZNode);
	}

	public void stop() {
		watcher.unregisterListener(this);
		supervisorServers.clear();
		started = false;
	}

	private void add(final List<String> servers) throws IOException, KeeperException {
		synchronized (this.supervisorServers) {
			for (String sid : servers) {
				StormSupervisorInfo sinfo = new StormSupervisorInfo(this.watcher, sid);
				supervisorServers.put(sinfo.info.get_hostname(), sinfo);
			}
		}
	}

	public String getHostName(String supername) {
		StormSupervisorInfo sinfo = this.supervisorServers.get(supername);
		if (sinfo == null) {
			return null;
		} else {
			return sinfo.info.get_hostname();
		}
	}

	public void updateSupervisorInfo() throws IOException, KeeperException {
		List<String> servers = ZKUtil.listChildrenAndWatchThem(watcher, watcher.stormServerZNode);
		add(servers);
	}

	@Override
	public void nodeDeleted(String path) {
		if (path.startsWith(watcher.stormServerZNode + "/")) {
			String serverName = ZKUtil.getNodeName(path);
			for (String hostName : this.supervisorServers.keySet()) {
				StormSupervisorInfo sinfo = this.supervisorServers.get(hostName);
				if (sinfo.sid.equals(serverName)) {
					this.supervisorServers.remove(hostName);
					stormServer.restartSupervisor(sinfo);
					break;
				}
			}
		}
	}

	@Override
	public void nodeChildrenChanged(String path) {
		if (path.equals(watcher.stormServerZNode)) {
			try {
				List<String> servers = ZKUtil.listChildrenAndWatchThem(watcher, watcher.stormServerZNode);
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

}
