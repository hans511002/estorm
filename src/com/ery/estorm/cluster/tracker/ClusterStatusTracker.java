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
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import com.ery.estorm.daemon.Server;
import com.ery.estorm.util.VersionInfo;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperNodeTracker;
import com.ery.estorm.zk.ZooKeeperWatcher;

/**
 * 集群状态监控
 * 
 */
public class ClusterStatusTracker extends ZooKeeperNodeTracker {
	private static final Log LOG = LogFactory.getLog(ClusterStatusTracker.class);

	public ClusterStatusTracker(ZooKeeperWatcher watcher, Server abortable) {
		super(watcher, watcher.estormClusterStateZNode, abortable);
	}

	/**
	 * Checks if cluster is up.
	 * 
	 * @return true if the cluster up ('shutdown' is its name up in zk) znode exists with data, false if not
	 */
	public boolean isClusterUp() {
		return super.getData(false) != null;
	}

	/**
	 * Sets the cluster as up.
	 * 
	 * @throws KeeperException
	 *             unexpected zk exception
	 * @throws IOException
	 */
	public void setClusterUp() throws KeeperException, IOException {
		if (!isClusterUp()) {
			byte[] upData = toByteArray();
			try {
				ZKUtil.createAndWatch(watcher, watcher.estormClusterStateZNode, upData);
			} catch (KeeperException.NodeExistsException nee) {
				ZKUtil.setData(watcher, watcher.estormClusterStateZNode, upData);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * Sets the cluster as down by deleting the znode.
	 * 
	 * @throws KeeperException
	 *             unexpected zk exception
	 * @throws IOException
	 */
	public void setClusterDown() throws KeeperException, IOException {
		try {
			ZKUtil.deleteNode(watcher, watcher.estormClusterStateZNode);
		} catch (KeeperException.NoNodeException nne) {
			LOG.warn("Attempted to set cluster as down but already down, cluster " + "state node (" + watcher.estormClusterStateZNode
					+ ") not found");
		}
	}

	static byte[] toByteArray() {
		String[] versions = VersionInfo.versionReport();
		String startTime = new Date(System.currentTimeMillis()).toLocaleString();
		StringBuffer sb = new StringBuffer();
		for (String ver : versions) {
			sb.append(ver);
			sb.append(" ");
		}
		sb.append("\nStartTime:");
		sb.append(startTime);
		return sb.toString().getBytes();
	}
}
