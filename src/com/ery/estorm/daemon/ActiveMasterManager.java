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
package com.ery.estorm.daemon;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import com.ery.estorm.ServerInfo;
import com.ery.estorm.exceptions.DeserializationException;
import com.ery.estorm.monitor.MonitoredTask;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperListener;
import com.ery.estorm.zk.ZooKeeperWatcher;

public class ActiveMasterManager extends ZooKeeperListener {
	private static final Log LOG = LogFactory.getLog(ActiveMasterManager.class);

	final AtomicBoolean clusterHasActiveMaster = new AtomicBoolean(false);
	final AtomicBoolean clusterShutDown = new AtomicBoolean(false);

	private final ServerInfo sn;
	private ServerInfo activeMaster;
	private final DaemonMaster master;

	boolean isActiveMaster;

	/**
	 * @param watcher
	 * @param sn
	 *            ServerName
	 * @param master
	 *            In an instance of a Master.
	 */
	ActiveMasterManager(ZooKeeperWatcher watcher, ServerInfo sn, DaemonMaster master) {
		super(watcher);
		this.sn = sn;
		this.master = master;
	}

	@Override
	public void nodeCreated(String path) {
		handle(path);
	}

	@Override
	public void nodeDeleted(String path) {
		if (path.equals(watcher.estormClusterStateZNode) && !master.isStopped()) {
			clusterShutDown.set(true);
			if (master.isActiveMaster()) {
				master.stop("shutdown cluster");
			}
		}
		handle(path);
	}

	void handle(final String path) {
		if (path.equals(watcher.getMasterAddressZNode()) && !master.isStopped()) {
			handleMasterNodeChange();
		}
	}

	public boolean isActiveMaster() {
		return isActiveMaster;
	}

	public ServerInfo getActiveMaster() {
		return this.activeMaster;
	}

	private void handleMasterNodeChange() {
		// Watch the node and check if it exists.
		try {
			synchronized (clusterHasActiveMaster) {
				if (ZKUtil.watchAndCheckExists(watcher, watcher.getMasterAddressZNode())) {
					// A master node exists, there is an active master
					LOG.debug("A master is now available");
					byte[] data = ZKUtil.getDataAndWatch(watcher, watcher.getMasterAddressZNode());
					activeMaster = ServerInfo.parseFrom(data);
					clusterHasActiveMaster.set(true);
					if (activeMaster == null) {
						LOG.debug("No master available. Notifying waiting threads");
						clusterHasActiveMaster.set(false);
						// Notify any thread waiting to become the active master
						clusterHasActiveMaster.notifyAll();
					} else {
						isActiveMaster = sn.equals(this.activeMaster);
					}
					master.getServerManager().recordNewServer(activeMaster);

				} else {
					// Node is no longer there, cluster does not have an active
					// master
					LOG.debug("No master available. Notifying waiting threads");
					clusterHasActiveMaster.set(false);
					// Notify any thread waiting to become the active master
					clusterHasActiveMaster.notifyAll();
				}
			}
		} catch (KeeperException ke) {
			master.abort("Received an unexpected KeeperException, aborting", ke);
		} catch (IOException e) {
			master.abort("Received an unexpected KeeperException, aborting", e);
		}
	}

	/**
	 * Block until becoming the active master.
	 * 
	 * Method blocks until there is not another active master and our attempt to
	 * become the new active master is successful.
	 * 
	 * This also makes sure that we are watching the master znode so will be
	 * notified if another master dies.
	 * 
	 * @param startupStatus
	 * @return True if no issue becoming active master else false if another
	 *         master was running or if some other problem (zookeeper, stop flag
	 *         has been set on this Master)
	 */
	boolean blockUntilBecomingActiveMaster(MonitoredTask startupStatus) {
		while (true) {
			startupStatus.setStatus("Trying to register in ZK as active master");
			// Try to become the active master, watch if there is another
			// master.
			// Write out our ServerName as versioned bytes.
			try {
				String backupZNode = ZKUtil.joinZNode(this.watcher.estormBackupMasterAddressesZNode,
						this.sn.getHostPort());
				if (ZKUtil.createEphemeralNodeAndWatch(this.watcher, this.watcher.getMasterAddressZNode(),
						this.sn.toByteArray())) {
					// If we were a backup master before, delete our ZNode from
					// the backup
					// master directory since we are the active now)
					if (ZKUtil.checkExists(this.watcher, backupZNode) != -1) {
						LOG.info("Deleting ZNode for " + backupZNode + " from backup master directory");
						ZKUtil.deleteNodeFailSilent(this.watcher, backupZNode);// 从备中删除
					}
					// We are the master, return
					startupStatus.setStatus("Successfully registered as active master.");
					this.clusterHasActiveMaster.set(true);
					LOG.info("Registered Active Master=" + this.sn);
					this.isActiveMaster = true;
					return true;
				}

				// There is another active master running elsewhere or this is a
				// restart
				// and the master ephemeral node has not expired yet.
				this.clusterHasActiveMaster.set(true);

				/*
				 * Add a ZNode for ourselves in the backup master directory
				 * since we are not the active master.
				 * 
				 * If we become the active master later, ActiveMasterManager
				 * will delete this node explicitly. If we crash before then,
				 * ZooKeeper will delete this node for us since it is ephemeral.
				 */
				LOG.info("Adding ZNode for " + backupZNode + " in backup master directory");
				ZKUtil.createEphemeralNodeAndWatch(this.watcher, backupZNode, this.sn.toByteArray());
				String msg;
				byte[] bytes = ZKUtil.getDataAndWatch(this.watcher, this.watcher.getMasterAddressZNode());
				if (bytes == null) {
					msg = ("A master was detected, but went down before its address "
							+ "could be read.  Attempting to become the next active master");
				} else {
					ServerInfo currentMaster;
					try {
						currentMaster = ServerInfo.parseFrom(bytes);
					} catch (DeserializationException e) {
						LOG.warn("Failed parse", e);
						// Hopefully next time around we won't fail the parse.
						// Dangerous.
						continue;
					}
					if (ServerInfo.isSameHostnameAndPort(currentMaster, this.sn)) {
						msg = ("Current master has this master's address, " + currentMaster + "; master was restarted? Deleting node.");
						// Hurry along the expiration of the znode.
						ZKUtil.deleteNode(this.watcher, this.watcher.getMasterAddressZNode());
					} else {
						msg = "Another master is the active master, " + currentMaster +
								"; waiting to become the next active master";
					}
				}
				LOG.info(msg);
				startupStatus.setStatus(msg);

			} catch (KeeperException ke) {
				master.abort("Received an unexpected KeeperException, aborting", ke);
				return false;
			} catch (IOException e) {
				master.abort("Received an unexpected IOException, aborting", e);
				return false;
			}
			synchronized (this.clusterHasActiveMaster) {
				while (this.clusterHasActiveMaster.get() && !this.master.isStopped()) {
					try {
						this.clusterHasActiveMaster.wait();
					} catch (InterruptedException e) {
						// We expect to be interrupted when a master dies,
						// will fall out if so
						LOG.debug("Interrupted waiting for master to die", e);
					}
				}
				if (clusterShutDown.get()) {
					this.master.stop("Cluster went down before this master became active");
				}
				if (this.master.isStopped()) {
					return false;
				}
				// there is no active master so we can try to become active
				// master again
			}
		}
	}

	/**
	 * @return True if cluster has an active master.
	 */
	public boolean haveActiveMaster() {
		try {
			if (ZKUtil.checkExists(watcher, watcher.getMasterAddressZNode()) >= 0) {
				return true;
			}
		} catch (KeeperException ke) {
			LOG.info("Received an unexpected KeeperException when checking " + "isActiveMaster : " + ke);
		} catch (IOException e) {
			LOG.info("Received an unexpected KeeperException when checking " + "isActiveMaster : " + e);
		}
		return false;
	}

	// stop this master to delete activeMaster znode
	public void stop() {
		try {
			if (activeMaster != null && activeMaster.equals(this.sn)) {
				ZKUtil.deleteNode(watcher, watcher.getMasterAddressZNode());
			}
		} catch (KeeperException e) {
			LOG.error(this.watcher.prefix("Error deleting our own master address node"), e);
		} catch (IOException e) {
			LOG.error(this.watcher.prefix("Error deleting our own master address node"), e);
		}
	}
}
