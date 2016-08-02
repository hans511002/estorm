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
package com.ery.estorm.zk;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;

import com.ery.estorm.config.Configuration;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.Abortable;
import com.ery.estorm.exceptions.ZooKeeperConnectionException;
import com.ery.estorm.util.Threads;

/**
 * Acts as the single ZooKeeper Watcher. One instance of this is instantiated
 * for each Master, RegionServer, and client process.
 * 
 * <p>
 * This is the only class that implements {@link Watcher}. Other internal
 * classes which need to be notified of ZooKeeper events must register with the
 * local instance of this watcher via {@link #registerListener}.
 * 
 * <p>
 * This class also holds and manages the connection to ZooKeeper. Code to deal
 * with connection related events and exceptions are handled here.
 */
public class ZooKeeperWatcher implements Watcher, Abortable, Closeable, Serializable {
	private static final Log LOG = LogFactory.getLog(ZooKeeperWatcher.class);

	// Identifier for this watcher (for logging only). It is made of the prefix
	// passed on construction and the zookeeper sessionid.
	private String identifier;

	// zookeeper quorum
	private String quorum;

	// zookeeper connection
	private RecoverableZooKeeper recoverableZooKeeper;

	// abortable in case of zk failure
	public Abortable abortable;
	// Used if abortable is null
	private boolean aborted = false;

	// listeners to be notified
	private final List<ZooKeeperListener> listeners = new CopyOnWriteArrayList<ZooKeeperListener>();

	// Used by ZKUtil:waitForZKConnectionIfAuthenticating to wait for SASL
	// negotiation to complete
	public CountDownLatch saslLatch = new CountDownLatch(1);

	// node names
	// base znode for this cluster
	public String baseZNode;

	// ========storm=============
	// base storm node
	public String stormBaseZNode;//
	// nimbus host node
	public String estormNimbus;// baseZNode/nimbus
	// znode for storm server dir
	public String stormServerZNode; // stormBaseZNode/supervisors
	public String stormWorkerbeats;
	public String stormErrors;
	public String stormTopologys;// storm0.9/storms
	public String stormAssignments;

	// ========estorm=============
	// znode containing location of server hosting meta region
	public String estormTopologyZNode;// dir storm程序提交临时节点 值为StormTopology序列化对象
	// znode containing ephemeral nodes of the topology node
	public String estormNodeZNode; // dir storm程序提交临时节点 值为节点配置信息序列化对象
	public String estormJoinZNode; // dir JoinTable list
	public String estormJoinOrderZNode; // dir JoinTable list
	public String estormServerJoinStateZNode; // dir 服务器加载运行状态
	public String estormLogZNode; // dir host list

	public String estormConfigDbZNode;//
	public String estormAssignmentZNode;// 提交到集群执行的命令 stop top

	// znode of currently active master
	private String estormMasterAddressZNode;
	// 集群状态，命令等
	public String estormClusterStateZNode;

	// znode of this master in backup master directory, if not the active master
	public String estormBackupMasterAddressesZNode;// dir

	// Certain ZooKeeper nodes need to be world-readable
	public static final ArrayList<ACL> CREATOR_ALL_AND_WORLD_READABLE = new ArrayList<ACL>() {
		{
			add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));
			add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.AUTH_IDS));
		}
	};

	private final Configuration conf;

	private final Exception constructorCaller;

	/**
	 * Instantiate a ZooKeeper connection and watcher.
	 * 
	 * @param identifier
	 *            string that is passed to RecoverableZookeeper to be used as
	 *            identifier for this instance. Use null for default.
	 * @throws IOException
	 * @throws ZooKeeperConnectionException
	 */
	public ZooKeeperWatcher(Configuration conf, String identifier, Abortable abortable)
			throws ZooKeeperConnectionException, IOException {
		this(conf, identifier, abortable, false);
	}

	/**
	 * Instantiate a ZooKeeper connection and watcher.
	 * 
	 * @param conf
	 * @param identifier
	 *            string that is passed to RecoverableZookeeper to be used as
	 *            identifier for this instance. Use null for default.
	 * @param abortable
	 *            Can be null if there is on error there is no host to abort:
	 *            e.g. client context.
	 * @param canCreateBaseZNode
	 * @throws IOException
	 * @throws ZooKeeperConnectionException
	 */
	public ZooKeeperWatcher(Configuration conf, String identifier, Abortable abortable, boolean canCreateBaseZNode)
			throws IOException, ZooKeeperConnectionException {
		this.conf = conf;
		// Capture a stack trace now. Will print it out later if problem so we
		// can
		// distingush amongst the myriad ZKWs.
		try {
			throw new Exception("ZKW CONSTRUCTOR STACK TRACE FOR DEBUGGING");
		} catch (Exception e) {
			this.constructorCaller = e;
		}
		this.quorum = conf.get(EStormConstant.ZOOKEEPER_QUORUM);
		// Identifier will get the sessionid appended later below down when we
		// handle the syncconnect event.
		this.identifier = identifier;
		this.abortable = abortable;
		setNodeNames(conf);
		this.recoverableZooKeeper = ZKUtil.connect(conf, quorum, this, identifier);
		if (canCreateBaseZNode) {
			createBaseZNodes();
		}
	}

	private void createBaseZNodes() throws ZooKeeperConnectionException, IOException {
		try {
			// Create all the necessary "directories" of znodes
			ZKUtil.createAndFailSilent(this, baseZNode);
			ZKUtil.createAndFailSilent(this, stormServerZNode);
			ZKUtil.createAndFailSilent(this, estormTopologyZNode);
			ZKUtil.createAndFailSilent(this, estormNodeZNode);
			ZKUtil.createAndFailSilent(this, estormJoinZNode);
			ZKUtil.createAndFailSilent(this, estormLogZNode);
			ZKUtil.createAndFailSilent(this, estormJoinOrderZNode);
			ZKUtil.createAndFailSilent(this, estormServerJoinStateZNode);
			ZKUtil.createAndFailSilent(this, estormBackupMasterAddressesZNode);
			String kafkaMsgZNode = ZKUtil.joinZNode(baseZNode, conf.get(EStormConstant.STORM_MSG_KAFKA_ZNODE, "kafka"));
			ZKUtil.createAndFailSilent(this, kafkaMsgZNode);
		} catch (KeeperException e) {
			throw new ZooKeeperConnectionException(prefix("Unexpected KeeperException creating base node"), e);
		}
	}

	@Override
	public String toString() {
		return this.identifier;
	}

	/**
	 * Adds this instance's identifier as a prefix to the passed
	 * <code>str</code>
	 * 
	 * @param str
	 *            String to amend.
	 * @return A new string with this instance's identifier as prefix: e.g. if
	 *         passed 'hello world', the returned string could be
	 */
	public String prefix(final String str) {
		return this.toString() + " " + str;
	}

	/**
	 * Set the local variable node names using the specified configuration.
	 */
	private void setNodeNames(Configuration conf) {
		baseZNode = conf.get(EStormConstant.ZOOKEEPER_BASE_ZNODE, "/estorm");

		//
		// // znode of currently active master
		// private String masterAddressZNode;
		//
		// // znode of this master in backup master directory, if not the active
		// master
		// public String backupMasterAddressesZNode;

		// // znode containing the lock for the nodes
		// public String nodeLockZNode;
		stormBaseZNode = conf.get(EStormConstant.STORM_BASE_ZNODE, "/storm");
		stormServerZNode = ZKUtil.joinZNode(stormBaseZNode, "supervisors");
		stormWorkerbeats = ZKUtil.joinZNode(stormBaseZNode, "workerbeats");
		stormErrors = ZKUtil.joinZNode(stormBaseZNode, "errors");
		stormTopologys = ZKUtil.joinZNode(stormBaseZNode, "storms");
		stormAssignments = ZKUtil.joinZNode(stormBaseZNode, "assignments");

		estormNimbus = ZKUtil.joinZNode(baseZNode, conf.get(EStormConstant.STORM_MASTER_ZNODE, "stormNimbus"));
		estormTopologyZNode = ZKUtil.joinZNode(baseZNode, conf.get(EStormConstant.STORM_TOPOLOGY_ZNODE, "topology"));
		estormNodeZNode = ZKUtil.joinZNode(baseZNode, conf.get(EStormConstant.STORM_TASKNODE_ZNODE, "tasknode"));
		estormJoinZNode = ZKUtil.joinZNode(baseZNode, conf.get(EStormConstant.STORM_JOINDATA_NODE_ZNODE, "joinNode"));
		estormJoinOrderZNode = ZKUtil.joinZNode(baseZNode,
				conf.get(EStormConstant.STORM_JOINDATA_NODE_ZNODE, "joinOrder"));
		estormLogZNode = ZKUtil.joinZNode(baseZNode, conf.get(EStormConstant.STORM_JOINDATA_NODE_ZNODE, "logs"));
		estormServerJoinStateZNode = ZKUtil.joinZNode(estormJoinOrderZNode, "RuningState");
		estormJoinOrderZNode = ZKUtil.joinZNode(estormJoinOrderZNode, "RuningOrder");

		estormMasterAddressZNode = ZKUtil.joinZNode(baseZNode,
				conf.get(EStormConstant.STORM_MAG_MASTER_ZNODE, "master"));
		estormBackupMasterAddressesZNode = ZKUtil.joinZNode(baseZNode,
				conf.get(EStormConstant.STORM_MAG_BACKMASTER_ZNODE, "backup-masters"));
		estormClusterStateZNode = ZKUtil.joinZNode(baseZNode,
				conf.get(EStormConstant.ESTORM_CLUSTER_STATUS_ZNODE, "status"));// 控制集群启停
		estormConfigDbZNode = ZKUtil.joinZNode(baseZNode,
				conf.get(EStormConstant.ESTORM_CLUSTER_STATUS_ZNODE, "configdb"));
		estormAssignmentZNode = ZKUtil.joinZNode(baseZNode, "assignment");
	}

	/**
	 * Register the specified listener to receive ZooKeeper events.
	 * 
	 * @param listener
	 */
	public void registerListener(ZooKeeperListener listener) {
		if (listeners.contains(listener) == false)
			listeners.add(listener);
	}

	public void registerListener(ZooKeeperListener listener, int index) {
		if (listeners.contains(listener) == false) {
			listeners.add(index, listener);
		} else {
			listeners.remove(listener);
			listeners.add(0, listener);
		}
	}

	/**
	 * Register the specified listener to receive ZooKeeper events and add it as
	 * the first in the list of current listeners.
	 * 
	 * @param listener
	 */
	public void registerListenerFirst(ZooKeeperListener listener) {
		if (listeners.contains(listener) == false) {
			listeners.add(0, listener);
		} else {
			listeners.remove(listener);
			listeners.add(0, listener);
		}
	}

	public void unregisterListener(ZooKeeperListener listener) {
		listeners.remove(listener);
	}

	/**
	 * Clean all existing listeners
	 */
	public void unregisterAllListeners() {
		listeners.clear();
	}

	/**
	 * Get a copy of current registered listeners
	 */
	public List<ZooKeeperListener> getListeners() {
		return new ArrayList<ZooKeeperListener>(listeners);
	}

	/**
	 * @return The number of currently registered listeners
	 */
	public int getNumberOfListeners() {
		return listeners.size();
	}

	/**
	 * Get the connection to ZooKeeper.
	 * 
	 * @return connection reference to zookeeper
	 */
	public RecoverableZooKeeper getRecoverableZooKeeper() {
		return recoverableZooKeeper;
	}

	public void reconnectAfterExpiration() throws IOException, InterruptedException {
		recoverableZooKeeper.reconnectAfterExpiration();
	}

	/**
	 * Get the quorum address of this instance.
	 * 
	 * @return quorum string of this zookeeper connection instance
	 */
	public String getQuorum() {
		return quorum;
	}

	/**
	 * Method called from ZooKeeper for events and connection status.
	 * <p>
	 * Valid events are passed along to listeners. Connection status changes are
	 * dealt with locally.
	 */
	@Override
	public void process(WatchedEvent event) {

		String path = event.getPath();
		if (path != null || EventType.NodeDeleted == event.getType()) {
			try {
				ZKUtil.watchAndCheckExists(this, path);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		LOG.debug(prefix("Received ZooKeeper Event, " + "type=" + event.getType() + ", " + "state=" + event.getState() +
				", " + "path=" + path));
		switch (event.getType()) {

		// If event type is NONE, this is a connection status change
		case None: {
			connectionEvent(event);
			break;
		}

		// Otherwise pass along to the listeners

		case NodeCreated: {
			for (ZooKeeperListener listener : listeners) {
				listener.nodeCreated(path);
			}
			break;
		}

		case NodeDeleted: {
			for (ZooKeeperListener listener : listeners) {
				listener.nodeDeleted(path);
			}
			break;
		}

		case NodeDataChanged: {
			for (ZooKeeperListener listener : listeners) {
				listener.nodeDataChanged(path);
			}
			break;
		}

		case NodeChildrenChanged: {
			for (ZooKeeperListener listener : listeners) {
				listener.nodeChildrenChanged(path);
			}
			break;
		}
		}
	}

	// Connection management

	/**
	 * Called when there is a connection-related event via the Watcher callback.
	 * <p>
	 * If Disconnected or Expired, this should shutdown the cluster. But, since
	 * we send a KeeperException.SessionExpiredException along with the abort
	 * call, it's possible for the Abortable to catch it and try to create a new
	 * session with ZooKeeper. This is what the client does in HCM.
	 * <p>
	 * 
	 * @param event
	 */
	private void connectionEvent(WatchedEvent event) {
		switch (event.getState()) {
		case SyncConnected:
			// Now, this callback can be invoked before the this.zookeeper is
			// set.
			// Wait a little while.
			long finished = System.currentTimeMillis() +
					this.conf.getLong("hbase.zookeeper.watcher.sync.connected.wait", 2000);
			while (System.currentTimeMillis() < finished) {
				Threads.sleep(1);
				if (this.recoverableZooKeeper != null)
					break;
			}
			if (this.recoverableZooKeeper == null) {
				LOG.error("ZK is null on connection event -- see stack trace "
						+ "for the stack trace when constructor was called on this zkw", this.constructorCaller);
				throw new NullPointerException("ZK is null");
			}
			this.identifier = this.identifier + "-0x" + Long.toHexString(this.recoverableZooKeeper.getSessionId());
			// Update our identifier. Otherwise ignore.
			LOG.debug(this.identifier + " connected");
			break;

		// Abort the server if Disconnected or Expired
		case Disconnected:
			LOG.debug(prefix("Received Disconnected from ZooKeeper, ignoring"));
			break;

		case Expired:
			String msg = prefix(this.identifier + " received expired from " + "ZooKeeper, aborting");
			// TODO: One thought is to add call to ZooKeeperListener so say,
			// ZooKeeperNodeTracker can zero out its data values.
			if (this.abortable != null) {
				this.abortable.abort(msg, new KeeperException.SessionExpiredException());
			}
			break;

		case ConnectedReadOnly:
		case SaslAuthenticated:
			break;

		default:
			throw new IllegalStateException("Received event is not valid: " + event.getState());
		}
	}

	/**
	 * Forces a synchronization of this ZooKeeper client connection.
	 * <p>
	 * Executing this method before running other methods will ensure that the
	 * subsequent operations are up-to-date and consistent as of the time that
	 * the sync is complete.
	 * <p>
	 * This is used for compareAndSwap type operations where we need to read the
	 * data of an existing node and delete or transition that node, utilizing
	 * the previously read version and data. We want to ensure that the version
	 * read is up-to-date from when we begin the operation.
	 */
	public void sync(String path) {
		this.recoverableZooKeeper.sync(path, null, null);
	}

	/**
	 * Handles KeeperExceptions in client calls.
	 * <p>
	 * This may be temporary but for now this gives one place to deal with
	 * these.
	 * <p>
	 * TODO: Currently this method rethrows the exception to let the caller
	 * handle
	 * <p>
	 * 
	 * @param ke
	 * @throws KeeperException
	 */
	public void keeperException(KeeperException ke) throws KeeperException {
		LOG.error(prefix("Received unexpected KeeperException, re-throwing exception"), ke);
		throw ke;
	}

	/**
	 * Handles InterruptedExceptions in client calls.
	 * <p>
	 * This may be temporary but for now this gives one place to deal with
	 * these.
	 * <p>
	 * TODO: Currently, this method does nothing. Is this ever expected to
	 * happen? Do we abort or can we let it run? Maybe this should be logged as
	 * WARN? It shouldn't happen?
	 * <p>
	 * 
	 * @param ie
	 */
	public void interruptedException(InterruptedException ie) {
		LOG.debug(prefix("Received InterruptedException, doing nothing here"), ie);
		// At least preserver interrupt.
		Thread.currentThread().interrupt();
		// no-op
	}

	/**
	 * Close the connection to ZooKeeper.
	 * 
	 * @throws InterruptedException
	 */
	public void close() {
		try {
			if (recoverableZooKeeper != null) {
				recoverableZooKeeper.close();
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	public Configuration getConfiguration() {
		return conf;
	}

	@Override
	public void abort(String why, Throwable e) {
		if (this.abortable != null)
			this.abortable.abort(why, e);
		else
			this.aborted = true;
	}

	@Override
	public boolean isAborted() {
		return this.abortable == null ? this.aborted : this.abortable.isAborted();
	}

	/**
	 * @return Path to the currently active master.
	 */
	public String getMasterAddressZNode() {
		return this.estormMasterAddressZNode;
	}

}
