package com.ery.estorm.client.node.tracker;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import com.ery.estorm.ServerInfo;
import com.ery.estorm.client.node.StormNodeConfig;
import com.ery.estorm.client.node.StormNodeListenOrder.BackMasterLisenThread;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperListener;
import com.ery.estorm.zk.ZooKeeperWatcher;

//NODe节点监听运行主机
public class BackMasterTracker extends ZooKeeperListener {
	private static final Log LOG = LogFactory.getLog(BackMasterTracker.class);
	public final Map<String, ServerInfo> masterServers = new ConcurrentHashMap<String, ServerInfo>();

	boolean started;

	public BackMasterTracker(ZooKeeperWatcher watcher) {
		super(watcher);
		started = false;
	}

	public void start() {
		if (started)
			return;
		watcher.registerListener(this);
		try {
			List<String> servers = ZKUtil.listChildrenAndWatchThem(watcher, watcher.estormBackupMasterAddressesZNode);
			add(servers);
		} catch (Exception e) {

		}
	}

	public void stop() {
		watcher.unregisterListener(this);
		masterServers.clear();
		started = false;
	}

	private void add(final List<String> servers) throws IOException, KeeperException {
		synchronized (this.masterServers) {
			for (String n : servers) {
				byte[] data = ZKUtil.getDataAndWatch(watcher, watcher.estormBackupMasterAddressesZNode + "/" + n);
				ServerInfo sn = ServerInfo.parseFrom(data);
				if (!this.masterServers.containsKey(sn.hostName)) {
					this.masterServers.put(sn.getHostname(), sn);
					// 启动备Master通信线程
					BackMasterLisenThread bml = StormNodeConfig.listen.new BackMasterLisenThread(sn);
					synchronized (StormNodeConfig.listen.backMasteLisen) {
						StormNodeConfig.listen.backMasteLisen.put(sn, bml);
						bml.start();
					}
				}
			}
		}
	}

	@Override
	public void nodeDeleted(String path) {
		if (path.startsWith(watcher.estormBackupMasterAddressesZNode + "/")) {
			try {
				byte[] data = ZKUtil.getDataAndWatch(watcher, path);
				ServerInfo sn = ServerInfo.parseFrom(data);
				synchronized (this.masterServers) {
					this.masterServers.remove(sn);
					synchronized (StormNodeConfig.listen.backMasteLisen) {
						BackMasterLisenThread bml = StormNodeConfig.listen.backMasteLisen.remove(sn);
						if (bml != null)
							bml.interrupt();// stop();
					}
				}
			} catch (Exception e) {
			}
		}
	}

	@Override
	public void nodeChildrenChanged(String path) {
		if (path.startsWith(watcher.estormBackupMasterAddressesZNode)) {
			try {
				List<String> servers = ZKUtil.listChildrenAndWatchThem(watcher, watcher.stormServerZNode);
				add(servers);
			} catch (IOException e) {
			} catch (KeeperException e) {
			}
		}
	}

}
