package com.ery.estorm.client.node.tracker;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import backtype.storm.generated.SupervisorInfo;

import com.ery.estorm.daemon.stormcluster.StormServerManage;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperListener;
import com.ery.estorm.zk.ZooKeeperWatcher;

//NODe节点监听运行主机，只用于获取本机OrderHeader.localhost
public class SupervisorTracker extends ZooKeeperListener {
	private static final Log LOG = LogFactory.getLog(SupervisorTracker.class);
	public Map<String, SupervisorInfo> supervisors = new HashMap<String, SupervisorInfo>();

	boolean started;

	public SupervisorTracker(ZooKeeperWatcher watcher) {
		super(watcher);
		started = false;
	}

	public void start() throws KeeperException, IOException {
		if (started)
			return;
		// watcher.registerListener(this);//worker中的BoltNode不需要持续监听
		// List<String> servers = ZKUtil.listChildrenAndWatchThem(watcher,
		// watcher.stormServerZNode);
		List<String> servers = ZKUtil.listChildrenNoWatch(watcher, watcher.stormServerZNode);
		add(servers);
	}

	public void stop() {
		watcher.unregisterListener(this);
		supervisors.clear();
		started = false;
	}

	@Override
	public void nodeDeleted(String path) {
		if (path.startsWith(watcher.estormBackupMasterAddressesZNode + "/")) {
			String serverName = ZKUtil.getNodeName(path);
			this.supervisors.remove(serverName);
		}
	}

	private void add(final List<String> servers) throws IOException {
		synchronized (this.supervisors) {
			for (String n : servers) {
				String superName = ZKUtil.getNodeName(n);
				if (!this.supervisors.containsKey(superName)) {
					try {
						byte[] data = ZKUtil.getDataAndWatch(watcher, ZKUtil.joinZNode(watcher.stormServerZNode, n));
						if (data != null) {
							SupervisorInfo sinfo = StormServerManage.stormMetaSerializer.deserialize(data,
									SupervisorInfo.class);
							// SupervisorInfo sinfo =
							// EStormConstant.castObject(data);
							this.supervisors.put(superName, sinfo);
						}
					} catch (KeeperException e) {
					}
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
			} catch (KeeperException e) {
			}
		}
	}

}
