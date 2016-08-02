package com.ery.estorm.client.node.tracker;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ery.estorm.ServerInfo;
import com.ery.estorm.client.node.StormNodeConfig;
import com.ery.estorm.daemon.NullAbortable;
import com.ery.estorm.zk.ZooKeeperNodeTracker;
import com.ery.estorm.zk.ZooKeeperWatcher;

public class MasterTracker extends ZooKeeperNodeTracker {
	protected static final Log LOG = LogFactory.getLog(MasterTracker.class);
	public ServerInfo master;

	public MasterTracker(ZooKeeperWatcher zkw) {
		super(zkw, zkw.getMasterAddressZNode(), new NullAbortable());
		((NullAbortable) super.abortable).setObj(this);
		super.start();
		parseNode();
	}

	@Override
	public synchronized void nodeDeleted(String path) {
		if (path.equals(node)) {
			StormNodeConfig.close(master);
			master = null;
		}
	}

	@Override
	public synchronized void nodeDataChanged(String path) {
		if (path.equals(node)) {
			nodeCreated(path);
			ServerInfo oldM = master;
			parseNode();
			if (!ServerInfo.isSameHostnameAndPort(oldM, master)) {
				StormNodeConfig.close(oldM);
			}
		}
	}

	public ServerInfo getMaster(boolean refresh) {
		refresh = refresh || this.data == null;
		super.getData(refresh);
		if (refresh) {
			parseNode();
		} else {
			if (master == null) {
				parseNode();
			}
		}
		return master;
	}

	private void parseNode() {
		try {
			if (this.data != null)
				master = ServerInfo.parseFrom(data);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
