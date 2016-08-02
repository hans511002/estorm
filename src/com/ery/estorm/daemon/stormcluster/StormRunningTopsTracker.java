package com.ery.estorm.daemon.stormcluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import backtype.storm.utils.Utils;

import com.ery.estorm.daemon.DaemonMaster;
import com.ery.estorm.daemon.topology.TopologyInfo;
import com.ery.estorm.daemon.topology.TopologyInfo.TopologyName;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperListener;
import com.ery.estorm.zk.ZooKeeperWatcher;

/**
 * 监听正在运行的top
 * 
 * @author hans
 * 
 */
public class StormRunningTopsTracker extends ZooKeeperListener {
	protected static final Log LOG = LogFactory.getLog(StormRunningTopsTracker.class);
	// Top列表 TopologyName storm中运行的实例名称，与分配的不同在于时间和批次号
	public Map<String, StormRunningTopInfoTracker> stormInfos = new HashMap<String, StormRunningTopInfoTracker>();
	private DaemonMaster master;
	public static Pattern stormNamePattern = Pattern.compile(".*-\\d+-\\d+");

	public StormRunningTopsTracker(ZooKeeperWatcher watcher, DaemonMaster master) {
		super(watcher);
		this.master = master;

	}

	public void start() {
		watcher.registerListener(this);
		nodeChildrenChanged(watcher.stormTopologys);
	}

	// 原始Storm生成的名称 时间
	public TopologyName getStormName(String path) {
		String nm = ZKUtil.getNodeName(path);
		if (stormNamePattern.matcher(nm).matches()) {
			String mms[] = nm.split("-");
			if (mms.length == 3) {
				TopologyName tn = new TopologyName(mms[0], Long.parseLong(mms[2]));
				tn.batchID = Integer.parseInt(mms[1]);
				return tn;
			}
		}
		return null;
	}

	@Override
	public void nodeDeleted(String path) {
		if (path.startsWith(watcher.stormTopologys + "/")) {
			TopologyName tn = getStormName(path);
			nodeDeleted(tn);
		}
	}

	public synchronized void nodeDeleted(TopologyName tn) {
		if (tn == null)
			return;
		synchronized (this.stormInfos) {
			this.stormInfos.remove(tn.topName);
		}
		TopologyInfo atn = master.getAssignmentManager().assignTops.get(tn.topName);
		if (atn != null) {
			master.getAssignmentManager().removeTop(atn);
			atn.deleteToZk(watcher);
		}
	}

	@Override
	public void nodeChildrenChanged(String path) {
		try {
			if (path.equals(watcher.stormTopologys)) {
				List<String> servers = ZKUtil.listChildrenAndWatchThem(watcher, watcher.stormTopologys);
				add(servers);
			}
		} catch (IOException e) {
			master.abort("Unexpected zk exception getting running topology nodes", e);
		} catch (KeeperException e) {
			master.abort("Unexpected zk exception getting running topology nodes", e);
		}
	}

	/**
	 * 防止事件过快，未及时反应，使用列表判断
	 * 
	 * @param servers
	 * @throws IOException
	 */
	private void add(final List<String> servers) throws IOException {
		List<TopologyName> add = new ArrayList<TopologyName>();
		List<TopologyName> del = new ArrayList<TopologyName>();
		synchronized (this.stormInfos) {
			for (String string : this.stormInfos.keySet()) {
				if (!servers.contains(this.stormInfos.get(string).runningTopName.toString())) {
					del.add(this.stormInfos.get(string).runningTopName);
				}
			}
			for (String string : servers) {
				TopologyName tn = getStormName(string);
				if (!this.stormInfos.containsValue(tn)) {
					add.add(tn);
				}
			}
		}
		for (TopologyName tn : del) {
			nodeDeleted(tn);
		}
		synchronized (this.stormInfos) {
			for (TopologyName tn : add) {
				if (tn != null) {
					if (!stormInfos.containsKey(tn.topName)) {
						stormInfos.put(tn.topName, new StormRunningTopInfoTracker(watcher, master, tn));
						TopologyInfo atn = master.getAssignmentManager().assignTops.get(tn.topName);// ().onlineTopologys.get(key)
						if (atn != null) {
							master.getAssignmentManager().putTop(atn);
							master.getAssignmentManager().runTopNames.put(tn.topName, tn);
						}
					}
				}
			}
			for (String topName : master.getAssignmentManager().assignTops.keySet()) {
				TopologyInfo asTop = master.getAssignmentManager().assignTops.get(topName);
				if (asTop != null) {
					if (!stormInfos.containsKey(asTop.assignTopName.topName)) {// 存在节点，不在Storm中运行
						asTop.deleteToZk(watcher);
						Utils.sleep(1000);
					}
				}
			}
		}
	}

}
