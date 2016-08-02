package com.ery.estorm.join;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.utils.Utils;

import com.ery.estorm.client.node.StormNodeConfig;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.Server;
import com.ery.estorm.daemon.topology.NodeInfo.JoinPO;
import com.ery.estorm.daemon.topology.NodeInfo.JoinPO.LoadState;
import com.ery.estorm.util.HasThread;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperListener;
import com.ery.estorm.zk.ZooKeeperWatcher;

/**
 * <pre>
 * 所有管理节点及boltNode上运行，监听关联数据配置变更,在boltNode上运行时需要判断相关性
 * 监听关联数据配置信息，管理关联节点
 * </pre>
 * 
 * @author .hans
 * 
 */
public class JoinListTracker extends ZooKeeperListener {
	private static final Log LOG = LogFactory.getLog(StormNodeConfig.class);
	String node;
	java.util.regex.Pattern pattern = null;
	Server abortable;
	public final Map<Long, JoinPO> joins = new HashMap<Long, JoinPO>();
	JoinDataManage joinDataManage = null;
	long joinSyncLoadListenInterval = 30000;

	public JoinDataManage getJoinDataManage() {
		return joinDataManage;
	}

	public void setJoinDataManage(JoinDataManage joinDataManage) {
		this.joinDataManage = joinDataManage;
	}

	public JoinListTracker(ZooKeeperWatcher watcher, Server abortable) {
		super(watcher);
		this.node = watcher.estormJoinZNode;
		pattern = Pattern.compile(watcher.estormJoinZNode + "/join_(\\d+)$");
		// nodePri = this.node + "/join_";
		this.abortable = abortable;
	}

	public void start() {
		this.watcher.registerListener(this);
	}

	// boltNode客户端或主Master
	// 主Master在JoinDataManage中启动监听数据库时调用
	// 启动周期性刷新数据线程
	public void runListen() {
		final JoinListTracker joinListTracker = this;
		new HasThread() {
			@Override
			public void run() {
				Thread.currentThread().setName("JoinSyncLoad");
				while (!abortable.isAborted() && !abortable.isStopped()) {
					try {
						joinListTracker.syncLoadJoin();
						Utils.sleep(joinSyncLoadListenInterval);
					} catch (Exception e) {
						LOG.error("周期加载本地内存关联 数据", e);
					}

				}

			}
		}.setDaemon(true).start();
	}

	// 排除关联节点的子节点
	private boolean isJoinNode(String path) {
		return pattern.matcher(path).find();
	}

	private long getJoinId(String path) {
		Matcher m = pattern.matcher(path);
		if (m.find())
			return Long.parseLong(m.group(1));
		else
			return 0;
	}

	public void nodeDataChanged(String path) {
		nodeCreated(path);
	}

	public void nodeDeleted(String path) {
		if (!isJoinNode(path))
			return;
		long joinId = getJoinId(path);// Long.parseLong(ZKUtil.getNodeName(path).replace("join_",
										// ""));
		synchronized (joins) {
			JoinPO jpo = this.joins.remove(joinId);
			if (jpo.INLOCAL_MEMORY != 0) {
				JoinData.remove(jpo);
			}
		}
	}

	public void nodeCreated(String path) {
		if (!isJoinNode(path))
			return;
		long joinId = getJoinId(path);// Long.parseLong(ZKUtil.getNodeName(path).replace("join_",
										// ""));
		try {
			byte[] data = ZKUtil.getDataAndWatch(watcher, path);
			if (data != null) {
				JoinPO jpo = EStormConstant.castObject(data);
				synchronized (joins) {
					joins.put(joinId, jpo);
					// 加载
					if (jpo.INLOCAL_MEMORY != 0 && StormNodeConfig.isNode)
						JoinData.loadJoinData(jpo);
				}
			}
		} catch (Exception e) {
			abortable.abort("Unexpected exception handling nodeCreated event", e);
		}
	}

	// 判断Join的周期性加载
	public void syncLoadJoin() {
		synchronized (joins) {
			for (Long l : joins.keySet()) {
				JoinPO jpo = joins.get(l);
				if (jpo.needLoad == false)
					continue;
				// boltNode 非内存不在节点上处理
				if (StormNodeConfig.isNode && jpo.INLOCAL_MEMORY == 1) {
					if (jpo.FLASH_TYPE == 1) {

					} else if (jpo.FLASH_TYPE == 2) {

					}
					try {
						jpo.loadState = LoadState.Initing;
						JoinData.loadJoinData(jpo);
					} catch (Exception e) {
						LOG.error("分配加载关联数据的任务失败：", e);
					}

				} else if (jpo.INLOCAL_MEMORY == 0) {// 内存不在master上处理
					if (jpo.FLASH_TYPE == 1) {

					} else if (jpo.FLASH_TYPE == 2) {

					}
					try {
						if (joinDataManage == null) {
							throw new RuntimeException("joinDataManage is null");
						}
						jpo.loadState = LoadState.Initing;
						joinDataManage.AssignJoinDataOrder(jpo);// joinDataManage
																// != null && 为空
					} catch (Exception e) {
						LOG.error("分配加载关联数据的任务失败：", e);
					}
				}
			}
		}
		// 周期：基准事件 间隔类型 间隔大小
		// 事件：SQL事件,ZK监听，文件监听。（轮询时间框架参数配置）)
	}

}
