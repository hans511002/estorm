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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.guava.collect.LinkedHashMultimap;
import org.apache.zookeeper.KeeperException;

import backtype.storm.utils.Utils;

import com.ery.estorm.config.ConfigListenThread;
import com.ery.estorm.config.Configuration;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.stormcluster.StormServerManage;
import com.ery.estorm.daemon.stormcluster.StormSupervisorTracker;
import com.ery.estorm.daemon.topology.NodeInfo;
import com.ery.estorm.daemon.topology.TopologyInfo;
import com.ery.estorm.daemon.topology.TopologyInfo.TopologyName;
import com.ery.estorm.executor.ExecutorService;
import com.ery.estorm.log.LogMag;
import com.ery.estorm.log.LogMag.LogNodeWriter;
import com.ery.estorm.socket.OrderHeader;
import com.ery.estorm.socket.OrderHeader.Order;
import com.ery.estorm.socket.OrderHeader.OrderSeq;
import com.ery.estorm.socket.SCServer.Handler;
import com.ery.estorm.util.KeyLocker;
import com.ery.estorm.util.Threads;
import com.ery.estorm.util.ToolUtil;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperListener;

/**
 * topology 生成及分配提交，去除服务端TopologyTracker NodeTracker
 */
public class AssignmentManager extends ZooKeeperListener {
	private static final Log LOG = LogFactory.getLog(AssignmentManager.class);

	final DaemonMaster master;
	final StormServerManage stormServer;
	final StormSupervisorTracker supervisorTracker;// storm work node

	final ServerManager serverManager;
	// final TopologyTracker topTracker;
	// private final TopologyManager topologyManager;

	final ExecutorService executorService;
	// Thread pool executor service for timeout monitor
	private java.util.concurrent.ExecutorService threadPoolExecutorService;
	// A bunch of ZK events workers. Each is a single thread executor service
	private final java.util.concurrent.ExecutorService zkEventWorkers;

	final private KeyLocker<String> locker = new KeyLocker<String>();
	ConfigListenThread configListen;
	// ===================================全局配置===========新生成分配的添加到此=====================================
	public Map<String, TopologyInfo> assignTops = new HashMap<String, TopologyInfo>();
	public Map<String, TopologyName> runTopNames = new HashMap<String, TopologyName>();
	// 全局spoout集合 包含mq 及node生成的mq ID>0 为MSG ID<0为nodemsg
	public Map<Long, NodeInfo> assignMQs = new HashMap<Long, NodeInfo>();
	// 全局bolt集合 nodeId唯一
	public Map<Long, NodeInfo> assignNodes = new HashMap<Long, NodeInfo>();// bolt
	float autoRebuildTopRatio = 0.2f;// 20%重生成top,当前未实现
	int autoRebuildTopBoltmqNum = 5;// 5 个bolt消息 重生成top

	String lastOrder;
	// ===================================================================================

	Configuration conf;
	public State state;

	public static enum State {
		RUNNING, //
		PAUSING, // 停止扫描配置库，不进行变更生效
	}

	public AssignmentManager(DaemonMaster master) throws KeeperException, IOException {
		super(master.getZooKeeper());
		this.master = master;
		this.serverManager = master.getServerManager();
		this.stormServer = master.getStormServer();
		// this.topologyManager = master.getTopologyManager();
		this.supervisorTracker = stormServer.getSupervisorTracker();

		// this.topTracker = master.getTopTracker();
		this.executorService = master.getExecutorService();
		conf = master.getConfiguration();
		autoRebuildTopRatio = conf.getFloat(EStormConstant.STORM_BOLT_TO_MQ_EXCESS, 0.2f);

		// Only read favored nodes if using the favored nodes load balancer.
		// This is the max attempts, not retries, so it should be at least 1.
		int maxThreads = conf.getInt(EStormConstant.ESTORM_ASSIGNMENT_THREADS, 2);
		this.threadPoolExecutorService = Threads.getBoundedCachedThreadPool(maxThreads, 60L, TimeUnit.SECONDS,
				Threads.newDaemonThreadFactory("AM.ASSIGN"));

		int workers = conf.getInt(EStormConstant.ESTORM_ASSIGNMENT_ZKEVENT_THREADS, 5);
		ThreadFactory threadFactory = Threads.newDaemonThreadFactory("AM.ZK.Worker");
		zkEventWorkers = Threads.getBoundedCachedThreadPool(workers, 60L, TimeUnit.SECONDS, threadFactory);
		configListen = new ConfigListenThread(watcher, master, this);
		configListen.setDaemon(true);
		state = State.PAUSING;
	}

	public void setState(State s) {
		state = s;
	}

	public List<TopologyInfo> getTopologyInfo() {
		List<TopologyInfo> runningTops = new ArrayList<TopologyInfo>();
		try {
			List<String> tops = ZKUtil.listChildrenAndWatchThem(watcher, watcher.estormTopologyZNode);
			for (String node : tops) {
				// TopologyName assignTopName =
				// TopologyName.parseServerName(ZKUtil.getNodeName(node));
				String topPath = ZKUtil.joinZNode(watcher.estormTopologyZNode, node);
				byte[] data = ZKUtil.getDataAndWatch(watcher, topPath);
				boolean needDel = true;
				if (data != null) {
					TopologyInfo topologyInfo = EStormConstant.castObject(data);
					if (topologyInfo != null) {
						runningTops.add(topologyInfo);
						needDel = false;
					}
				}
				if (needDel) {
					ZKUtil.deleteNode(watcher, topPath);
				}
			}
		} catch (IOException e) {
			master.abort("Unexpected zk exception getting TOP nodes", e);
		} catch (KeeperException e) {
			master.abort("Unexpected zk exception getting TOP nodes", e);
		}
		return runningTops;
	}

	// 从ZK初始化已经存在的top
	private void initTopInfo() {
		List<TopologyInfo> topInfos = getTopologyInfo();
		synchronized (this.assignTops) {
			assignTops.clear();
			assignMQs.clear();
			assignNodes.clear();
			for (TopologyInfo topInfo : topInfos) {
				assignTops.put(topInfo.assignTopName.topName, topInfo);
				List<NodeInfo> spouts = new ArrayList<NodeInfo>();
				List<NodeInfo> bolts = new ArrayList<NodeInfo>();
				// 从topInfo.st中反序列化spouts bolts
				TopologyInfo.deserializeNodeInfos(topInfo.st, spouts, bolts);
				for (NodeInfo nodeInfo : spouts) {
					assignMQs.put(nodeInfo.getNodeId(), nodeInfo);// 要从数据读取变更属性，需要复制
				}
				for (NodeInfo nodeInfo : bolts) {
					assignNodes.put(nodeInfo.getNodeId(), nodeInfo);
				}
			}
		}
	}

	// storm 运行节点删除时调用
	public boolean removeTop(TopologyInfo topInfo) {
		if (topInfo == null)
			return false;
		// if (!assignTops.containsKey(topInfo.assignTopName.topName)) {
		// return false;
		// }
		synchronized (assignTops) {
			synchronized (assignMQs) {
				synchronized (assignNodes) {
					if (assignTops.containsKey(topInfo.assignTopName.topName)) {
						TopologyInfo _topInfo = assignTops.get(topInfo.assignTopName.topName);
						if (_topInfo != null && !_topInfo.assignTopName.equals(topInfo.assignTopName)) {
							assignTops.remove(topInfo.assignTopName.topName);
						}
					}
					this.runTopNames.remove(topInfo.assignTopName.topName);
					List<NodeInfo> spouts = new ArrayList<NodeInfo>();
					List<NodeInfo> bolts = new ArrayList<NodeInfo>();
					// 从topInfo.st中反序列化spouts bolts
					TopologyInfo.deserializeNodeInfos(topInfo.st, spouts, bolts);
					for (NodeInfo nodeInfo : spouts) {
						if (assignMQs.containsKey(nodeInfo.getNodeId())) {
							NodeInfo _nodeInfo = assignMQs.get(nodeInfo.getNodeId());
							if (_nodeInfo != null) {
								if (_nodeInfo.stime != nodeInfo.stime) {
									continue;
								}
								assignMQs.remove(nodeInfo.getNodeId());// 要从数据读取变更属性，需要复制
								nodeInfo.deleteToZk(watcher);
							}
						}
					}
					for (NodeInfo nodeInfo : bolts) {
						if (assignNodes.containsKey(nodeInfo.getNodeId())) {
							NodeInfo _nodeInfo = assignNodes.get(nodeInfo.getNodeId());
							if (_nodeInfo != null) {
								if (_nodeInfo.stime != nodeInfo.stime) {
									continue;
								}
								assignNodes.remove(nodeInfo.getNodeId());
								nodeInfo.deleteToZk(watcher);
							}
						}
					}
				}
			}
		}
		return true;
	}

	public boolean putTop(TopologyInfo topInfo) {
		if (topInfo == null)
			return false;
		synchronized (assignTops) {
			synchronized (assignMQs) {
				synchronized (assignNodes) {
					// 从topInfo.st中反序列化spouts bolts
					assignTops.put(topInfo.assignTopName.topName, topInfo);// 不能修改，不需要复制
					List<NodeInfo> spouts = new ArrayList<NodeInfo>();
					List<NodeInfo> bolts = new ArrayList<NodeInfo>();
					TopologyInfo.deserializeNodeInfos(topInfo.st, spouts, bolts);

					for (NodeInfo nodeInfo : spouts) {
						assignMQs.put(nodeInfo.getNodeId(), nodeInfo);// 要从数据读取变更属性，需要复制
					}
					for (NodeInfo nodeInfo : bolts) {
						assignNodes.put(nodeInfo.getNodeId(), nodeInfo);
					}
				}
			}
		}
		return true;
	}

	private void invokeAssign(NodeInfo nodeInfo) {
		threadPoolExecutorService.submit(new NodeAssignCallable(this, nodeInfo));
	}

	private void invokeAssign(TopologyInfo topInfo) {
		threadPoolExecutorService.submit(new TopAssignCallable(this, topInfo));
	}

	private void invokeUnassign(TopologyInfo topInfo) {
		threadPoolExecutorService.submit(new TopUnAssignCallable(this, topInfo));
	}

	private void invokeAssign(NodeInfo nodeInfo, boolean force) {
		threadPoolExecutorService.submit(new NodeAssignCallable(this, nodeInfo, force));
	}

	private void invokeAssign(TopologyInfo topInfo, boolean force) {
		threadPoolExecutorService.submit(new TopAssignCallable(this, topInfo, force));
	}

	private void invokeUnassign(TopologyInfo topInfo, boolean force) {
		threadPoolExecutorService.submit(new TopUnAssignCallable(this, topInfo, force));
	}

	// TODO 当存在时是否重新提交,否则只变更其下的Node属性 提交TOP,写TOp ZK node zk
	public void assign(TopologyInfo tif, boolean force) throws IOException, KeeperException {

	}

	//
	public void unassign(TopologyInfo region, boolean force) {
	}

	// 变更NodeInfo属性,发送Node命令等 写ZK值
	public void assign(NodeInfo nodeInfo, boolean force) {

	}

	// We don't want to have two events on the same region managed
	// simultaneously.
	// For this reason, we need to wait if an event on the same region is
	// currently in progress.
	// So we track the region names of the events in progress, and we keep a
	// waiting list.
	private final Set<String> nodesInProgress = new HashSet<String>();
	// In a LinkedHashMultimap, the put order is kept when we retrieve the
	// collection back. We need
	// this as we want the events to be managed in the same order as we received
	// them.
	private final LinkedHashMultimap<String, TopOrderRunnable> zkEventWorkerWaitingList = LinkedHashMultimap.create();

	/**
	 * A specific runnable that works only on a region.
	 */
	private abstract class TopOrderRunnable implements Runnable {
		final String topName;

		public TopOrderRunnable(String topName) {
			this.topName = topName;
		}

		abstract String getTopName();
	}

	/**
	 * Submit a task, ensuring that there is only one task at a time that
	 * working on a given region. Order is respected.
	 */
	protected void zkEventWorkersSubmit(final TopOrderRunnable regRunnable) {
		synchronized (nodesInProgress) {
			// If we're there is already a task with this region, we add it to
			// the
			// waiting list and return.
			if (nodesInProgress.contains(regRunnable.getTopName())) {
				synchronized (zkEventWorkerWaitingList) {
					zkEventWorkerWaitingList.put(regRunnable.getTopName(), regRunnable);
				}
				return;
			}

			// No event in progress on this region => we can submit a new task
			// immediately.
			nodesInProgress.add(regRunnable.getTopName());
			zkEventWorkers.submit(new Runnable() {
				@Override
				public void run() {
					try {
						regRunnable.run();
					} finally {
						// now that we have finished, let's see if there is an
						// event for the same region in the
						// waiting list. If it's the case, we can now submit it
						// to the pool.
						synchronized (nodesInProgress) {
							nodesInProgress.remove(regRunnable.getTopName());
							synchronized (zkEventWorkerWaitingList) {
								java.util.Set<TopOrderRunnable> waiting = zkEventWorkerWaitingList.get(regRunnable
										.getTopName());
								if (!waiting.isEmpty()) {
									// We want the first object only. The only
									// way to get it is through an iterator.
									TopOrderRunnable toSubmit = waiting.iterator().next();
									zkEventWorkerWaitingList.remove(toSubmit.getTopName(), toSubmit);
									zkEventWorkersSubmit(toSubmit);
								}
							}
						}
					}
				}
			});
		}
	}

	// ZooKeeper events
	/**
	 * New unassigned node has been created.
	 */
	@Override
	public void nodeCreated(String path) {
		if (path.equals(watcher.estormAssignmentZNode)) {// 命令
			handleAssignmentEvent(path);
		}
	}

	/**
	 * Existing unassigned node has had data changed.
	 */
	@Override
	public void nodeDataChanged(String path) {
		if (path.equals(watcher.estormAssignmentZNode)) {// 命令
			handleAssignmentEvent(path);
		}
	}

	private void handleAssignmentEvent(String path) {
		try {
			if (path.equals(watcher.estormAssignmentZNode)) {// 命令
				byte[] data = ZKUtil.getDataAndWatch(watcher, path);
				if (data != null) {
					String order = new String(data);
					if (order.equals(lastOrder)) {
						return;
					} else {
						lastOrder = order;
					}
					if (order.startsWith("stop ")) {// stop deactivate activate
													// rebalance
						String pars[] = order.split(" ");
						final String topName = pars[1];
						TopologyInfo topInfo = this.assignTops.get(topName);
						if (topInfo != null) {
							final List<TopologyInfo> stopTops = new ArrayList<TopologyInfo>();
							final int waitSes;
							stopTops.add(topInfo);
							if (pars.length > 2) {
								waitSes = Integer.parseInt(pars[2]);
							} else {
								waitSes = 10;
							}
							zkEventWorkersSubmit(new TopOrderRunnable(topName) {
								@Override
								public void run() {
									try {
										StopTopologyNodes(stopTops, waitSes);
									} catch (Exception e) {
										LOG.error("执行命令失败：", e);
									}
								}

								@Override
								public String getTopName() {
									return topName;
								}
							});
						}
					} else if (order.startsWith("rebalance ")) {
						String pars[] = order.split(" ");
						final String topName = pars[1];
						final TopologyInfo topInfo = this.assignTops.get(topName);
						final int waitSes;
						final int newWorkders;
						if (pars.length > 2) {
							waitSes = Integer.parseInt(pars[2]);
						} else {
							waitSes = 10;
						}
						if (pars.length > 3) {
							newWorkders = Integer.parseInt(pars[3]);
						} else {
							newWorkders = 0;
						}
						final Map<String, Integer> parallelism = new HashMap<String, Integer>();
						for (int i = 4; i < pars.length; i++) {
							String tmp[] = pars[i].split("=");
							if (tmp.length == 2) {
								int num = (int) ToolUtil.toLong(tmp[1]);
								if (num > 0)
									parallelism.put(tmp[0], num);
							}
						}
						if (topInfo != null) {
							zkEventWorkersSubmit(new TopOrderRunnable(topName) {
								@Override
								public void run() {
									try {
										rebalance(topInfo, waitSes, newWorkders, parallelism);
									} catch (Exception e) {
										LOG.error("执行命令失败：", e);
									}
								}

								@Override
								public String getTopName() {
									return topName;
								}
							});
						}
					} else if (order.startsWith("activate ")) {
						String pars[] = order.split(" ");
						final String topName = pars[1];
						final TopologyInfo topInfo = this.assignTops.get(topName);
						if (topInfo != null) {
							zkEventWorkersSubmit(new TopOrderRunnable(topName) {
								@Override
								public void run() {
									try {
										activateTopology(topInfo);
										// stormServer.activateTopology(topInfo.assignTopName.topName);
									} catch (Exception e) {
										LOG.error("执行命令失败：", e);
									}
								}

								@Override
								public String getTopName() {
									return topName;
								}
							});
						}
					} else if (order.startsWith("deactivate ")) {
						String pars[] = order.split(" ");
						final String topName = pars[1];
						final TopologyInfo topInfo = this.assignTops.get(topName);
						final int waitSes;
						if (pars.length > 2) {
							waitSes = Integer.parseInt(pars[2]);
						} else {
							waitSes = 10;
						}
						if (topInfo != null) {
							zkEventWorkersSubmit(new TopOrderRunnable(topName) {
								@Override
								public void run() {
									try {
										deactivateTopology(topInfo, waitSes);
										// stormServer.deactivateTopology(topInfo.assignTopName.topName);
									} catch (Exception e) {
										LOG.error("执行命令失败：", e);
									}
								}

								@Override
								public String getTopName() {
									return topName;
								}
							});
						}

					}
				}
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void StopTopologyNodes(TopologyInfo topInfo, int waitSes) {
		List<TopologyInfo> stopTops = new ArrayList<TopologyInfo>();
		stopTops.add(topInfo);
		StopTopologyNodes(stopTops, waitSes);
	}

	// 停止TOP，等待Node返回
	public void StopTopologyNodes(List<TopologyInfo> needStopTops, int waitSes) {
		OrderHeader header = new OrderHeader();
		header.order = Order.stop;
		List<OrderHeader> resList = new ArrayList<OrderHeader>();
		int numRequest = sendOrderAndWait(header, resList, needStopTops, waitSes);
		if (resList.size() == numRequest) {
			LOG.info("所有运行节点客户端均成功返回状态");

		} else {
			LOG.info("存在客户端未成功返回状态，可能存在消息数据丢失");
			Utils.sleep(waitSes * 1000);
		}

		// 发送storm 的kill 命令
		List<Long> nodes = new ArrayList<Long>();
		for (TopologyInfo topInfo : needStopTops) {
			List<Long> ids = topInfo.getcontainsNodes();
			for (Long nodeId : ids) {
				if (nodeId > 0) {
					nodes.add(nodeId);
					LogMag.nodeSummarys.get(nodeId).STOP_TIME = System.currentTimeMillis();
				}
			}
			master.getStormServer().stopTopology(topInfo.getTopName().topName, waitSes);
		}
		LogNodeWriter.writeLog();
		for (Long nodeId : nodes) {
			LogMag.nodeSummarys.remove(nodeId);
		}
	}

	public void PauseTopology(TopologyInfo topInfo, int waitSes) {
		OrderHeader header = new OrderHeader();
		header.order = Order.stop;
		List<OrderHeader> resList = new ArrayList<OrderHeader>();
		List<TopologyInfo> orderTops = new ArrayList<TopologyInfo>();
		orderTops.add(topInfo);
		int numRequest = sendOrderAndWait(header, resList, orderTops, waitSes);
		if (resList.size() == numRequest) {
			LOG.info("所有运行节点客户端均成功返回状态");

		} else {
			LOG.info("存在客户端未成功返回状态，可能存在消息数据丢失");
			Utils.sleep(waitSes * 1000);
		}
	}

	public void deactivateTopology(TopologyInfo topInfo, int waitSes) {
		// PauseTopology(topInfo, waitSes);
		PauseTopology(topInfo, waitSes);
		OrderHeader header = new OrderHeader();
		header.order = Order.pause;
		List<OrderHeader> resList = new ArrayList<OrderHeader>();
		List<TopologyInfo> orderTops = new ArrayList<TopologyInfo>();
		orderTops.add(topInfo);
		int numRequest = sendOrderAndWait(header, resList, orderTops, waitSes);
		if (resList.size() == numRequest) {
			LOG.info("所有运行节点客户端均成功返回状态");

		} else {
			LOG.info("存在客户端未成功返回状态，可能存在消息数据丢失");
			Utils.sleep(waitSes * 1000);
		}
		master.getStormServer().deactivateTopology(topInfo.assignTopName.topName);
	}

	public void activateTopology(TopologyInfo topInfo) {
		master.getStormServer().activateTopology(topInfo.assignTopName.topName);
	}

	// Syntax: [storm rebalance topology-name [-w wait-time-secs] [-n
	// new-num-workers] [-e component=parallelism]*]
	public void rebalance(TopologyInfo topInfo, int waitSes, int newWorkders, Map<String, Integer> parallelism) {
		// PauseTopology(topInfo, waitSes);
		OrderHeader header = new OrderHeader();
		header.order = Order.pause;
		List<OrderHeader> resList = new ArrayList<OrderHeader>();
		List<TopologyInfo> orderTops = new ArrayList<TopologyInfo>();
		orderTops.add(topInfo);
		int numRequest = sendOrderAndWait(header, resList, orderTops, waitSes);
		if (resList.size() == numRequest) {
			LOG.info("所有运行节点客户端均成功返回状态");

		} else {
			LOG.info("存在客户端未成功返回状态，可能存在消息数据丢失");
			Utils.sleep(waitSes * 1000);
		}
		master.getStormServer().rebalance(topInfo.assignTopName.topName, waitSes, newWorkders, parallelism);
	}

	public int sendOrderAndWait(OrderHeader header, List<OrderHeader> resList, List<TopologyInfo> orderTops, int waitSes) {
		header.isRequest = true;
		header.fromHost = master.getServerName();
		for (TopologyInfo topInfo : orderTops) {
			for (String spoutName : topInfo.st.get_spouts().keySet()) {
				header.data.put(spoutName, false);
			}
			for (String spoutName : topInfo.st.get_bolts().keySet()) {
				header.data.put(spoutName, false);
				Long nodeId = Long.parseLong(spoutName.substring("node_".length()));
				synchronized (this.assignNodes) {
					this.assignNodes.remove(nodeId);// 提前删除要停止的Node节点，停止命令处还会删除一次
				}
			}
		}

		OrderSeq os = OrderSeq.createOrderSeq(header);
		synchronized (OrderHeader.orderResponse) {
			OrderHeader.orderResponse.put(os, resList);// 监听回复
		}
		int numRequest = 0;
		for (Handler hand : master.getScServer().handles) {
			if (hand.isClose || !hand.isSupervisor)
				continue;
			try {
				synchronized (hand.sc) {
					header.sendHeader(hand.sc.getOutputStream(), hand.outputBuffer);
					numRequest++;
				}
			} catch (IOException e) {
				e.printStackTrace();
				hand.close();
			}
		}
		long t = System.currentTimeMillis();
		while (true) {// 等待各TOP节点统计数据不再变化
			if (((double) resList.size() / (double) numRequest) > 0.9) {
				break;
			}
			if (((double) resList.size() / (double) numRequest) < 0.5) {// 至少一半返回
				t = System.currentTimeMillis();
			}
			if (System.currentTimeMillis() - t > 30000 + waitSes * 1000) {
				break;
			}
			Utils.sleep(100);
		}
		return numRequest;
	} //

	public void rebalance(List<TopologyInfo> needStopTops, int waitSes, int newWorkders[],
			Map<String, Integer> parallelism[]) throws IOException {
		OrderHeader header = new OrderHeader();
		header.order = Order.pause;
		List<OrderHeader> resList = new ArrayList<OrderHeader>();
		int numRequest = sendOrderAndWait(header, resList, needStopTops, waitSes);
		if (resList.size() == numRequest) {
			LOG.info("所有运行节点客户端均成功返回状态");

		} else {
			LOG.info("存在客户端未成功返回状态，可能存在消息数据丢失");
			Utils.sleep(waitSes * 1000);
		}
		// 发送storm 的kill 命令
		for (int i = 0; i < needStopTops.size(); i++) {
			TopologyInfo topInfo = needStopTops.get(i);
			master.getStormServer().rebalance(topInfo.assignTopName.topName, waitSes,
					newWorkders != null ? newWorkders[i] : 0, parallelism != null ? parallelism[i] : null);
		}
	}

	public void start() {
		while (master.getStormServer() == null || !master.getStormServer().isRunning) {
			Utils.sleep(100);
		}
		initTopInfo();
		configListen.start();
		state = State.RUNNING;
		try {
			ZKUtil.watchAndCheckExists(watcher, watcher.estormAssignmentZNode);// 启动集群命令监听
		} catch (Exception e) {
			LOG.error("监听集群命令节点错误：", e);
		}
	}

	public void stop() {
		if (configListen != null)
			configListen.stop();
		state = State.PAUSING;
	}

}
