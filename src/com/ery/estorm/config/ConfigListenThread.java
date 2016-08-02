package com.ery.estorm.config;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import storm.kafka.SpoutConfig;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.ery.estorm.NodeColumnCalc;
import com.ery.estorm.NodeTest;
import com.ery.estorm.client.node.BoltNode;
import com.ery.estorm.client.node.SpoutNode;
import com.ery.estorm.daemon.AssignmentManager;
import com.ery.estorm.daemon.DaemonMaster;
import com.ery.estorm.daemon.Server;
import com.ery.estorm.daemon.topology.NodeInfo;
import com.ery.estorm.daemon.topology.NodeInfo.DataStorePO;
import com.ery.estorm.daemon.topology.NodeInfo.FieldPO;
import com.ery.estorm.daemon.topology.NodeInfo.GroupMethod;
import com.ery.estorm.daemon.topology.NodeInfo.InOutPO;
import com.ery.estorm.daemon.topology.NodeInfo.JoinPO;
import com.ery.estorm.daemon.topology.NodeInfo.MsgFieldPO;
import com.ery.estorm.daemon.topology.NodeInfo.MsgPO;
import com.ery.estorm.daemon.topology.NodeInfo.NodeFieldPO;
import com.ery.estorm.daemon.topology.NodeInfo.NodeInput;
import com.ery.estorm.daemon.topology.NodeInfo.NodeJoinPO;
import com.ery.estorm.daemon.topology.NodeInfo.NodePO;
import com.ery.estorm.daemon.topology.NodeInfo.PushPO;
import com.ery.estorm.daemon.topology.TopologyInfo;
import com.ery.estorm.daemon.topology.TopologyInfo.TopologyName;
import com.ery.estorm.exceptions.NodeParseException;
import com.ery.estorm.handler.TopologySubmit;
import com.ery.estorm.join.JoinDataManage;
import com.ery.estorm.log.LogMag;
import com.ery.estorm.log.NodeLog.NodeSummaryLogPo;
import com.ery.estorm.socket.OrderHeader;
import com.ery.estorm.socket.OrderHeader.Order;
import com.ery.estorm.socket.SCServer;
import com.ery.estorm.util.ToolUtil;
import com.ery.estorm.zk.ZKAssign;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperNodeTracker;
import com.ery.estorm.zk.ZooKeeperWatcher;
import com.ery.base.support.jdbc.DataAccess;

public class ConfigListenThread extends Thread {
	public static final Log LOG = LogFactory.getLog(ConfigListenThread.class);

	Connection con;// 长连接
	Configuration conf;
	long listenInterval = 5000;
	public ConfigInfo configInfo;
	ConfigDBTracker confDbTracker;
	String configTime;
	String _configTime;
	public DataAccess access = new DataAccess();
	final DaemonMaster master;
	final AssignmentManager assignmentManager;
	final JoinDataManage joinDataManage;
	final ZooKeeperWatcher watcher;

	private SCServer scServer;
	boolean firstRun = true;

	public ConfigListenThread(ZooKeeperWatcher watcher, DaemonMaster master, AssignmentManager assignmentManager) {
		this.master = master;
		this.assignmentManager = assignmentManager;
		this.joinDataManage = master.getJoinDataManage();
		this.scServer = master.getScServer();
		this.watcher = watcher;
		joinDataManage.setListenThread(this);
		this.conf = master.getConfiguration();
		listenInterval = conf.getLong(EStormConstant.CONFIG_LISTEN_INTERVAL, 5000);
		configInfo = new ConfigInfo(conf);
		confDbTracker = new ConfigDBTracker(master.getZooKeeper(), master);
		if (confDbTracker.getData(false) == null) {
			confDbTracker.createConfigDbNode(configInfo.toString().getBytes());
		}
		NodeColumnCalc.start(confDbTracker);
	}

	private void updateLastModTime() {
		String path = watcher.estormConfigDbZNode + "/configTime";
		try {
			if (configTime == null) {// 检查在线Storm中和Top是否与分配的一致，不一致则删除对应TOP节点
				byte[] data = null;
				if (assignmentManager.assignNodes.size() > 0)
					if (ZKUtil.watchAndCheckExists(watcher, path)) {
						LOG.info("读取配置时间：" + path);
						data = ZKUtil.getData(watcher, path);
					}
				if (data == null) {
					configTime = EStormConstant.utsTiimeString;
					LOG.info("初始化,读取全部节点,配置时间：" + configTime);
				} else {
					configTime = new String(data);
					LOG.info("读取到的配置时间：" + configTime);
				}
			} else {
				if (_configTime.compareTo(configTime) > 0) {
					ZKUtil.createSetData(watcher, path, _configTime.getBytes());
					configTime = _configTime;
				}
			}
		} catch (Exception e) {
			LOG.error("更新配置读取时间异常:", e);
		}
	}

	@Override
	public void run() {
		// 从ZK初始化 configTime 集群重启要删除
		Thread.currentThread().setName("ConfigListen");
		LOG.info("睡眠" + listenInterval + "ms 扫描节点配置");
		Utils.sleep(listenInterval);
		updateLastModTime();
		while (!master.isStopped()) {
			String backCfgTime = configTime;
			try {
				// 检查在线Storm中和Top是否与分配的一致，不一致则删除对应TOP节点
				for (String topName : assignmentManager.assignTops.keySet()) {
					TopologyInfo asTop = assignmentManager.assignTops.get(topName);
					if (asTop != null) {
						if (!master.getStormServer().stormsTracker.stormInfos.containsKey(asTop.assignTopName.topName)) {// 存在节点，不在Storm中运行
							asTop.deleteToZk(watcher);
						}
					}
				}
				if (assignmentManager.state.equals(AssignmentManager.State.RUNNING)) {
					ConfigInfo newConfig = ConfigInfo.pasreConofig(confDbTracker.getString());
					if (newConfig != null && !newConfig.equals(configInfo)) {
						close();
						configInfo = newConfig;
					}
					synchronized (access) {
						open();
						_configTime = configTime;
						processNewNode();
						updateLastModTime();
					}
				}
			} catch (Exception e) {
				configTime = backCfgTime;
				LOG.error("读取配置，启动TOP异常：", e);
			} finally {
				synchronized (access) {
					close();
				}
				Utils.sleep(listenInterval);
			}
		}
	}

	public void processNewNode() throws SQLException, IOException, KeeperException {
		// 扫描数据库表配置
		List<NodePO> boltNodes = EStormConfigRead.readBoltNodes(access, configTime);
		List<JoinPO> modJoinTabs = new ArrayList<JoinPO>();
		List<NodePO> refNodes = new ArrayList<NodePO>();// 未修改，被引用
		if (boltNodes.size() == 0) {
			firstRun = false;
			return;
		}
		String nodeIds = "";
		for (NodePO po : boltNodes) {
			if (nodeIds.length() > 0)
				nodeIds += ",";
			nodeIds += po.NODE_ID;
			if (po.MODIFY_TIME.compareTo(_configTime) > 0) {
				_configTime = po.MODIFY_TIME;
			}
		}
		List<NodeInput> nodeInputs = EStormConfigRead.readNodeInputs(access, nodeIds);
		String msgIds = "";
		for (NodePO nodePo : boltNodes) {
			nodePo.inputs = new ArrayList<NodeInput>();
			boolean find = false;
			for (NodeInput input : nodeInputs) {
				if (find == false && input.NODE_ID != nodePo.NODE_ID)
					continue;
				else if (find == true && input.NODE_ID != nodePo.NODE_ID)
					break;
				if (input.INPUT_TYPE == 0) {
					if (msgIds.length() > 0)
						msgIds += ",";
					msgIds += input.INPUT_SRC_ID;
				}
				nodePo.inputs.add(input);
				find = true;
			}
		}
		List<MsgPO> msgs = new ArrayList<MsgPO>();
		if (!msgIds.equals(""))
			msgs = EStormConfigRead.readMsgNodes(access, msgIds);
		HashMap<Long, MsgPO> msgPORel = EStormConstant.converToMap(msgs);
		HashMap<Long, NodePO> nodePORel = EStormConstant.converToMap(boltNodes);
		// new HashMap<Long, NodePO>();
		// for (NodePO npo : boltNodes) {
		// nodePORel.put(npo.NODE_ID, npo);
		// }
		// for (MsgPO mpo : msgs) {
		// msgPORel.put(mpo.MSG_ID, mpo);
		// }
		List<NodePO> nullInput = new ArrayList<NodePO>();
		for (NodePO nodePo : boltNodes) {
			boolean haveNullinput = false;
			for (NodeInput input : nodePo.inputs) {
				convertNodeInput(input, refNodes, msgPORel, nodePORel);
				if (input.inputPo == null) {
					haveNullinput = true;
					LOG.error("删除节点[" + nodePo.NODE_ID + "]: 配置错误，输入配置[" + input.INPUT_ID + "]对应的输入对象[" +
							input.INPUT_SRC_ID + "]不存在");
				}
			}
			if (haveNullinput) {
				nullInput.add(nodePo);
				nodePORel.remove(nodePo.getNodeId());
			}
		}
		for (NodePO nodePo : boltNodes) {
			if (nodePo.pushInfos != null && nodePo.pushInfos.size() > 0)
				for (InOutPO push : nodePo.pushInfos) {
					checkNodePush(nodePo, (PushPO) push);
				}
		}
		boltNodes.removeAll(nullInput);
		nullInput.clear();
		if (boltNodes.size() == 0)
			return;
		// 关联配置
		List<NodeJoinPO> nodeJoins = EStormConfigRead.readNodeJoinRel(access, nodeIds);
		String joinIds = "";
		if (nodeJoins != null && nodeJoins.size() > 0) {
			for (NodePO nodePo : boltNodes) {
				nodePo.joins = new ArrayList<NodeJoinPO>();
				boolean find = false;
				for (NodeJoinPO joinPO : nodeJoins) {
					if (find == false && joinPO.NODE_ID != nodePo.NODE_ID)
						continue;
					else if (find == true && joinPO.NODE_ID != nodePo.NODE_ID)
						break;
					if (joinIds.length() > 0)
						joinIds += ",";
					joinIds += joinPO.JOIN_ID;
					joinPO.joinPO = null;// joinDataManage.joins.get(joinPO.JOIN_ID).clone();//
											// 复制一份，写入ZK，让JOIN监听更新数据
					nodePo.joins.add(joinPO);
				}
			}
			// 读取关联表数据加载
			List<JoinPO> joinTabs = EStormConfigRead.readJoins(access, joinIds);
			HashMap<Long, JoinPO> joinPORel = EStormConstant.converToMap(joinTabs);
			for (NodePO nodePo : boltNodes) {
				for (NodeJoinPO joinPO : nodePo.joins) {
					JoinPO jpo = joinPORel.get(joinPO.JOIN_ID);
					try {
						if (jpo != null)
							jpo.init();
					} catch (NodeParseException e) {
						jpo = null;
					}
					if (jpo == null || joinPO.PAR_JOIN_FIELD == null || joinPO.PAR_JOIN_FIELD.trim().equals("")) {
						nullInput.add(nodePo);
						nodePORel.remove(nodePo.getNodeId());
						break;
					}
					joinPO.joinPO = jpo;
					jpo = joinDataManage.joins.get(joinPO.JOIN_ID);// 写入ZK，让JOIN监听更新数据
					// 比对判断 类型：modifyType
					if (jpo == null) {
						joinPO.joinPO.modifyType = 1;// 新增
					} else {// 比对关联 规则修改，是否需要重新加载生效
						joinPO.joinPO.modifyType = joinPO.joinPO.CompareBase(jpo);
					}
					if (joinPO.joinPO.modifyType != 0) {
						modJoinTabs.add(joinPO.joinPO);
					}
				}
			}
			boltNodes.removeAll(nullInput);
			nullInput.clear();
			if (boltNodes.size() == 0)
				return;
		}
		// 节点存储读取
		List<DataStorePO> nodeStores = EStormConfigRead.readDataStores(access, nodeIds);
		if (nodeStores != null && nodeStores.size() > 0) {
			for (NodePO nodePo : boltNodes) {
				boolean find = false;
				for (DataStorePO storePO : nodeStores) {
					if (find == false && storePO.NODE_ID != nodePo.NODE_ID)
						continue;
					else if (find == true && storePO.NODE_ID != nodePo.NODE_ID)
						break;
					if (nodePo.storeInfos == null)
						nodePo.storeInfos = new ArrayList<InOutPO>();
					nodePo.storeInfos.add(storePO);
					find = true;
				}
			}
		}
		// 节点订阅
		List<PushPO> nodePushs = EStormConfigRead.readNodePushs(access, nodeIds);
		if (nodePushs != null && nodePushs.size() > 0) {
			for (NodePO nodePo : boltNodes) {
				boolean find = false;
				for (PushPO pushPO : nodePushs) {
					if (find == false && pushPO.NODE_ID != nodePo.NODE_ID)
						continue;
					else if (find == true && pushPO.NODE_ID != nodePo.NODE_ID)
						break;
					if (nodePo.pushInfos == null)
						nodePo.pushInfos = new ArrayList<InOutPO>();
					nodePo.pushInfos.add(pushPO);
					find = true;
				}
			}
		}
		for (NodePO npo : boltNodes) {
			try {
				npo.init();// 初始化节点，解析内部规则
			} catch (NodeParseException e) {
				LOG.error("节点[" + npo.getNodeId() + "]初始解析失败,移除对应节点：", e);
				nodePORel.remove(npo.getNodeId());
				nullInput.add(npo);
			}
		}
		boltNodes.removeAll(nullInput);
		nullInput.clear();
		/**
		 * 级联删除依赖节点不存在的节点
		 */
		for (NodePO nodePo : boltNodes) {
			deleteNodeException(boltNodes, nodePORel, nodePo);
		}
		HashMap<Long, NodePO> _nodePORel = new HashMap<Long, NodePO>();
		// 先判断 是否有特殊变更的NODE，有则先停止对应的TOP，重新生成TOP让变更生效
		List<NodePO> modNodePos = compareNodes(boltNodes);// 输出字段数有变更 输入有变更
															// 移除无变更在运行的
		List<TopologyInfo> needStopTops = null;
		// 找到对应的需要发送停止命令的TOp
		needStopTops = getNeedStopTops(modNodePos, boltNodes);// 查找依赖当前节点的已经在运行的节点

		if (needStopTops != null && needStopTops.size() > 0)
			_nodePORel = parseStopTopologyNodes(needStopTops, boltNodes, nodePORel);// 将需要停止掉的NOde添加到boltNodes

		// 重新处理节点关联关系，可能存在依赖的节点在运行，但是需要某些节点更新停止TOP，一起停止了，需要还原
		if (_nodePORel != null && _nodePORel.size() > 0) {
			for (NodePO nodePo : boltNodes) {
				boolean haveInputRecovert = false;
				for (NodeInput input : nodePo.inputs) {
					long nodeId = 0 - input.inputPo.getNodeId();// 还原
					if (input.INPUT_TYPE == 1 && nodeId == input.INPUT_SRC_ID) {// 关联的node
						if (_nodePORel.containsKey(input.INPUT_SRC_ID)) {// 查找新的节点依赖老的节点，重新初始化新节点
							NodePO npo = _nodePORel.get(input.INPUT_SRC_ID);
							input.inputPo = npo;
							refNodes.remove(npo);// 已经删除，不再需要更新
							haveInputRecovert = true;
							// for (InOutPO ipo : npo.pushInfos) {
							// if (ipo.getNodeId() == 0 - npo.NODE_ID)
							// npo.pushInfos.remove(ipo);
							// }
						}
					}
				}
				if (haveInputRecovert)// 重新初始化新节点
					nodePo.reInit();
				// 查找老的节点是否依赖新修改的节点，重新初始化老节点

			}
		}

		// 构建top，打印输出无法正常启动(输入不满足，规则配置存在问题)的节点
		Map<Long, NodeInfo> nodeRel = new HashMap<Long, NodeInfo>();
		List<NodePO> cp = new ArrayList<NodePO>(boltNodes);
		List<TopologyInfo> tops = buildTopology(cp, nodeRel);
		// 存在需要停止的top
		if (needStopTops != null && needStopTops.size() > 0) {
			for (TopologyInfo ntop : tops) {
				for (TopologyInfo otop : needStopTops) {
					if (ntop.assignTopName.topName.equals(otop.assignTopName.topName)) {
						break;
					}
				}
			}
			// 找到对应的TOp 发送停止命令
			this.assignmentManager.StopTopologyNodes(needStopTops, 30);
			// 停止Tops 等待结束
		}
		// 引用节点，只需要更新，捕获到后添加个订阅事件
		submitRefNodes(refNodes, _nodePORel);
		// 写TOP Zk数据// 必需先写TOP后写NODE数据
		submitTopology(tops);
		// 提交关联
		submitJoinNodes(modJoinTabs);
		// 更新节点
		for (NodePO npo : boltNodes) { // 添加ZK
			LOG.info("更新节点：" + npo.NODE_ID + " path:" +
					ZKUtil.joinZNode(watcher.estormNodeZNode, nodeRel.get(npo.getNodeId()).nodeName));
			nodeRel.get(npo.getNodeId()).updateToZk(this.watcher);
			// 为节点生成运行日志ID
			NodeSummaryLogPo nlog = LogMag.nodeSummarys.get(npo.NODE_ID);
			if (nlog == null) {
				nlog = new NodeSummaryLogPo();
				synchronized (LogMag.nodeSummarys) {
					LogMag.nodeSummarys.put(npo.NODE_ID, nlog);
				}
			}
			nlog.LOG_ID = EStormConfigRead.readMaxSeq(access, conf, "ST_NODE_LOG", "LOG_ID") + 1;
			nlog.NODE_ID = npo.NODE_ID;
			nlog.USE_TIME = 0;
			nlog.START_TIME = System.currentTimeMillis();// 启动时间
			nlog.STOP_TIME = System.currentTimeMillis();// 停止日志
			nlog.INPUT_TOTAL_NUM = 0;// 输入记录数
			nlog.OUTPUT_TOTAL_NUM = 0;// 输出记录总数
			nlog.PUSH_NUM = 0;// 对外Push记录总次数
			// 统计每个输入源的记录数，JSON
			nlog.INPUT_SRC_TOTAL_NUM.clear();
			// 对外PUSH每个源记录数
			nlog.PUSH_OUT_TOTAL_NUM.clear();
			nlog.HOST_NAME_SUM.clear();
			nlog.insertLog(access);
		}
		startTopology(tops);
	}

	private void checkNodePush(NodePO npo, PushPO push) {
		// TODO Auto-generated method stub
		if (push.pushMsgType == 1)// 增量
			npo.outputMsgType = npo.outputMsgType | 4;
	}

	// 调用外部命令启动TOP
	private void startTopology(List<TopologyInfo> tops) {
		String stormCommand = this.master.getStormServer().getStormCommand();
		String estormHome = conf.get(EStormConstant.ESTORM_HOME, System.getenv("ESTORM_HOME"));

		for (TopologyInfo top : tops) {
			String path = ZKUtil.joinZNode(watcher.estormTopologyZNode, top.assignTopName.toString());
			String command = stormCommand + " jar " + estormHome + "/estorm-exec.jar " +
					TopologySubmit.class.getCanonicalName() + " " + path + "";
			// command = estormHome + "/t.sh \"" + path + "\"";
			int retry = 0;
			while (retry++ < 3) {
				try {
					int res = -1;
					if ((EStormConstant.DebugType & 2) == 2) {
						res = NodeTest.runTop(path);
					} else if ((EStormConstant.DebugType & 4) == 4) {
						res = startTopology(top, command);
					} else {
						res = TopologySubmit.runTop(top);
					}
					switch (res) {
					case 0: {
						LOG.info("执行外部命令启动TOP[" + top.assignTopName + "]成功:" + command);
						LOG.info("等待Storm停止");
						long StopTime = System.currentTimeMillis();
						Utils.sleep(1000);
						while (true) {
							synchronized (master.getStormServer().stormsTracker.stormInfos) {
								if (master.getStormServer().stormsTracker.stormInfos
										.containsKey(top.assignTopName.topName)) {
									break;
								}
							}
							if (System.currentTimeMillis() - StopTime > 60000) {
								LOG.info("等待Storm超时60秒，退出等待");
							}
							Utils.sleep(100);
						}
						break;
					}
					case 1:
						LOG.info("执行外部命令启动TOP[" + top.assignTopName + "]失败,ZK数据丢失,重试第" + retry + "次");
						try {
							top.updateToZk(watcher);
						} catch (IOException e) {
							LOG.error("重写TOP数据异常:", e);
						} catch (KeeperException e) {
							LOG.error("重写TOP数据异常:", e);
						}
						break;
					case 2:
						LOG.info("执行外部命令启动TOP[" + top.assignTopName + "]失败,已经存在相同的TOP,等待10秒重试:" + command);
						Utils.sleep(10000);
						break;
					case 3:
						LOG.info("执行外部命令启动TOP[" + top.assignTopName + "]失败,TOp非法,重试第" + retry + "次");
						try {
							top.updateToZk(watcher);
						} catch (IOException e) {
							LOG.error("重写TOP数据异常:", e);
						} catch (KeeperException e) {
							LOG.error("重写TOP数据异常:", e);
						}
						break;
					}
					if (res == 0)
						break;
				} catch (Exception e) {
					if (retry == 3) {
						try {
							ZKUtil.deleteNode(watcher, path);
						} catch (Exception e1) {
							LOG.error("", e1);
						}
					} else {
						LOG.error("执行TOP启动命令异常：", e);
					}
				}
			}
		}
	}

	private int startTopology(TopologyInfo top, String command) {
		try {
			LOG.info("执行外部命令,启动TOP:" + command);
			Process proc = ToolUtil.runProcess(command, LOG);
			InputStream in = proc.getInputStream();
			DataInputStream sin = new DataInputStream(in);
			DataInputStream err = new DataInputStream(proc.getErrorStream());
			String line = null;
			String topName = top.assignTopName.topName;
			while ((line = sin.readLine()) != null) {
				LOG.info("[start:" + topName + "]" + line);
			}
			while ((line = err.readLine()) != null) {
				LOG.error("[start:" + topName + "]" + line);
			}
			int res = proc.waitFor();
			proc.destroy();
			return res;
		} catch (IOException e) {
			LOG.error("执行外部命令启动TOP[" + top.assignTopName + "]发生异常:" + command, e);
			return -1;
		} catch (InterruptedException e) {
			LOG.error("执行外部命令启动TOP[" + top.assignTopName + "]发生异常:" + command, e);
			return -1;
			// } catch (AlreadyAliveException e) {
			// e.printStackTrace();
			// return -1;
			// } catch (InvalidTopologyException e) {
			// e.printStackTrace();
			// return -1;
		}
	}

	// 提交TOP,写TOp ZK
	private void submitTopology(List<TopologyInfo> tops) throws IOException, KeeperException {
		for (TopologyInfo top : tops) {
			// assignmentManager.assign(top, true);
			top.updateToZk(this.watcher);

		}
	}

	// 找到对应的TOp modNodePos:全是boltNode
	private List<TopologyInfo> getNeedStopTops(List<NodePO> modNodePos, List<NodePO> leanNodePos) {
		List<TopologyInfo> res = new ArrayList<TopologyInfo>();
		if ((modNodePos == null || modNodePos.size() == 0) && (leanNodePos == null || leanNodePos.size() == 0)) {// 存在需要停止的节点
			return res;
		}

		for (String topName : assignmentManager.assignTops.keySet()) {
			TopologyInfo topInfo = assignmentManager.assignTops.get(topName);
			for (NodePO node : modNodePos) {// 修改需要停止的
				if (topInfo.containsNode("node_" + node.getNodeId())) {
					res.add(topInfo);
					break;
				}
			}
			for (NodePO node : leanNodePos) {// 依赖变更需要停止的
				if (topInfo.leanNode(node)) {
					res.add(topInfo);
					break;
				}
			}
		}
		return res;
	}

	// 停止Tops 等待结束,将停止掉的NOde添加到boltNodes 删除TOP 节点
	private HashMap<Long, NodePO> parseStopTopologyNodes(List<TopologyInfo> needStopTops, List<NodePO> boltNodes,
			HashMap<Long, NodePO> nodePORel) {
		// 发送停止命令
		HashMap<Long, NodePO> oldNodeRel = new HashMap<Long, NodePO>();
		OrderHeader header = new OrderHeader();
		header.isRequest = true;
		header.order = Order.stop;
		header.fromHost = master.getServerName();
		for (TopologyInfo topInfo : needStopTops) {
			for (String spoutName : topInfo.st.get_spouts().keySet()) {
				header.data.put(spoutName, false);
			}
			for (String spoutName : topInfo.st.get_bolts().keySet()) {
				header.data.put(spoutName, false);
				Long nodeId = Long.parseLong(spoutName.substring("node_".length()));
				synchronized (assignmentManager.assignNodes) {
					NodeInfo nfo = assignmentManager.assignNodes.remove(nodeId);// 删除要停止的Node节点
					if (!nodePORel.containsKey(nodeId)) {
						NodePO npo = (NodePO) nfo.getNode();
						boltNodes.add(npo);
						oldNodeRel.put(nodeId, npo);
					}
				}
			}
		}
		return oldNodeRel;
	}

	// TODO 输出字段数有变更 输入有变更 移除无变正在运行的
	private List<NodePO> compareNodes(List<NodePO> boltNodes) {
		List<NodePO> modPos = new ArrayList<NodePO>();
		List<NodePO> delPos = new ArrayList<NodePO>();
		for (NodePO npo : boltNodes) {
			if (firstRun) {
				if (assignmentManager.assignNodes.containsKey(npo.getNodeId())) {
					delPos.add(npo);
					break;
				}
			}
			this.master.getStormServer();
			if (assignmentManager.assignNodes.containsKey(npo.getNodeId())) {
				// 比对，未实现比对前默认有变更就重新节点
				// if (false) {
				modPos.add(npo);
				//
				// }
			}
		}
		boltNodes.removeAll(delPos);
		return modPos;
	}

	private List<TopologyInfo> buildTopology(List<NodePO> boltNodes, Map<Long, NodeInfo> nodeRel) {
		HashMap<Long, NodePO> allNodePORel = EStormConstant.converToMap(boltNodes);
		HashMap<String, Object> addedRel = new HashMap<String, Object>();
		List<TopologyInfo> tops = new ArrayList<TopologyInfo>();
		// 解析节点
		while (boltNodes.size() > 0) {// 存在未添加的节点
			NodePO np = boltNodes.remove(0);
			NodeInfo node = new NodeInfo(np);
			node.nodeName = ("node_" + np.NODE_ID);// 名称前缀区分// msg join node
			node.nodeId = np.NODE_ID;
			node.nodeType = 2;// 1spout 2bolt
			nodeRel.put(np.NODE_ID, node);
			TopologyBuilder builder = new TopologyBuilder();
			TopologyInfo top = new TopologyInfo();
			top.assignTopName = new TopologyName(np.NODE_ID + "_");// 从SP
																	// BL中生成名称

			BoltDeclarer bd = builder.setBolt(node.nodeName, new BoltNode(conf, node), np.MAX_PARALLELISM_NUM);
			bd.setMaxSpoutPending(np.MAX_PENDING);
			// 防止重复添加
			addedRel.put(node.nodeName, bd);
			// 向上找输入依赖
			buildNodeInput(builder, bd, node, boltNodes, nodeRel, allNodePORel, addedRel, top);
			// 向下找输出依赖
			buildNodeOutput(builder, bd, node, boltNodes, nodeRel, allNodePORel, addedRel, top);
			top.st = builder.createTopology();
			top.assignTopName.topName = "top_" +
					top.assignTopName.topName.substring(0, top.assignTopName.topName.length() - 1);
			tops.add(top);
			if (LOG.isDebugEnabled())
				LOG.debug("生成TOP:" + top);
		}
		return tops;
	}

	private void buildNodeOutput(TopologyBuilder builder, BoltDeclarer bd, NodeInfo nowNode, List<NodePO> boltNodes,
			Map<Long, NodeInfo> nodeRel, HashMap<Long, NodePO> allNodePORel, HashMap<String, Object> addedRel,
			TopologyInfo top) {
		NodePO np = (NodePO) nowNode.node;
		List<NodePO> _boltNodes = new ArrayList<NodePO>(boltNodes);
		for (NodePO npo : _boltNodes) {
			for (NodeInput input : npo.inputs) {
				if (input.INPUT_TYPE == 1 && input.INPUT_SRC_ID == np.NODE_ID) {// npo
																				// 依赖
																				// nowNode
					NodeInfo node = new NodeInfo(npo);
					node.nodeName = ("node_" + npo.NODE_ID);// 名称前缀区分// msg join
															// node
					if (addedRel.containsKey(node.nodeName))// 防止重复添加 多节点引用一个消息
						continue;
					synchronized (boltNodes) {
						boltNodes.remove(npo);
					}
					node.nodeId = npo.NODE_ID;
					node.nodeType = 2;// 1spout 2bolt
					nodeRel.put(npo.NODE_ID, node);
					if (top.workerNum < npo.WORKER_NUM) {
						top.workerNum = npo.WORKER_NUM;
					}
					BoltDeclarer pbd = builder
							.setBolt(node.nodeName, new BoltNode(conf, node), npo.MAX_PARALLELISM_NUM);
					pbd.setMaxSpoutPending(npo.MAX_PENDING);
					addedRel.put(node.nodeName, pbd);
					top.assignTopName.topName += npo.NODE_ID + "_";
					if (input.groupFields != null && input.groupFields.size() > 0)
						pbd.fieldsGrouping(nowNode.nodeName, nowNode.nodeName + "_" + input.MSG_TYPE, new Fields(
								input.groupFields));
					else
						pbd.shuffleGrouping(nowNode.nodeName, nowNode.nodeName + "_" + input.MSG_TYPE);
					buildNodeInput(builder, pbd, node, boltNodes, nodeRel, allNodePORel, addedRel, top);// 递归向上
					buildNodeOutput(builder, pbd, node, boltNodes, nodeRel, allNodePORel, addedRel, top);// 递归向下
				}
			}
		}
	}

	private void buildNodeInput(TopologyBuilder builder, BoltDeclarer bd, NodeInfo nowNode, List<NodePO> boltNodes,
			Map<Long, NodeInfo> nodeRel, HashMap<Long, NodePO> allNodePORel, HashMap<String, Object> addedRel,
			TopologyInfo top) {
		NodePO np = (NodePO) nowNode.node;
		for (NodeInput input : np.inputs) {
			if (input.INPUT_TYPE == 0 || input.inputPo instanceof MsgPO) {// msg
																			// INPUT_TYPE==0
																			// 一定是MsgPO
				MsgPO mpo = (MsgPO) input.inputPo;
				NodeInfo node = new NodeInfo(mpo);
				node.nodeName = ("msg_" + mpo.MSG_ID);// 名称前缀区分// msg join node
				if (addedRel.containsKey(node.nodeName))// 防止重复添加 多节点引用一个消息
					continue;
				node.nodeId = mpo.MSG_ID;
				node.nodeType = 1;// 1spout 2bolt
				SpoutConfig spoutConf = SpoutNode.createSpoutConfig(conf, mpo, node.nodeName);
				SpoutNode spout = new SpoutNode(conf, spoutConf, node);
				SpoutDeclarer sd = builder.setSpout(node.nodeName, spout,
						MAX(input.MAX_PARALLELISM_NUM, mpo.MAX_PARALLELISM_NUM));
				sd.setMaxSpoutPending(input.MAX_PENDING);
				addedRel.put(node.nodeName, sd);
				if (input.groupFields != null && input.groupFields.size() > 0)
					bd.fieldsGrouping(node.nodeName, node.nodeName, new Fields(input.groupFields));
				else
					bd.shuffleGrouping(node.nodeName, node.nodeName);
			} else if (input.INPUT_TYPE == 1) {// node
				NodePO npo = (NodePO) input.inputPo;
				NodeInfo node = new NodeInfo(npo);
				node.nodeName = ("node_" + npo.NODE_ID);// 名称前缀区分// msg join
														// node
				if (addedRel.containsKey(node.nodeName))// 防止重复添加 多节点引用一个消息
					continue;
				synchronized (boltNodes) {
					boltNodes.remove(npo);
				}
				node.nodeId = npo.NODE_ID;
				node.nodeType = 2;// 1spout 2bolt
				nodeRel.put(npo.NODE_ID, node);
				if (top.workerNum < npo.WORKER_NUM) {
					top.workerNum = npo.WORKER_NUM;
				}
				BoltDeclarer pbd = builder.setBolt(node.nodeName, new BoltNode(conf, node), npo.MAX_PARALLELISM_NUM);
				pbd.setMaxSpoutPending(npo.MAX_PENDING);
				addedRel.put(node.nodeName, pbd);
				top.assignTopName.topName = npo.NODE_ID + "_" + top.assignTopName.topName;
				if (input.groupFields != null && input.groupFields.size() > 0) {
					bd.fieldsGrouping(node.nodeName, node.nodeName + "_" + input.MSG_TYPE,
							new Fields(input.groupFields));
				} else {
					bd.shuffleGrouping(node.nodeName, node.nodeName + "_" + input.MSG_TYPE);
				}
				buildNodeInput(builder, pbd, node, boltNodes, nodeRel, allNodePORel, addedRel, top);// 递归向上
				buildNodeOutput(builder, pbd, node, boltNodes, nodeRel, allNodePORel, addedRel, top);// 递归向下
			}
		}
	}

	static int MAX(int l, int r) {
		return l > r ? l : r;
	}

	// 删除依赖节点node的节点，级联删除
	private void deleteNodeException(List<NodePO> boltNodes, HashMap<Long, NodePO> nodePORel, NodePO node) {
		for (NodeInput input : node.inputs) {
			if (input.INPUT_TYPE == 1) {
				NodeInfo nfo = assignmentManager.assignNodes.get(input.INPUT_SRC_ID);
				if (nfo == null) {// 依赖未在运行的节点
					if (nodePORel.containsKey(input.INPUT_SRC_ID) == false) {// 不存在
						boltNodes.remove(node);
					}
				} else {
					if (nodePORel.containsKey(input.INPUT_SRC_ID)) {// 同时存在
						// 变更 能正确初始化，说明变更规则无问题
						deleteNodeException(boltNodes, nodePORel, nodePORel.get(input.INPUT_SRC_ID));
					}
				}
			}
		}
	}

	// 直接生成nodeinfo写ZK
	private void submitRefNodes(List<NodePO> refNodes, HashMap<Long, NodePO> ndoeInReSubmitRel) {
		for (NodePO npo : refNodes) {
			if (ndoeInReSubmitRel.containsKey(npo.NODE_ID)) {
				continue;
			}
			try {
				// 提交变更，更新ZK数据
				ZooKeeperWatcher zkw = master.getZooKeeper();
				NodeInfo ni = assignmentManager.assignNodes.get(npo.NODE_ID);
				if (ni != null) {
					ni.node = npo;
					ZKUtil.createSetData(zkw, ZKAssign.getNodePath(zkw, ni.nodeName), EStormConstant.Serialize(ni));
				}
			} catch (Exception e) {
				LOG.error("提交更新节点[" + npo.NODE_ID + "]", e);
			}
		}
	}

	// 直接生成nodeinfo写ZK
	private void submitJoinNodes(List<JoinPO> modJoinTabs) throws IOException, KeeperException {
		JoinDataManage jData = master.getJoinDataManage();
		for (JoinPO jpo : modJoinTabs) {
			jData.AssignJoinDataOrder(jpo);
		}
	}

	// 解析转换节点输入配置信息
	private void convertNodeInput(NodeInput input, List<NodePO> refNodes, HashMap<Long, MsgPO> msgPORel,
			HashMap<Long, NodePO> nodePORel) {
		if (input.INPUT_TYPE == 0) {// 0，消息队列
			NodeInfo nfo = assignmentManager.assignMQs.get(input.INPUT_SRC_ID);
			if (nfo != null) {
				InOutPO ipo = nfo.getNode();
				assert (ipo instanceof MsgPO);
				MsgPO mpo = (MsgPO) ipo;
				if (input.MAX_PARALLELISM_NUM > mpo.MAX_PARALLELISM_NUM)
					mpo.MAX_PARALLELISM_NUM = input.MAX_PARALLELISM_NUM;// 设置新的最大并行值
				input.inputPo = mpo;
			} else {
				MsgPO mpo = msgPORel.get(input.INPUT_SRC_ID);
				input.inputPo = mpo;
			}
		} else if (input.INPUT_TYPE == 1) {// 1，节点
			NodeInfo nfo = assignmentManager.assignNodes.get(input.INPUT_SRC_ID);
			if (nfo != null && nodePORel.containsKey(input.INPUT_SRC_ID) == false) {// 已经在运行,并在修改更新列表中没有
				InOutPO ipo = nfo.getNode();
				assert (ipo instanceof NodePO);
				NodePO npo = (NodePO) ipo;
				NodeInfo mnf = assignmentManager.assignMQs.get(0 - input.INPUT_SRC_ID);// 节点对应的消息
				if (mnf != null) {// 已经存在临时消息订阅
					ipo = nfo.getNode();
					assert (ipo instanceof MsgPO);
					MsgPO mpo = (MsgPO) ipo;
					if (input.MAX_PARALLELISM_NUM > mpo.MAX_PARALLELISM_NUM)
						mpo.MAX_PARALLELISM_NUM = input.MAX_PARALLELISM_NUM;// 设置新的最大并行值
					input.inputPo = mpo;
				} else {
					// 向nfo中增加一个kafka的消息订阅
					boolean haveGroupByCalc = false;
					for (int i = 0; i < npo.fields.size(); i++) {
						NodeFieldPO nfpo = (NodeFieldPO) npo.fields.get(i);
						if (GroupMethod.valueOf(nfpo.GROUP_METHOD) != GroupMethod.NONE) {
							haveGroupByCalc = true;
							break;
						}
					}
					if (npo.inputs.size() == 1 && haveGroupByCalc == false) {
						input.MSG_TYPE = 0;// 当是节点输入时的输入消息类型,依赖节点只有一个输入且无汇总时不区分增全量
					}

					if (npo.pushInfos == null)
						npo.pushInfos = new ArrayList<InOutPO>();
					boolean hasPush = false;
					for (InOutPO _ipo : npo.pushInfos) {
						if (((PushPO) _ipo).PUSH_ID == 0 - npo.NODE_ID && ((PushPO) _ipo).pushMsgType == input.MSG_TYPE) {
							hasPush = true;
						}
					}
					if (!hasPush) {
						PushPO ppo = new PushPO();
						ppo.PUSH_ID = 0 - npo.NODE_ID;// 需要加上增量全标识符
						ppo.NODE_ID = npo.NODE_ID;
						ppo.PUSH_TYPE = 4;//
						ppo.PUSH_URL = conf.get(EStormConstant.KAFKA_MSGSERVER_ADDRESS);
						ppo.PUSH_USER = "";
						ppo.PUSH_PASS = "";
						ppo.PUSH_EXPR = "";
						ppo.PUSH_PARAM = EStormConstant.KAFKA_MSG_TAGNAME + "=node_" + npo.NODE_ID;
						ppo.PUSH_PARAM += "\nkafka.msg.isnodemsg=true";
						ppo.PUSH_PARAM += "\nkafka.zk.quorum=true";
						ppo.PUSH_PARAM += "\nkafka.msg.partitions.type=1";
						ppo.PUSH_PARAM += "\nkafka.msg.partitions=3";
						ppo.PUSH_PARAM += "\nkafka.msg.replication.factor=3";
						ppo.PUSH_PARAM += "\nkafka.msg.producer.type=async";
						ppo.PUSH_PARAM += "\nkafka.msg.compression.codec=snappy";
						ppo.pushMsgType = input.MSG_TYPE;
						npo.pushInfos.add(ppo);
						refNodes.add(npo);
						nfo.modifyType = nfo.modifyType | 64;
					}
					if (input.MSG_TYPE == 1) {// 0：全量 1：增量
						npo.outputMsgType = npo.outputMsgType | 4;// 0无输出：1 4增量
																	// 2全量 3增全量
					}

					// 构造添加模拟消息
					MsgPO mpo = new MsgPO();
					mpo.MSG_ID = 0 - npo.NODE_ID;
					mpo.MSG_NAME = npo.NODE_NAME;
					mpo.MSG_TAG_NAME = "node_" + npo.NODE_ID;
					mpo.MAX_PARALLELISM_NUM = input.MAX_PARALLELISM_NUM;
					mpo.fields = new ArrayList<FieldPO>();
					mpo.READ_OPS_PARAMS = "storm.kafka.msg.force.start=true";
					for (FieldPO f : npo.fields) {
						NodeFieldPO fpo = (NodeFieldPO) f;
						MsgFieldPO mfpo = new MsgFieldPO();
						mfpo.FIELD_NAME = fpo.FIELD_NAME;
						mfpo.FIELD_CN_NAME = fpo.FIELD_CN_NAME;
						mfpo.MSG_ID = mpo.MSG_ID;
						mfpo.FIELD_ID = fpo.FIELD_ID;
						mfpo.FIELD_DATA_TYPE = fpo.FIELD_DATA_TYPE;
						mfpo.ORDER_ID = fpo.ORDER_ID;
						mpo.fields.add(mfpo);
					}
					input.inputPo = mpo;
				}
			} else {
				NodePO npo = nodePORel.get(input.INPUT_SRC_ID);
				if (npo == null) {
					input.inputPo = null;
					return;
				}
				input.inputPo = npo;
				boolean haveGroupByCalc = false;
				for (int i = 0; i < npo.fields.size(); i++) {
					NodeFieldPO nfpo = (NodeFieldPO) npo.fields.get(i);
					if (GroupMethod.valueOf(nfpo.GROUP_METHOD) != GroupMethod.NONE) {
						haveGroupByCalc = true;
						break;
					}
				}
				if (npo.inputs.size() == 1 && haveGroupByCalc == false) {
					input.MSG_TYPE = 0;// 当是节点输入时的输入消息类型,依赖节点只有一个输入且无汇总时不区分增全量
				}
				if (input.MSG_TYPE == 0) {// 0：全量 1：增量
					npo.outputMsgType = npo.outputMsgType | 2;// 0无输出：1增量 2全量
																// 3增全量
				} else if (input.MSG_TYPE == 1) {// 0：全量 1：增量
					npo.outputMsgType = npo.outputMsgType | 1;// 0无输出：1 4增量 2全量
																// 3 5增全量
				}
			}
		}
	}

	public void open() throws SQLException {
		if (con != null && con.isClosed()) {
			close();
		}
		if (con == null) {
			con = DriverManager.getConnection(configInfo.url, configInfo.user, configInfo.password);
			access.setConnection(con);
		}
	}

	public void close() {
		try {
			if (con != null)
				con.close();
		} catch (SQLException e1) {
			LOG.warn("监听配置库异常：" + e1);
		}
		con = null;
	}

	public static class ConfigInfo {
		public String drive;
		public String url;
		public String user;
		public String password;

		public ConfigInfo(Configuration conf) {
			drive = conf.get(EStormConstant.CONFIG_DB_DRIVER_KEY);
			url = conf.get(EStormConstant.CONFIG_DB_URL_KEY);
			user = conf.get(EStormConstant.CONFIG_DB_USER_KEY);
			password = conf.get(EStormConstant.CONFIG_DB_PASS_KEY);
		}

		public ConfigInfo(String str) {
			// org.gjt.mm.mysql.Driver[estorm/estorm@jdbc:mysql://hadoop01:3306/estorm]
			String[] pars = str.split("[\\[/@\\]]");
			drive = pars[0];
			user = pars[1];
			password = pars[2];
			url = str.substring(drive.length() + 1 + user.length() + 1 + password.length() + 1, str.length() - 1);
		}

		public String toString() {
			return drive + "[" + user + "/" + password + "@" + url + "]";
		}

		public boolean equals(Object obj) {
			return toString().equals(obj.toString());
		}

		public static ConfigInfo pasreConofig(String str) {
			return COONFIGDB_PATTERN.matcher(str).matches() ? new ConfigInfo(str) : null;

		}
	}

	public static class ConfigDBTracker extends ZooKeeperNodeTracker {
		protected static final Log LOG = LogFactory.getLog(ConfigDBTracker.class);

		public ConfigDBTracker(ZooKeeperWatcher watcher, Server abortable) {
			super(watcher, watcher.estormConfigDbZNode, abortable);
			this.start();
		}

		public void createConfigDbNode(byte[] data) {
			try {
				ZKUtil.createNodeIfNotExistsAndWatch(this.watcher, node, data);
			} catch (KeeperException e) {
				LOG.error("创建配置节点错误:", e);
			} catch (IOException e) {
				LOG.error("创建配置节点错误:", e);
			}
		}

		public String getString() {
			if (this.data == null)
				return null;
			return new String(this.data);
		}
	}

	public static final Pattern COONFIGDB_PATTERN = Pattern.compile(".*\\[\\w+/\\w+@.*\\]$");
}
