package com.ery.estorm.client.node;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.ery.estorm.NodeColumnCalc;
import com.ery.estorm.client.node.JoinDataTree.JoinDataTreeVal;
import com.ery.estorm.client.node.prostore.ProStoreConnection;
import com.ery.estorm.client.push.PushManage;
import com.ery.estorm.client.store.StoreManage;
import com.ery.estorm.config.Configuration;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.topology.NodeInfo;
import com.ery.estorm.daemon.topology.NodeInfo.FieldPO;
import com.ery.estorm.daemon.topology.NodeInfo.GroupMethod;
import com.ery.estorm.daemon.topology.NodeInfo.InOutPO;
import com.ery.estorm.daemon.topology.NodeInfo.InputFieldRel;
import com.ery.estorm.daemon.topology.NodeInfo.JoinPO;
import com.ery.estorm.daemon.topology.NodeInfo.JoinPO.LoadState;
import com.ery.estorm.daemon.topology.NodeInfo.NodeFieldPO;
import com.ery.estorm.daemon.topology.NodeInfo.NodeInput;
import com.ery.estorm.daemon.topology.NodeInfo.NodeJoinPO;
import com.ery.estorm.daemon.topology.NodeInfo.NodePO;
import com.ery.estorm.join.DataSerializable;
import com.ery.estorm.join.JoinData;
import com.ery.estorm.join.JoinData.JoinDataTable;
import com.ery.estorm.log.LogMag;
import com.ery.estorm.log.NodeLog.NodeMsgLogPo;
import com.ery.estorm.util.DataInputBuffer;
import com.ery.estorm.util.DataOutputBuffer;
import com.ery.estorm.util.ToolUtil;
import com.ery.estorm.zk.ZKAssign;
import com.ery.estorm.zk.ZooKeeperWatcher;

@SuppressWarnings("unchecked")
public class BoltNode extends BaseRichBolt implements StormNodeProcess {
	private static final long serialVersionUID = -5638131391587595127L;
	public static final Log LOG = LogFactory.getLog(BoltNode.class);

	String procStoreType = null;
	String procStoreUrl = null;
	String procStorePar = null;
	OutputCollector _collector = null;
	TopologyContext context = null;

	public ZooKeeperWatcher zkw = null;// 不能序列化
	public String nodeZkPath = null;
	public NodeInfo nodeInfo = null;// 提交TOP 时初始化,
	NodePO nodePo = null;
	public boolean isPause = false;// 暂停不再取消息
	public Configuration conf = null;
	public int minWaitMillis = 0;
	long processTupleNum = 0;
	long lastPauseTupleNum = 0;
	long pauseTime = 0;
	boolean stoped = false;
	boolean isProcessTuple = false;
	@SuppressWarnings("rawtypes")
	Map comConf = null;
	ProStoreConnection proStoreConn = null;
	public Map<Long, NodeInput> InputRels[] = null;
	boolean isMulInput = false;
	boolean haveNextCalcCol = false;
	Map<String, BufferRow> buffer = new HashMap<String, BufferRow>();
	long _UpdateIntervalMs = 10000;
	long _lastUpdateTime = 0;

	// 一个节点一个实例，固定不变的，不需要在每条记录中存在
	public static class JoinTree {
		public JoinTree(NodeJoinPO joinPo) {
			this.joinPo = joinPo;
		}

		NodeJoinPO joinPo = null;
		public JoinTree[] subJoin = null;
		public Expression busKeyExpr = null;

		public ProStoreConnection proStoreConn = null;// 外部存储关联
		Map<String, byte[][]> memData = null;// 内存关联
	}

	JoinTree[] joins = null;// 只存根关联
	Map<Long, JoinTree> joinRels = null;// JoinId找Join

	public static class BufferRow {
		public Object[] data = null;
		public ProcData procData = null;// 过程数据
		public String key = null;

		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append("key=" + key + "\ndata=");
			for (int j = 0; j < data.length; j++) {
				sb.append(data[j].toString());
				sb.append("\t");
			}
			sb.append("\nprocData=" + procData.toString());
			return sb.toString();
		}

		public String toString(NodePO nodePo) {
			StringBuffer sb = new StringBuffer();
			sb.append("key=" + key + "\ndata=");
			for (int j = 0; j < data.length; j++) {
				sb.append(nodePo.nodeOutFields[j].FIELD_NAME);
				sb.append("=");
				sb.append(data[j].toString());
				sb.append("\t");
			}
			sb.append("\nprocData=" + procData.toString());
			return sb.toString();
		}
	}

	public static class ProcData implements Serializable {
		private static final long serialVersionUID = -5576235994370109812L;
		public long count;
		public JoinDataTree joinData;
		// public Map<String, Object> varData = null;// 过程数据
		// input, joinData, flag
		Map<Long, Tuple> inputs = null;
		Map<Long, Long> rowCount = null;

		public static ProcData deserialize(byte[] data, DataInputBuffer buffer) throws IOException {
			if (buffer == null)
				buffer = new DataInputBuffer();
			buffer.reset(data, data.length);
			ProcData procData = new ProcData();
			procData.count = com.ery.estorm.util.WritableUtils.readVLong(buffer);
			// procData.count = buffer.readLong();
			procData.joinData = (JoinDataTree) DataSerializable.readFromBytes(buffer);
			procData.inputs = (Map<Long, Tuple>) DataSerializable.readFromBytes(buffer);
			procData.rowCount = (Map<Long, Long>) DataSerializable.readFromBytes(buffer);
			return procData;
		}

		public byte[] serializable(DataOutputBuffer buffer) throws IOException {// 外部缓存
			if (buffer == null)
				buffer = new DataOutputBuffer();
			buffer.reset();
			com.ery.estorm.util.WritableUtils.writeVLong(buffer, count);
			// buffer.writeLong(com.ery.estorm.util.Bytes.toBytes(val)count);//
			// row count;
			DataSerializable.writeToBytes(joinData, buffer);
			DataSerializable.writeToBytes(inputs, buffer);
			DataSerializable.writeToBytes(rowCount, buffer);
			return DataSerializable.getBufferData(buffer);
		}

		Tuple getInput(InputFieldRel ifr) {
			return inputs.get(Long.parseLong(ifr.srcType + "" + ifr.srcId));
		}
	}

	public BoltNode(Configuration conf, NodeInfo node) {
		this.conf = conf;
		this.nodeInfo = node;
	}

	// public static class JoinNodeInfo {
	//
	// }
	//
	// Map<String, JoinNodeInfo> inputNodes = new HashMap<String,
	// JoinNodeInfo>();
	/**
	 * 链接以及需要的关联数据是否准备就绪,
	 */
	boolean isReady() {
		if (proStoreConn == null) {
			while (proStoreConn == null) {
				try {
					proStoreConn = new ProStoreConnection(this, conf);
				} catch (Exception e) {
					LOG.error("初始化数据存储连接失败,请检查存储配置和存储服务器," + e.getMessage(), e);
					Utils.sleep(5000);
				}
			}
		}
		return true;
	}

	// // msg_msgId msg_-nodeId node_nodeId
	NodeMsgLogPo getMsgLog(String srcComId) {
		// 生成日志对象
		NodeMsgLogPo msgLog = new NodeMsgLogPo();
		msgLog.USE_TIME = System.nanoTime();
		msgLog.NODE_ID = this.nodeInfo.getNodeId();
		if (srcComId.startsWith("msg_")) {
			msgLog.SRC_TYPE = 0;// 来源类型 0消息 1节点消息
			long sid = Long.parseLong(srcComId.substring(4));
			if (sid > 0) {
				msgLog.SRC_ID = sid;// 来源ID
			} else {
				msgLog.SRC_TYPE = 1;
				msgLog.SRC_ID = -sid;// 来源ID
			}
		} else if (srcComId.startsWith("node_")) {// 增加了增量全量结果标识
			msgLog.SRC_TYPE = 1;
			msgLog.SRC_ID = Long.parseLong(srcComId.split("_")[1]);
		}
		msgLog.SRC_DATA = null;// JSON
		msgLog.DEST_BEFORE_DATA = null;// 目标处理前数据
		msgLog.DEST_AFTER_DATA = null;// 目标处理后数据
		msgLog.PROCESS_TIME = System.currentTimeMillis();
		msgLog.HOST_NAME = StormNodeConfig.hostName;
		return msgLog;
	}

	@Override
	public void execute(Tuple input) {
		while (!isReady() && !stoped) {
			Utils.sleep(1000);
		}
		synchronized (context) {
			// StormNodeConfig.pm = new PushManage();
			// StormNodeConfig.pm.start();
			isProcessTuple = true;
			// StormNodeConfig.listen.register(nodeInfo.nodeName, this);
			// StormNodeConfig.nodeConfig.register(nodeZkPath, this);
			// Map<String, Object> row = null;// new HashMap<String, Object>();
			BufferRow destRow = new BufferRow();
			Object[] incData = null;
			if ((nodePo.outputMsgType & 1) == 1 || (nodePo.outputMsgType & 4) == 4) {
				incData = new Object[this.nodePo.nodeOutFields.length];
			}
			destRow.data = new Object[this.nodePo.nodeOutFields.length];
			byte flag[] = new byte[destRow.data.length];// 修改更新标识
			haveNextCalcCol = false;

			String srcComId = input.getSourceComponent();
			// String srcStreamId =
			// input.getSourceStreamId();//一个节点分多个业务流时用,默认与srcComId相同
			NodeMsgLogPo msgLog = getMsgLog(srcComId);
			NodeInput msgIn = InputRels[msgLog.SRC_TYPE].get(msgLog.SRC_ID);
			try {
				// EStormConstant.objectMapper
				// Object[] _destRow = null;
				BufferRow bufRow = null;
				// Map<String, Object> procData = null;// 过程数据
				// 计算输入对应的目标记录
				if (isMulInput) {// 多输入,BUSKEY不从关联来 增量合并记录
					// bus key
					calcDestBusKey(destRow.data, flag, input, msgIn, null);// 先取BUSKEY
					// join data
					JoinDataTree joinData = null;
					if (nodePo.joins != null && nodePo.joins.size() > 0)
						joinData = joinData(input, msgIn);// 关联数据,可能是多个输入做关联
					// calc row col
					calcDestRow(destRow.data, flag, input, msgIn, joinData);
					if (this.nodePo.LOG_LEVEL == 2) {
						msgLog.SRC_DATA = EStormConstant.objectMapper.writeValueAsString(input) + ";" +
								EStormConstant.objectMapper.writeValueAsString(destRow.data);
					}
					for (int i = 0; i < destRow.data.length; i++) {
						if (((NodeFieldPO) nodePo.nodeOutFields[i]).calcMethod == GroupMethod.COUNT)
							destRow.data[i] = 1;
						if ((nodePo.outputMsgType & 1) == 1 || (nodePo.outputMsgType & 4) == 4) {
							incData[i] = destRow.data[i];
						}
					}
					// Read DestRow _destRow.length -destRow.length=2 __count
					// __partsVal is map
					destRow.key = this.proStoreConn.getBusKey(destRow.data);
					bufRow = buffer.get(destRow.key);
					if (bufRow == null) {
						bufRow = this.proStoreConn.readData(destRow, msgLog);// 从数据存储中读取原始数据
						if (bufRow.procData != null && bufRow.procData.joinData != null) {
							bufRow.procData.joinData.initJpo(joinRels);
						}
					} else {
						bufRow.key = destRow.key;
					}
					if (this.nodePo.LOG_LEVEL == 2) {
						msgLog.DEST_BEFORE_DATA = EStormConstant.objectMapper.writeValueAsString(bufRow.data);
					}
					// 合成目标数据// 合并比对计算
					comDestRow(destRow, flag, bufRow, input, joinData, msgIn);
				} else {// 单输入 BUSKEY可以从关联来
					// join data
					JoinDataTree joinData = null;
					if (nodePo.joins != null && nodePo.joins.size() > 0)
						joinData = joinData(input, msgIn);// 先关联数据
					// bus key
					calcDestBusKey(destRow.data, flag, input, msgIn, joinData);
					// calc row col
					calcDestRow(destRow.data, flag, input, msgIn, joinData);
					if (this.nodePo.LOG_LEVEL == 2) {
						msgLog.SRC_DATA = EStormConstant.objectMapper.writeValueAsString(input) + ";" +
								EStormConstant.objectMapper.writeValueAsString(destRow);
					}
					// Read DestRow _destRow.length -destRow.length=1 __count
					// __partsVal is map
					destRow.key = this.proStoreConn.getBusKey(destRow.data);
					if (this.nodePo.haveGroupByCalc) {// 一个输入,且未分组计算,则不需要读取原数据//
														// 从数据存储中读取原始数据
						bufRow = buffer.get(destRow.key);
						if (bufRow == null) {
							bufRow = this.proStoreConn.readData(destRow, msgLog);// 从数据存储中读取原始数据
							if (bufRow.procData != null && bufRow.procData.joinData != null) {
								bufRow.procData.joinData.initJpo(joinRels);
							}
						} else {
							bufRow.key = destRow.key;
						}
					}
					if (this.nodePo.LOG_LEVEL == 2) {
						msgLog.DEST_BEFORE_DATA = EStormConstant.objectMapper.writeValueAsString(bufRow.data);
					}
					if (this.nodePo.haveGroupByCalc) { // 合成目标数据// 合并比对计算
						for (int i = 0; i < destRow.data.length; i++) {
							if (((NodeFieldPO) nodePo.nodeOutFields[i]).calcMethod == GroupMethod.COUNT)
								destRow.data[i] = 1;
							if ((nodePo.outputMsgType & 1) == 1 || (nodePo.outputMsgType & 4) == 4) {
								incData[i] = destRow.data[i];
							}
						}
						comDestRow(destRow, flag, bufRow, input, joinData, msgIn);
					}
				}
				if (this.nodePo.LOG_LEVEL == 2) {
					msgLog.DEST_AFTER_DATA = EStormConstant.objectMapper.writeValueAsString(destRow);// 日志中不写过程数据
				}
				if (destRow.procData == null || this.nodePo.haveGroupByCalc) {// 完整记录计算
					if (LOG.isDebugEnabled()) {
						StringBuffer sb = new StringBuffer();
						sb.append("Bolt " + nodePo.NODE_ID + " 输出");
						for (int i = 0; i < destRow.data.length; i++) {
							sb.append("\t[" + nodePo.nodeOutFields[i].FIELD_NAME + "]=" + destRow.data[i]);
						}
						LOG.debug(sb.toString());
					}
					if (nodePo.storeInfos != null && nodePo.storeInfos.size() > 0) {
						storeMsg(destRow);// 异步存储
					}
					if (nodePo.pushInfos != null && nodePo.pushInfos.size() > 0) {
						pushMsg(destRow, incData);// 订阅分发 需要区分增全量
					}
					processTupleNum++;
					if ((nodePo.outputMsgType & 2) == 2) {// 0无输出：1增量 2全量 3增全量
						msgLog.OUTPUT_NUM++;// 消息处理完后输出的消息数
						_collector.emit(this.nodeInfo.nodeName + "_0", Arrays.asList(destRow.data));// 0
																									// 全量
																									// 1增量
					}
					if ((nodePo.outputMsgType & 1) == 1) {// 0无输出：1增量 2全量 3增全量
						msgLog.OUTPUT_NUM++;// 消息处理完后输出的消息数
						if (this.nodePo.haveGroupByCalc) {
							for (int i = 0; i < incData.length; i++) {
								NodeFieldPO nfpo = (NodeFieldPO) nodePo.nodeOutFields[i];
								if (nfpo.calcMethod == GroupMethod.COUNT) {
									incData[i] = 1;// convertToLong(inputRow.data[i])
													// + 1;
								}
							}
						}
						_collector.emit(this.nodeInfo.nodeName + "_1", Arrays.asList(incData));
						// if (isMulInput)
						// else if (this.nodePo.haveGroupByCalc)// 单输入 汇总
						// _collector.emit(this.nodeInfo.nodeName + "_1",
						// Arrays.asList(incData));
						// 只有一个输入且无汇总时不区分已经置全量0
					}
				}
				_collector.ack(input);
				// 回写存储
				long now = System.currentTimeMillis();
				buffer.put(destRow.key, destRow);
				destRow.key = null;
				if (buffer.size() >= nodePo.maxBufferSize || now - _lastUpdateTime >= _UpdateIntervalMs) {
					boolean needOpen = false;
					msgLog.ERROR = "submit data rows:" + buffer.size();
					if (LOG.isDebugEnabled())
						LOG.debug("submit data rows:" + buffer.size());
					while (true) {
						try {
							if (needOpen)
								this.proStoreConn.openConn();
							for (String key : buffer.keySet()) {
								this.proStoreConn.writeData(buffer.get(key), key);
							}
							this.proStoreConn.submit();
							this.buffer.clear();
							_lastUpdateTime = now;
							break;
						} catch (Exception e) {
							this.proStoreConn.close();
							needOpen = true;
							LOG.error("提交异常，等待10秒重试：", e);
							Utils.sleep(10000);
						}
					}
				}
			} catch (Exception e) {
				msgLog.ERROR += "; " + e.getMessage();
				LOG.error("消息处理失败:" + msgLog, e);
				_collector.fail(input);
			} finally {
				// 写日志,不是真实写入,添加到日志对象队列中
				msgLog.USE_TIME = (System.nanoTime() - msgLog.USE_TIME);
				LogMag.addLog(msgLog);
				isProcessTuple = false;
			}
		}
	}

	private void storeMsg(BufferRow destRow) {
		StoreManage.add(nodePo, destRow);
	}

	// public long PUSH_ID;
	// public long NODE_ID;
	// public long PUSH_TYPE;// 订阅类型 订阅类型// 0，WS // 1，SOCKET // 2，HTTP // 3，ZK
	// // 4, kafka
	// public String PUSH_URL;// 订阅地址
	// public String PUSH_USER;
	// public String PUSH_PASS;
	// public String PUSH_EXPR;// 条件表达式
	// public String PUSH_PARAM;// push参数，根据接口类型不一样，参数规则不一样
	//
	// public int SUPPORT_BATCH;
	// public long PUSH_BUFFER_SIZE;
	// public long PUSH_TIME_INTERVAL;
	// TODO 存在多个订阅,最好在同一JVM中开统一线程做订阅处理,不影响主线程处理消息
	private void pushMsg(BufferRow destRow, Object[] incData) {
		if (nodePo.pushInfos.size() == 0)
			return;
		if ((nodePo.outputMsgType & 4) == 4) {
			PushManage.add(nodePo, destRow, incData);
		} else {
			PushManage.add(nodePo, destRow, null);
		}
	}

	// 最后合并成目标数据记录,以目标数据inputRow返回,函数返回过程数据对象
	private void comDestRow(BufferRow inputRow, byte[] flag, BufferRow destRow, Tuple input, JoinDataTree joinData,
			NodeInput msgIn) throws IOException {
		if (this.nodePo.haveGroupByCalc) {// 可能是单输入 也可能是多输入
			if (isMulInput) {// 多输入
				if (destRow.data == null) {// 分组时不会出来需要组合计算的字段,在初始化限定了的
					ProcData procData = new ProcData();
					procData.rowCount = new HashMap<Long, Long>();
					procData.rowCount.put(msgIn.INPUT_ID, 1L);
					for (int i = 0; i < nodePo.nodeOutFields.length; i++) {
						NodeFieldPO nfpo = (NodeFieldPO) nodePo.nodeOutFields[i];
						if (nfpo.calcMethod == GroupMethod.COUNT)
							inputRow.data[i] = 1;
					}
					inputRow.procData = procData;
					return;
				} else {
					ProcData procData = destRow.procData;
					Long rowCount = procData.rowCount.get(msgIn.INPUT_ID);
					if (rowCount == null)
						rowCount = 0l;
					for (int i = 0; i < nodePo.nodeOutFields.length; i++) {
						NodeFieldPO nfpo = (NodeFieldPO) nodePo.nodeOutFields[i];
						if (nfpo.IS_BUS_KEY != 0) {
							if (destRow.data[i] == null)
								destRow.data[i] = inputRow.data[i];
							continue;
						}
						if (flag[i] != 1) {// 非此输入计算结算
							continue;
						}
						if (rowCount == 0) {
							destRow.data[i] = inputRow.data[i];
							continue;
						}
						if (nfpo.calcMethod == GroupMethod.NONE || nfpo.calcMethod == GroupMethod.FIRST) {
							if (destRow.data[i] == null)
								destRow.data[i] = inputRow.data[i];
							continue;
						}
						switch (nfpo.calcMethod) {
						case SUM:
							destRow.data[i] = SUM(nfpo, destRow.data[i], inputRow.data[i]);
							break;
						case MIN:
							destRow.data[i] = MIN(nfpo, destRow.data[i], inputRow.data[i]);
							break;
						case MAX:
							destRow.data[i] = MAX(nfpo, destRow.data[i], inputRow.data[i]);
							break;
						case AVG:
							destRow.data[i] = AVG(nfpo, destRow.data[i], inputRow.data[i], rowCount);
							break;
						case COUNT:
							destRow.data[i] = rowCount + 1;
							break;
						case LAST:
							destRow.data[i] = inputRow.data[i];
							break;
						default:
							throw new IOException("不支持的聚类计算");
						}
					}
					procData.rowCount.put(msgIn.INPUT_ID, rowCount + 1);
					// for (int i = 0; i < inputRow.data.length; i++) {
					// inputRow.data[i] = destRow.data[i];
					// }
					inputRow.data = destRow.data;
					inputRow.procData = destRow.procData;
				}
			} else {// 单输入
				if (destRow.data == null) {
					// for (int i = 0; i < nodePo.nodeOutFields.length; i++) {
					// NodeFieldPO nfpo = (NodeFieldPO) nodePo.nodeOutFields[i];
					// if (nfpo.calcMethod == GroupMethod.COUNT)
					// inputRow.data[i] = 1;
					// }
					inputRow.procData = new ProcData();
					inputRow.procData.count = 1;//
					return;
				} else {
					groupBySingle(destRow, inputRow);
				}
			}
		} else {// 肯定是多输入
			if (destRow.data == null) {
				if (haveNextCalcCol) {
					inputRow.procData = new ProcData();
					// int SRC_TYPE = 0;// 来源类型 0消息 1节点消息
					// long SRC_ID = 0;
					// String srcComId=input.getSourceComponent();
					// if (srcComId.startsWith("msg_")) {
					// long sid = Long.parseLong(srcComId.substring(4));
					// if (sid > 0) {
					// SRC_ID = sid;// 来源ID
					// } else {
					// SRC_TYPE = 1;
					// SRC_ID = -sid;// 来源ID
					// }
					// } else if (srcComId.startsWith("node_")) {
					// SRC_TYPE = 1;
					// SRC_ID = Long.parseLong(srcComId.substring(5));
					// }
					//
					// msgIn = InputRels[SRC_TYPE].get(SRC_ID);
					// if (msgIn != null) {
					// inputs.put(Long.parseLong(SRC_TYPE + "" + SRC_ID),
					// (Tuple) procData.get(srcComId));
					// }
					inputRow.procData.inputs = new HashMap<Long, Tuple>();
					inputRow.procData.inputs.put(Long.parseLong(msgIn.INPUT_TYPE + "" + msgIn.INPUT_SRC_ID), input);
					inputRow.procData.joinData = joinData;
				}
			} else {
				if (haveNextCalcCol) {
					inputRow.procData = destRow.procData;
					if (inputRow.procData != null) {
						if (inputRow.procData.joinData == null) {
							inputRow.procData.joinData = joinData;
						} else {
							inputRow.procData.joinData.ComJoinDataTree(joinData);
						}
					} else {
						inputRow.procData = new ProcData();
						// procData.put(input.getSourceComponent(), new Object[]
						// { input, joinData, flag });// 多输入存储
						inputRow.procData.joinData = joinData;
					}
					if (inputRow.procData.inputs == null)
						inputRow.procData.inputs = new HashMap<Long, Tuple>();
					Long inputId = Long.parseLong(msgIn.INPUT_TYPE + "" + msgIn.INPUT_SRC_ID);
					inputRow.procData.inputs.put(inputId, input); // 多输入存储
					haveNextCalcCol = false;
					boolean needThisIinput = false;
					for (int i = 0; i < inputRow.data.length; i++) {
						if (flag[i] == 1) {
							destRow.data[i] = inputRow.data[i];
						} else if (flag[i] == 4) {// 需要组合计算
							boolean[] res = procMulInputCol(destRow, i, inputRow, msgIn);
							if (res[0] == false) {// 判断是否还不能完成计算条件
								haveNextCalcCol = true;
								if (res[1])// 并且需要当前输入
									needThisIinput = true;
							}
						}
					}
					if (haveNextCalcCol) {
						if (needThisIinput == false) {
							inputRow.procData.inputs.remove(inputId);
						}
						inputRow.data = destRow.data;
					} else {
						if (inputRow.procData.inputs != null)
							inputRow.procData.inputs.clear();
						inputRow.procData.inputs = null;
					}
				} else {
					for (int i = 0; i < inputRow.data.length; i++) {
						if (flag[i] == 1) {
							inputRow.data[i] = destRow.data[i];// 反向取
						}
					}
				}
			}
		}

	}

	// 单输入情况下的汇总,入口已经判断是否汇总和输入数
	private void groupBySingle(BufferRow destRow, BufferRow inputRow) {

		for (int i = 0; i < nodePo.nodeOutFields.length; i++) {
			NodeFieldPO nfpo = (NodeFieldPO) nodePo.nodeOutFields[i];
			if (nfpo.IS_BUS_KEY != 0) {
				if (destRow.data[i] == null)
					destRow.data[i] = inputRow.data[i];
				continue;
			}
			if (nfpo.calcMethod == GroupMethod.NONE || nfpo.calcMethod == GroupMethod.FIRST) {
				if (destRow.data[i] == null)
					destRow.data[i] = inputRow.data[i];
				continue;
			}
			switch (nfpo.calcMethod) {
			case SUM:
				destRow.data[i] = SUM(nfpo, destRow.data[i], inputRow.data[i]);
				break;
			case MIN:
				destRow.data[i] = MIN(nfpo, destRow.data[i], inputRow.data[i]);
				break;
			case MAX:
				destRow.data[i] = MAX(nfpo, destRow.data[i], inputRow.data[i]);
				break;
			case AVG:
				destRow.data[i] = AVG(nfpo, destRow.data[i], inputRow.data[i], destRow.procData.count);
				break;
			case COUNT:
				destRow.data[i] = destRow.procData.count + 1;// convertToLong(inputRow.data[i])
																// + 1;
				break;
			case LAST:
				destRow.data[i] = inputRow.data[i];
				break;
			default:
				break;
			}
		}
		destRow.procData.count++;
		inputRow.data = destRow.data;
		inputRow.procData = destRow.procData;
	}

	private Object AVG(NodeFieldPO nfpo, Object a, Object b, Long rowCount) {
		if (a == null || b == null) {
			if (nfpo.filedType == 2) {// 2 4 long double
				return a == null ? (b == null ? 0l : convertToLong(b)) : convertToLong(a);
			} else if (nfpo.filedType == 8) {
				return a == null ? (b == null ? 0l : convertToDateLong(b)) : convertToDateLong(a);
			} else if (nfpo.filedType == 1) {
				return null;
			} else if (nfpo.filedType == 4) {
				return a == null ? (b == null ? (double) 0.0 : convertToDouble(b)) : convertToDouble(a);
			}
			return null;
		}
		if (nfpo.filedType == 2) {// 2 4 long double
			double la = convertToDouble(a);
			long lb = convertToLong(b);
			if (lb + la * rowCount > Double.MAX_VALUE) {
				return (double) (lb - la) / (rowCount + 1) + la;
			} else {
				return (double) (lb + la * rowCount) / (rowCount + 1);
			}
		} else if (nfpo.filedType == 4) {
			Double la = convertToDouble(a);
			Double lb = convertToDouble(b);
			if (lb + la * rowCount > Double.MAX_VALUE) {
				return (double) (lb - la) / (rowCount + 1) + la;
			} else {
				return (lb + la * rowCount) / (rowCount + 1);
			}
		} else if (nfpo.filedType == 1) {
			return null;
		} else if (nfpo.filedType == 8) {// 算平均中间时间
			long la = convertToDateLong(a);
			long lb = convertToDateLong(b);
			if (lb + la * rowCount > Double.MAX_VALUE) {
				return (double) (lb - la) / (rowCount + 1) + la;
			} else {
				return (double) (lb + la * rowCount) / (rowCount + 1);
			}
		} else {
			return null;
		}
	}

	private Object MAX(NodeFieldPO nfpo, Object a, Object b) {
		if (a == null || b == null) {
			if (nfpo.filedType == 2) {// 2 4 long double
				return a == null ? (b == null ? 0l : convertToLong(b)) : convertToLong(a);
			} else if (nfpo.filedType == 4) {
				return a == null ? (b == null ? (double) 0.0 : convertToDouble(b)) : convertToDouble(a);
			} else if (nfpo.filedType == 1) {
				return a == null ? (b == null ? "" : b.toString()) : a.toString();
			} else if (nfpo.filedType == 8) {
				try {
					return a == null ? (b == null ? null : (b instanceof Long) ? b : EStormConstant.sdf.parse(
							b.toString()).getTime()) : (a instanceof Long) ? a : EStormConstant.sdf.parse(a.toString())
							.getTime();
				} catch (ParseException e) {
					return null;
				}
			}
		}
		if (nfpo.filedType == 2) {// 2 4 long double
			long la = convertToLong(a);
			long lb = convertToLong(b);
			return la < lb ? lb : la;
		} else if (nfpo.filedType == 4) {
			Double la = convertToDouble(a);
			Double lb = convertToDouble(b);
			return la < lb ? lb : la;
		} else if (nfpo.filedType == 1) {
			String sa = a == null ? "" : a.toString();
			String sb = b == null ? "" : b.toString();
			return sa.compareTo(sb) < 0 ? sb : sa;
		} else if (nfpo.filedType == 8) {
			long la = convertToDateLong(a);
			long lb = convertToDateLong(b);
			return la < lb ? lb : la;
		} else {
			return null;
		}
	}

	private Object MIN(NodeFieldPO nfpo, Object a, Object b) {
		if (a == null || b == null) {
			if (nfpo.filedType == 2) {// 2 4 long double
				return a == null ? (b == null ? 0l : convertToLong(b)) : convertToLong(a);
			} else if (nfpo.filedType == 4) {
				return a == null ? (b == null ? (double) 0.0 : convertToDouble(b)) : convertToDouble(a);
			} else if (nfpo.filedType == 1) {
				return a == null ? (b == null ? "" : b.toString()) : a.toString();
			} else if (nfpo.filedType == 8) {
				try {
					return a == null ? (b == null ? null : (b instanceof Long) ? b : EStormConstant.sdf.parse(
							b.toString()).getTime()) : (a instanceof Long) ? a : EStormConstant.sdf.parse(a.toString())
							.getTime();
				} catch (ParseException e) {
					return null;
				}
			}
		}
		if (nfpo.filedType == 2) {// 2 4 long double
			long la = convertToLong(a);
			long lb = convertToLong(b);
			return la > lb ? lb : la;
		} else if (nfpo.filedType == 4) {
			Double la = convertToDouble(a);
			Double lb = convertToDouble(b);
			return la > lb ? lb : la;
		} else if (nfpo.filedType == 1) {
			String sa = a == null ? "" : a.toString();
			String sb = b == null ? "" : b.toString();
			return sa.compareTo(sb) > 0 ? sb : sa;
		} else if (nfpo.filedType == 8) {
			long la = convertToDateLong(a);
			long lb = convertToDateLong(b);
			return la > lb ? lb : la;
		} else {
			return null;
		}
	}

	private Object SUM(NodeFieldPO nfpo, Object a, Object b) {
		if (a == null || b == null) {
			if (nfpo.filedType == 2) {// 2 4 long double
				return a == null ? (b == null ? 0l : convertToLong(b)) : convertToLong(a);
			} else {
				return a == null ? (b == null ? (double) 0.0 : convertToDouble(b)) : convertToDouble(a);
			}
		}
		if (nfpo.filedType == 2) {// 2 4 long double
			long la = convertToLong(a);
			long lb = convertToLong(b);
			return lb + la;
		} else if (nfpo.filedType == 4) {
			Double la = convertToDouble(a);
			Double lb = convertToDouble(b);
			return lb + la;
		} else if (nfpo.filedType == 1) {
			String sa = a == null ? "" : a.toString();
			String sb = b == null ? "" : b.toString();
			return sa + "," + sb;
		} else if (nfpo.filedType == 8) {
			long la = convertToDateLong(a);
			long lb = convertToDateLong(b);
			String sa = la == 0 ? "" : EStormConstant.sdf.format(new Date(la));
			String sb = lb == 0 ? "" : EStormConstant.sdf.format(new Date(lb));
			return sa + "," + sb;
		} else {
			return null;
		}
	}

	private long convertToDateLong(Object o) {
		if (o == null)
			return 0;
		try {
			if (o instanceof Integer) {
				return (Integer) o;
			} else if (o instanceof Long) {
				return (Long) o;
			} else if (o instanceof Double) {
				return ((Double) o).longValue();
			} else {
				String v = o.toString();
				try {
					return EStormConstant.sdf.parse(v).getTime();
				} catch (ParseException e) {
					return Long.parseLong(v);
				}
			}
		} catch (Exception e) {
			return 0;
		}
	}

	private long convertToLong(Object o) {
		if (o == null)
			return 0;
		try {
			if (o instanceof Integer) {
				return (Integer) o;
			} else if (o instanceof Long) {
				return (Long) o;
			} else if (o instanceof Double) {
				return ((Double) o).longValue();
			} else {
				String v = o.toString();
				if (v.indexOf('.') > 0) {
					return (long) Double.parseDouble(v);
				} else {
					return Long.parseLong(v);
				}
			}
		} catch (Exception e) {
			return 0;
		}
	}

	private double convertToDouble(Object o) {
		if (o == null)
			return 0.0;
		try {
			if (o instanceof Integer) {
				return (Integer) o;
			} else if (o instanceof Long) {
				return (Long) o;
			} else if (o instanceof Double) {
				return ((Double) o).longValue();
			} else {
				String v = o.toString();
				if (v.indexOf('.') > 0) {
					return (long) Double.parseDouble(v);
				} else {
					return Long.parseLong(v);
				}
			}
		} catch (Exception e) {
			return 0.0;
		}
	}

	// 多输入计算字段处理
	private boolean[] procMulInputCol(BufferRow destRow, int i, BufferRow inputRow, NodeInput msgIn) {
		boolean res[] = new boolean[2];
		res[0] = true;
		res[1] = false;
		// 计算字段及关联字段
		NodeFieldPO nfpo = (NodeFieldPO) nodePo.nodeOutFields[i];
		// boolean isGroupByCol = (nodePo.haveGroupByCalc && nfpo.calcMethod ==
		// GroupMethod.NONE);
		boolean allIn = true;
		switch (nfpo.calcType) { // 0:常量 1single 2:calc expr 3:class
		case 2:// 表达式计算值
			Map<String, Object> calcEnv = new HashMap<String, Object>();
			for (InputFieldRel ifr : nfpo.InputRels[0]) {
				if (ifr.srcType == 2) {// 来源关联表
					JoinDataTreeVal jdt = inputRow.procData.joinData.joinDataRel.get(ifr.srcId);
					if (jdt.inited) {// 属于当前输入关联
						String val = "";
						if (jdt.value != null) {
							val = new String(jdt.value[ifr.inputIndex]);
						} else {
							val = getNullStringValue(jdt.jpo.joinPO.nodeOutFields[ifr.inputIndex].filedType).toString();
						}
						Object obj = EStormConstant.castAviatorObject(val); // 需要做类型转换
						calcEnv.put(ifr.inputName, obj);
					} else {// 属于未到的输入关联
						allIn = false;// 不满足计算条件
						break;
					}
				} else {
					if (ifr.srcType == msgIn.INPUT_TYPE && ifr.srcId == msgIn.INPUT_SRC_ID) {
						res[1] = true;
					}
					Tuple input = inputRow.procData.getInput(ifr);
					if (input != null) {// 来源当前输入
						// Object obj =
						// EStormConstant.castAviatorObject(input.getString(ifr.inputIndex));
						// // 需要做类型转换
						// calcEnv.put(ifr.inputName, obj);
						calcEnv.put(
								ifr.inputName,
								castAviatorObject(input.getValue(ifr.inputIndex),
										((InOutPO) msgIn.inputPo).nodeOutFields[ifr.inputIndex]));
					} else {
						allIn = false;// 不满足计算条件
						break;
					}
				}
			}
			if (allIn) {
				destRow.data[i] = nfpo.expr[0].execute(calcEnv);
			} else {
				res[0] = false;
			}
			break;
		case 3:// 类计算值
			String[] env = new String[nfpo.InputRels[0].length];
			int index = 0;
			for (InputFieldRel ifr : nfpo.InputRels[0]) {
				if (ifr.srcType == 2) {// 来源关联表
					JoinDataTreeVal jdt = inputRow.procData.joinData.joinDataRel.get(ifr.srcId);
					if (jdt.inited) {// 属于当前输入关联
						if (jdt.value != null) {
							env[index++] = new String(jdt.value[ifr.inputIndex]);
						} else {
							env[index++] = getNullStringValue(jdt.jpo.joinPO.nodeOutFields[ifr.inputIndex].filedType)
									.toString();
						}
					} else {// 属于未到的输入关联
						allIn = false;// 不满足计算条件
						break;
					}
				} else {
					if (ifr.srcType == msgIn.INPUT_TYPE && ifr.srcId == msgIn.INPUT_SRC_ID) {
						res[1] = true;
					}
					Tuple input = inputRow.procData.getInput(ifr);
					if (input != null) {// 来源当前输入
						env[index++] = input.getString(ifr.inputIndex);
					} else {
						allIn = false;// 不满足计算条件
						break;
					}
					allIn = false;// 不满足计算条件
					break;
				}
			}
			if (allIn) {
				destRow.data[i] = nfpo.calcClass[0].calc(env);
			} else {
				res[0] = false;
			}
			break;
		}
		return res;
	}

	JoinDataTreeVal getJoinData(JoinDataTree joinData, InputFieldRel ifr) {
		return joinData.joinDataRel.get(ifr.srcId);
	}

	private JoinDataTree joinData(Tuple input, NodeInput msgIn) throws IOException {
		if (joins == null)
			return null;
		JoinDataTree jdt = new JoinDataTree();
		// jdt.data = new JoinDataTreeVal[joins.length];
		jdt.joinDataRel = new HashMap<Long, JoinDataTreeVal>();
		for (int i = 0; i < joins.length; i++) {
			JoinDataTreeVal _jdtv = new JoinDataTreeVal();
			_jdtv.jpo = joins[i].joinPo;
			// jdt.data[i] = new JoinDataTreeVal();
			// jdt.data[i].jpo = joins[i].joinPo;
			jdt.joinDataRel.put(joins[i].joinPo.JOIN_ID, _jdtv);
			// jdt.joinDataRel.put(joins[i].joinPo.JOIN_ID, jdt.data[i]);
			getJoinData(input, msgIn, jdt, _jdtv, joins[i], null);
			// getJoinData(input, msgIn, jdt, jdt.data[i], joins[i], null);
		}
		return jdt;
	}

	void getJoinData(Tuple input, NodeInput msgIn, JoinDataTree jdt, JoinDataTreeVal jdtv, JoinTree join,
			JoinDataTreeVal par) throws IOException {
		// 根使用input,子使用父
		if (join.joinPo.PAR_JOIN_ID == 0) {
			if (join.joinPo.parJoinRel[0].srcType != msgIn.INPUT_TYPE ||
					join.joinPo.parJoinRel[0].srcId != msgIn.INPUT_SRC_ID) {
				jdtv.inited = false;
				return;// 不是对应的输入关联,一个关联只能对应一个输入,因此可以这样判断
			}
			if (join.joinPo.joinPO.INLOCAL_MEMORY != 0) {// 内存
				StringBuffer inputKey = new StringBuffer();
				for (InputFieldRel ifr : join.joinPo.parJoinRel) {
					if (inputKey.length() > 0) {
						inputKey.append(JoinData.joinKeyColSplitChar);
					}
					inputKey.append(input.getString(ifr.inputIndex));
				}
				// 关联内存
				jdtv.inited = true;
				jdtv.value = join.memData.get(inputKey.toString());
			} else {// 外部存储
				if (join.proStoreConn.keyType == 1) {// hbase
					Map<String, Object> env = new HashMap<String, Object>();
					// 计算ROWKEY
					for (InputFieldRel ifr : join.joinPo.parJoinRel) {
						env.put(ifr.inputName,
								castAviatorObject(input.getValue(ifr.inputIndex),
										((InOutPO) msgIn.inputPo).nodeOutFields[ifr.inputIndex]));
						// env.put(ifr.inputName,
						// EStormConstant.castAviatorObject(input.getString(ifr.inputIndex)));
						// 使用的关联自己的字段做的ROWKEY计算,与建立索引时使用一致
					}
					byte[] rkey = join.busKeyExpr.execute(env).toString().getBytes();
					// 查询Hbase
					jdtv.inited = true;
					jdtv.value = join.proStoreConn.readData(rkey);
				} else if (join.proStoreConn.keyType == 2) {// RDBMS
					// join.proStoreConn.readData(cols);
				}
			}
		} else {// 子关联
			if (join.joinPo.joinPO.INLOCAL_MEMORY != 0) {// 内存
				StringBuffer inputKey = new StringBuffer();
				for (InputFieldRel ifr : join.joinPo.parJoinRel) {
					if (inputKey.length() > 0) {
						inputKey.append(JoinData.joinKeyColSplitChar);
					}
					inputKey.append(new String(par.value[ifr.inputIndex]));
				}
				// 关联内存
				jdtv.inited = true;
				jdtv.value = join.memData.get(inputKey.toString());
			} else {// 外部存储
				if (join.proStoreConn.keyType == 1) {// hbase
					Map<String, Object> env = new HashMap<String, Object>();
					// 计算ROWKEY
					for (InputFieldRel ifr : join.joinPo.parJoinRel) {
						env.put(ifr.inputName, EStormConstant.castAviatorObject(par.value[ifr.inputIndex]));// 使用的关联自己的字段做的ROWKEY计算,与建立索引时使用一致
					}
					byte[] rkey = join.busKeyExpr.execute(env).toString().getBytes();
					// 查询Hbase
					jdtv.inited = true;
					jdtv.value = join.proStoreConn.readData(rkey);
				} else if (join.proStoreConn.keyType == 2) {// RDBMS
					// join.proStoreConn.readData(cols);
				}
			}
		}
		// 递归子查询
		if (join.subJoin != null) {
			// jdtv.subValue = new JoinDataTreeVal[join.subJoin.length];
			for (int i = 0; i < join.subJoin.length; i++) {
				JoinDataTreeVal _jdtv = new JoinDataTreeVal();
				// jdtv.subValue[i] = new JoinDataTreeVal();
				_jdtv.jpo = join.subJoin[i].joinPo;
				// jdtv.subValue[i].jpo = join.subJoin[i].joinPo;
				jdt.joinDataRel.put(join.subJoin[i].joinPo.JOIN_ID, _jdtv);
				// jdt.joinDataRel.put(join.subJoin[i].joinPo.JOIN_ID,
				// jdtv.subValue[i]);
				getJoinData(input, msgIn, jdt, _jdtv, join.subJoin[i], jdtv);
				// getJoinData(input, msgIn, jdt, jdtv.subValue[i],
				// join.subJoin[i], jdtv);
			}
		}
	}

	private void calcDestBusKey(Object[] destRow, byte[] flag, Tuple input, NodeInput msgIn, JoinDataTree joinData) {
		Map<String, Object> calcEnv = new HashMap<String, Object>();
		for (int i = 0; i < nodePo.nodeOutFields.length; i++) {
			destRow[i] = null;
			flag[i] = 0;
			NodeFieldPO nfpo = (NodeFieldPO) nodePo.nodeOutFields[i];
			if (nfpo.IS_BUS_KEY == 0) {// 先计算主键
				continue;
			}
			calcEnv.clear();
			switch (nfpo.calcType) { // 0:常量 1single 2:calc expr 3:class
			case 0:
				destRow[i] = nfpo.constant;
				flag[i] = 1;
				break;
			case 1:// 单字段值
				if (nfpo.InputRels[msgIn.index][0].srcType != 2) {
					destRow[i] = input.getValue(nfpo.InputRels[msgIn.index][0].inputIndex);
					flag[i] = 1;
				} else {// 单输入,能关联数据
					JoinDataTreeVal jdt = joinData.joinDataRel.get(nfpo.InputRels[msgIn.index][0].srcId);
					if (jdt != null && jdt.inited && jdt.value != null) {
						destRow[i] = jdt.value[nfpo.InputRels[msgIn.index][0].inputIndex];
					}
					flag[i] = 1;
				}
				break;
			case 2:// 表达式计算值
				for (InputFieldRel ifr : nfpo.InputRels[msgIn.index]) {
					Object obj = input.getValue(ifr.inputIndex);
					// 需要做类型转换
					calcEnv.put(ifr.inputName,
							castAviatorObject(obj, ((InOutPO) msgIn.inputPo).nodeOutFields[ifr.inputIndex]));
				}
				destRow[i] = nfpo.expr[msgIn.index].execute(calcEnv);
				flag[i] = 1;
				break;
			case 3:// 类计算值
				String[] env = new String[nfpo.InputRels[msgIn.index].length];
				for (int n = 0; n < env.length; n++) {
					env[n] = input.getValue(nfpo.InputRels[msgIn.index][i].inputIndex).toString();
				}
				destRow[i] = nfpo.calcClass[msgIn.index].calc(env);
				flag[i] = 1;
				break;
			}
			convertToObject(destRow, i, nfpo);
		}
	}

	public Object castAviatorObject(Object obj, FieldPO fpo) {
		if (fpo.filedType == 8) {
			return new Date((Long) obj);
		}
		return obj;
	}

	private void convertToObject(Object[] destRow, int index, NodeFieldPO nfpo) {
		switch (nfpo.filedType) {
		case 1:
			if (destRow[index] == null) {
				destRow[index] = "";
			} else {
				destRow[index] = destRow[index].toString();
			}
			break;
		case 2:// CHAR字符，NUMBER(2)
			try {
				if (destRow[index] == null) {
					destRow[index] = 0l;
				} else if (destRow[index] instanceof String) {
					String val = (String) destRow[index];
					if (Character.isDigit(val.charAt(0))) {
						if (val.indexOf('.') > 0)
							destRow[index] = Double.parseDouble(val);
						else
							destRow[index] = Long.parseLong(val);
					} else {
						destRow[index] = 0l;
					}
				}
			} catch (Exception e) {
				destRow[index] = ToolUtil.toLong(destRow[index].toString());
			}
			break;
		case 4:
			try {
				if (destRow[index] == null) {
					destRow[index] = 0l;
				} else if (destRow[index] instanceof String) {
					String val = destRow[index].toString();
					if (Character.isDigit(val.charAt(0))) {
						destRow[index] = Double.parseDouble(val);
					} else {
						destRow[index] = (Double) 0.0;
					}
				}
			} catch (Exception e) {
				destRow[index] = ToolUtil.toDouble(destRow[index].toString());
			}
			break;
		case 8:// DATE
			try {
				if (destRow[index] == null) {
					destRow[index] = 0l;
				} else if (destRow[index] instanceof String) {
					String val = (String) destRow[index];
					destRow[index] = Long.parseLong(val);
				}
			} catch (Exception e) {
				destRow[index] = ToolUtil.toLong(destRow[index].toString());
			}
			break;
		}
	}

	// 通过输入和关联数据计算目标数据行
	private void calcDestRow(Object[] destRow, byte flag[], Tuple input, NodeInput msgIn, JoinDataTree joinData) {
		Map<String, Object> calcEnv = new HashMap<String, Object>();
		// 计算字段及关联字段
		for (int i = 0; i < nodePo.nodeOutFields.length; i++) {
			NodeFieldPO nfpo = (NodeFieldPO) nodePo.nodeOutFields[i];
			if (nfpo.IS_BUS_KEY != 0) {// 先计算主键
				continue;
			}// ||
				// boolean isGroupByCol = (nodePo.haveGroupByCalc &&
				// nfpo.calcMethod == GroupMethod.NONE);
			calcEnv.clear();
			switch (nfpo.calcType) { // 0:常量 1single 2:calc expr 3:class
			case 0:
				destRow[i] = nfpo.constant;
				flag[i] = 1;
				break;
			case 1:// 单字段值
				if (isMulInput) {// 多输入
					if (nfpo.InputRels[0][0].srcType == msgIn.INPUT_TYPE &&
							nfpo.InputRels[0][0].srcId == msgIn.INPUT_SRC_ID) {// 来源当前输入
						destRow[i] = input.getValue(nfpo.InputRels[0][0].inputIndex);
						flag[i] = 1;
					} else if (nfpo.InputRels[0][0].srcType == 2) {// 要求非Group主键
						JoinDataTreeVal jdt = joinData.joinDataRel.get(nfpo.InputRels[0][0].srcId);
						if (jdt.inited) {
							if (jdt.value != null)
								destRow[i] = jdt.value[nfpo.InputRels[0][0].inputIndex];
							else
								destRow[i] = null;
							flag[i] = 1;
						} else {// 属于另外的输入关联的表
							flag[i] = 2;// 二次填充计算
						}
					}
				} else {// 单输入
					if (nfpo.InputRels[0][0].srcType == 2) {// 来源关联表
						JoinDataTreeVal jdt = joinData.joinDataRel.get(nfpo.InputRels[0][0].srcId);
						if (jdt != null && jdt.inited) {
							if (jdt.value != null)
								destRow[i] = jdt.value[nfpo.InputRels[0][0].inputIndex];
							else
								destRow[i] = null;
						}
						flag[i] = 1;
					} else {// 来源当前输入
						destRow[i] = input.getValue(nfpo.InputRels[0][0].inputIndex);
						flag[i] = 1;
					}
				}
				break;
			case 2:// 表达式计算值
				if (isMulInput) {// 多输入
					boolean allIn = true;
					for (InputFieldRel ifr : nfpo.InputRels[0]) {
						if (ifr.srcType == 2) {// 来源关联表
							JoinDataTreeVal jdt = joinData.joinDataRel.get(ifr.srcId);
							if (jdt.inited) {// 属于当前输入关联
								String val = "";
								if (jdt.value != null) {
									val = new String(jdt.value[ifr.inputIndex]);
								} else {
									val = getNullStringValue(jdt.jpo.joinPO.nodeOutFields[ifr.inputIndex].filedType)
											.toString();
								}
								Object obj = EStormConstant.castAviatorObject(val); // 需要做类型转换
								calcEnv.put(ifr.inputName, obj);
							} else {// 不属于当前输入关联
								allIn = false;// 不满足计算条件
								break;
							}
						} else if (ifr.srcType == msgIn.INPUT_TYPE && ifr.srcId == msgIn.INPUT_SRC_ID) {// 来源当前输入
							// Object obj =
							// EStormConstant.castAviatorObject(input.getString(ifr.inputIndex));
							// // 需要做类型转换
							// calcEnv.put(ifr.inputName, obj);
							calcEnv.put(
									ifr.inputName,
									castAviatorObject(input.getValue(ifr.inputIndex),
											((InOutPO) msgIn.inputPo).nodeOutFields[ifr.inputIndex]));
						} else {
							allIn = false;// 不满足计算条件
							break;
						}
					}
					if (allIn) {
						destRow[i] = nfpo.expr[0].execute(calcEnv);
						flag[i] = 1;
					} else {
						flag[i] = 4;
						// 需要组合计算,要先缓存数据 input joinData
						haveNextCalcCol = true;
					}
				} else {// 单输入
					for (InputFieldRel ifr : nfpo.InputRels[0]) {
						if (ifr.srcType == 2) {// 来源关联表
							JoinDataTreeVal jdt = joinData.joinDataRel.get(ifr.srcId);
							String val = "";
							if (jdt != null && jdt.inited && jdt.value != null) {
								val = new String(jdt.value[ifr.inputIndex]);
							} else {
								JoinTree jt = joinRels.get(ifr.srcId);
								val = getNullStringValue(jt.joinPo.joinPO.nodeOutFields[ifr.inputIndex].filedType)
										.toString();
							}
							Object obj = EStormConstant.castAviatorObject(val); // 需要做类型转换
							calcEnv.put(ifr.inputName, obj);
						} else {// 来源当前输入
							calcEnv.put(
									ifr.inputName,
									castAviatorObject(input.getValue(ifr.inputIndex),
											((InOutPO) msgIn.inputPo).nodeOutFields[ifr.inputIndex]));

							// Object obj =
							// EStormConstant.castAviatorObject(input.getString(ifr.inputIndex));
							// // 需要做类型转换
							// calcEnv.put(ifr.inputName, obj);
						}
					}
					destRow[i] = nfpo.expr[msgIn.index].execute(calcEnv);
					flag[i] = 1;
				}
				break;
			case 3:// 类计算值
				if (isMulInput) {// 多输入
					boolean allIn = true;
					String[] env = new String[nfpo.InputRels[0].length];
					int index = 0;
					for (InputFieldRel ifr : nfpo.InputRels[0]) {
						if (ifr.srcType == 2) {// 来源关联表
							JoinDataTreeVal jdt = joinData.joinDataRel.get(ifr.srcId);
							if (jdt.inited) {// 属于当前输入关联
								if (jdt.value != null) {
									env[index++] = new String(jdt.value[ifr.inputIndex]);
								} else {
									env[index++] = getNullStringValue(
											jdt.jpo.joinPO.nodeOutFields[ifr.inputIndex].filedType).toString();
								}
							} else {// 不属于当前输入关联
								allIn = false;// 不满足计算条件
								break;
							}
						} else if (ifr.srcType == msgIn.INPUT_TYPE && ifr.srcId == msgIn.INPUT_SRC_ID) {// 来源当前输入
							env[index++] = input.getString(ifr.inputIndex);
						} else {
							allIn = false;// 不满足计算条件
							break;
						}
					}
					if (allIn) {
						destRow[i] = nfpo.calcClass[0].calc(env);
						flag[i] = 1;
					} else {
						flag[i] = 4;
						// 需要组合计算,要先缓存数据 input joinData
						haveNextCalcCol = true;
					}
				} else {// 单输入
					String[] env = new String[nfpo.InputRels[0].length];
					int index = 0;
					for (InputFieldRel ifr : nfpo.InputRels[0]) {
						if (ifr.srcType == 2) {// 来源关联表
							JoinDataTreeVal jdt = joinData.joinDataRel.get(ifr.srcId);
							String val = "";
							if (jdt != null && jdt.inited && jdt.value != null) {
								val = new String(jdt.value[ifr.inputIndex]);
							} else {
								JoinTree jt = joinRels.get(ifr.srcId);
								val = getNullStringValue(jt.joinPo.joinPO.nodeOutFields[ifr.inputIndex].filedType)
										.toString();
							}
							env[index++] = val;
						} else {// 来源当前输入
							env[index++] = input.getString(ifr.inputIndex);
						}
					}
					destRow[i] = nfpo.calcClass[0].calc(env);
					flag[i] = 1;
				}
				break;
			}
			convertToObject(destRow, i, nfpo);
		}
	}

	private String getNullStringValue(byte dataType) {
		if (dataType == 2) {// CHAR字符，NUMBER(2)
			return "0";
		} else if (dataType == 4) {// CHAR字符，NUMBER(2)
			return "0.0";
		} else if (dataType == 8) {// CHAR字符，NUMBER(2) DATE
			return EStormConstant.utsTiimeString;
		} else {// CHAR字符，NUMBER(2)
			return "";
		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		Thread.currentThread().setName(nodeInfo.nodeName);
		comConf = stormConf;
		this.context = context;
		this._collector = collector;
		// StormNodeConfig.addStorm(context.getStormId());
		initNodePo(false);
	}

	public void initNodePo(boolean isChange) {
		if (!isChange) {
			StormNodeConfig.init(this.conf, context.getThisWorkerPort());
		}
		if (StormNodeConfig.nodeConfig.onlineNodes.containsKey(this.nodeInfo.getNodeId())) {
			this.nodeInfo = StormNodeConfig.nodeConfig.onlineNodes.get(this.nodeInfo.getNodeId());
			nodePo = (NodePO) nodeInfo.node;
		} else {
			try {
				nodeInfo.updateToZk(zkw);
			} catch (Exception e) {
				LOG.error("列表中不存在节点，更新回写ZK失败", e);
				throw new RuntimeException(e);
			}
			nodePo = (NodePO) nodeInfo.node;
		}
		if (nodePo.IS_DEBUG == 1) {
			nodePo.LOG_LEVEL = 2;
		}
		if (!isChange) {
			this.isMulInput = nodePo.inputs.size() > 1;
			zkw = StormNodeConfig.zkw;
			minWaitMillis = this.conf.getInt(EStormConstant.STORM_STOP_MINWAIT_CONFIRM_MILLIS, 5000);
			nodeZkPath = ZKAssign.getNodePath(zkw, this.nodeInfo.nodeName.toString());
			StormNodeConfig.listen.register(nodeInfo.nodeName, this);
			StormNodeConfig.nodeConfig.register(nodeZkPath, this);
			InputRels = new Map[2];
			InputRels[0] = new HashMap<Long, NodeInput>();
			InputRels[1] = new HashMap<Long, NodeInput>();
			for (NodeInput input : nodePo.inputs) {
				InputRels[input.INPUT_TYPE].put(input.INPUT_SRC_ID, input);
			}
		}
		for (int i = 0; i < nodePo.nodeOutFields.length; i++) {
			NodeFieldPO nfpo = (NodeFieldPO) nodePo.nodeOutFields[i];
			if (nfpo.calcType == 2) {
				nfpo.expr = new Expression[nfpo.calcExprs.length];
				for (int j = 0; j < nfpo.calcExprs.length; j++) {
					nfpo.expr[j] = AviatorEvaluator.compile(nfpo.calcExprs[j], true);// 缓存编译表达式
					if (nfpo.expr[j] == null) {
						LOG.error("计算表达式编译错误 :" + nfpo.calcExprs[j]);
					}
				}
			} else if (nfpo.calcType == 3) {
				nfpo.calcClass = new NodeColumnCalc[nfpo.calcExprs.length];
				for (int j = 0; j < nfpo.calcExprs.length; j++) {
					nfpo.calcClass[j] = null;
					while (nfpo.calcClass[j] == null) {
						nfpo.calcClass[j] = NodeColumnCalc.getNodeColumnCalc(nfpo.calcExprs[j]);// 缓存编译表达式
						if (nfpo.calcClass[j] == null) {
							LOG.error("等待动态类编译加载 :" + nfpo.calcExprs[j]);
							Utils.sleep(2000);
						}
					}
				}
			}
		}
		if (!isChange) {
			try {
				long startTime = System.currentTimeMillis();
				while (proStoreConn == null) {
					try {
						proStoreConn = new ProStoreConnection(this, conf);
					} catch (Exception e) {
						LOG.error("初始化数据存储连接失败,请检查存储配置和存储服务器," + e.getMessage(), e);
						Utils.sleep(5000);
						if (System.currentTimeMillis() - startTime > 600000) {
							// 发送停止对应TOP的命令
							sendStopTopology("初始化数据存储连接失败,请检查存储配置和存储服务器," + e.getMessage());
						}
					}
				}
				initSubJoinTree();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private void initSubJoinTree() throws Exception {
		if (nodePo.joins == null)
			return;
		int level0 = 0;
		for (NodeJoinPO joinPo : nodePo.joins) {
			if (joinPo.PAR_JOIN_ID == 0)
				level0++;
		}
		joins = new JoinTree[level0];
		level0 = 0;
		for (NodeJoinPO joinPo : nodePo.joins) {
			if (joinPo.PAR_JOIN_ID == 0) {
				joins[level0] = new JoinTree(joinPo);
				joinRels.put(joinPo.JOIN_ID, joins[level0]);
				initJoinConn(joins[level0]);
				findSubJoin(joins[level0]);
				level0++;
			}
		}
	}

	void initJoinConn(JoinTree jtr) throws Exception {
		NodeJoinPO joinPo = jtr.joinPo;
		JoinPO jpo = StormNodeConfig.joinListTracker.joins.get(joinPo.JOIN_ID);
		while (jpo == null) {
			LOG.warn("等待关联数据加载:" + joinPo.JOIN_ID + " 加载未监听到关联节点");
			Utils.sleep(10000);
			jpo = StormNodeConfig.joinListTracker.joins.get(joinPo.JOIN_ID);
		}
		while (jpo.loadState != LoadState.Success) {
			LOG.warn("等待关联数据加载:" + joinPo.JOIN_ID + " loadState:" + jpo.loadState + " partitionLoadInfo:" +
					jpo.partitionLoadInfo);
			Utils.sleep(10000);
			jpo = StormNodeConfig.joinListTracker.joins.get(joinPo.JOIN_ID);
		}
		if (joinPo.joinPO.INLOCAL_MEMORY != 0) {// 内存
			JoinDataTable Jdt = JoinData.JoinTables.get(jpo.JOIN_ID);
			if (Jdt == null) {
				// 需要停止TOP
				LOG.error("内存关联数据[" + jpo.JOIN_ID + "]状态为加载成功,获取不到对应的内存表结构");
				sendStopTopology("内存关联数据[" + jpo.JOIN_ID + "]状态为加载成功,获取不到对应的内存表结构");
			} else {
				jtr.memData = Jdt.getBusTable(joinPo.JOIN_FIELD);
				if (jtr.memData == null) {
					// 需要停止TOP
					LOG.error("内存关联数据[" + jpo.JOIN_ID + "]状态为加载成功,对应的内存表中无对应的业务主键:" + joinPo.JOIN_FIELD +
							" all_bugkey:" + Jdt.getAllBusKey());
					sendStopTopology("内存关联数据[" + jpo.JOIN_ID + "]状态为加载成功,对应的内存表中无对应的业务主键:" + joinPo.JOIN_FIELD +
							" all_bugkey:" + Jdt.getAllBusKey());
				}
			}
		} else {// 初始化关联数据读取链接
			long startTime = System.currentTimeMillis();
			while (jtr.proStoreConn == null) {
				try {
					jtr.proStoreConn = new ProStoreConnection(this, conf, joinPo);
					if (jtr.proStoreConn.keyType == 1) {
						jtr.busKeyExpr = AviatorEvaluator.compile(jpo.busKeyRules[joinPo.useBusKeyIndex], true);
					}
				} catch (Exception e) {
					LOG.error("初始化数据存储连接失败,请检查存储配置和存储服务器," + e.getMessage(), e);
					Utils.sleep(5000);
					if (System.currentTimeMillis() - startTime > 600000) {
						// 发送停止对应TOP的命令
						sendStopTopology("初始化数据存储连接失败,请检查存储配置和存储服务器," + e.getMessage());
					}
				}
			}
		}
	}

	// TODO
	void sendStopTopology(String msg) throws Exception {
		throw new Exception(msg);
	}

	void findSubJoin(JoinTree join) throws Exception {
		int level0 = 0;
		for (NodeJoinPO joinPo : nodePo.joins) {
			if (joinPo.PAR_JOIN_ID == join.joinPo.JOIN_ID) {
				level0++;
			}
		}
		if (level0 == 0)
			return;
		join.subJoin = new JoinTree[level0];
		level0 = 0;
		for (NodeJoinPO joinPo : nodePo.joins) {
			if (joinPo.PAR_JOIN_ID == join.joinPo.JOIN_ID) {
				join.subJoin[level0] = new JoinTree(joinPo);
				joinRels.put(joinPo.JOIN_ID, join.subJoin[level0]);
				initJoinConn(join.subJoin[level0]);
				findSubJoin(join.subJoin[level0]);
				level0++;
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		NodePO npo = (NodePO) nodeInfo.node;
		List<String> fields = new ArrayList<String>();
		// Fields fds=new Fields(List<String> fields)
		if ((((NodePO) nodeInfo.node).outputMsgType & 1) > 0) {
			for (FieldPO f : npo.fields) {
				fields.add(f.FIELD_NAME);
			}
		}
		if ((((NodePO) nodeInfo.node).outputMsgType & 1) == 1) {
			declarer.declareStream(nodeInfo.nodeName + "_1", new Fields(fields));
		}
		if ((((NodePO) nodeInfo.node).outputMsgType & 1) == 2) {
			declarer.declareStream(nodeInfo.nodeName + "_0", new Fields(fields));
		}
	}

	@Override
	public void cleanup() {
		LOG.info("BOLT节点[" + this.nodeInfo.getNodeId() + "] 关闭");
		// StormNodeConfig.listen.remove(nodeInfo.nodeName, this);
		// StormNodeConfig.nodeConfig.remove(this.nodeZkPath, this);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return comConf;
	}

	@Override
	public NodeInfo getNodeInfo() {
		return this.nodeInfo;
	}

	@Override
	public int getTaskId() {
		return context.getThisTaskId();
	}

	@Override
	public void nodeConfigChanage(NodeInfo oldN, NodeInfo newN) {
		synchronized (context) {
			this.nodeInfo = newN;
			initNodePo(true);// 重新初始化
			// 后续变更事件
		}
		// 比对属性 执行命令
	}

	@Override
	public boolean pause() {
		this.isPause = true;
		lastPauseTupleNum = processTupleNum;
		pauseTime = System.currentTimeMillis();
		return true;
	}

	@Override
	public void recover() {
		this.isPause = false;
		stoped = false;
		// StormNodeConfig.listen.register(nodeInfo.nodeName, this);
		// StormNodeConfig.nodeConfig.register(this.nodeZkPath, this);
	}

	public boolean isStoped() {
		return this.stoped;
	}

	@Override
	public boolean stop() {
		LOG.info("BOLT节点[" + this.nodeInfo.getNodeId() + "] 停止");
		this.isPause = true;
		pauseTime = System.currentTimeMillis();
		// final BoltNode bn = this;

		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					lastPauseTupleNum = processTupleNum;
					long time = 0;
					long cl = System.currentTimeMillis();
					boolean isChanage = true;
					while (true) {
						while (true) {
							if (!isProcessTuple) {
								time = System.currentTimeMillis() - cl;
								if (lastPauseTupleNum == processTupleNum && time > Math.max(minWaitMillis / 5, 1000)) {
									if (isChanage == false) {// 至少二次不变，跳出
										break;
									}
									isChanage = false;
								}
								Utils.sleep(500);
								if (time > 30000) {
									lastPauseTupleNum = processTupleNum;
									cl = System.currentTimeMillis();
								}
							} else {
								lastPauseTupleNum = processTupleNum;
								isChanage = true;
								Utils.sleep(500);
							}
						}
						// 没有消息处理,等待超过minWaitMillis
						if (isProcessTuple == false && time >= minWaitMillis) {
							break;
						}
						Utils.sleep(100);
						time += 100;
					}
				} catch (Exception e) {
					Utils.sleep(10000);
				} finally {
					stoped = true;
					// StormNodeConfig.listen.remove(bn.nodeInfo.nodeName, bn);
					// StormNodeConfig.nodeConfig.remove(bn.nodeZkPath, bn);
				}
			}
		}).start();
		return true;
	}
}
