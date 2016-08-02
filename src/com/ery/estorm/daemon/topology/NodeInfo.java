package com.ery.estorm.daemon.topology;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.ery.estorm.NodeColumnCalc;
import com.ery.estorm.config.EStormConfigRead;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.exceptions.NodeParseException;
import com.ery.estorm.util.Addressing;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperWatcher;

public class NodeInfo implements Serializable {
	private static final long serialVersionUID = -4398612288809877114L;
	private static final Log LOG = LogFactory.getLog(NodeInfo.class);
	public InOutPO node;
	private volatile State state;
	private String __order;// Node的命令
	/**
	 * 远端处理用，本地写入ZK后置0，远端处理后置0 变更类型 1:基本信息 2:输入变更 4:输出变更(包括计算变更) 8:存储变更 16:关联数据变更 32:关联项目变更 64:订阅变更
	 */
	public int modifyType;
	private int taskNum;

	public String nodeName;// 名称前缀区分// msg join node
	public long stime = System.currentTimeMillis();
	public long nodeId;// 原始ID 一个消息被多个bolt使用 ，并且不在同一个流中,消息不写ZK节点
	public int nodeType = 0;// 1spout 2bolt

	public enum State {
		OPENING, // server has begun to open but not yet done
		PENDING, // sent rpc to server to close but has not begun
		RUNNING, // server has begun to close but not yet done
		CLOSING, //
		CLOSED, // server closed region and updated meta
	}

	// 需要重写复制属性
	public NodeInfo(InOutPO node) {
		this.node = node;
	}

	public int getTaskNum() {
		return taskNum;
	}

	public void setTaskNum(int num) {
		taskNum = num;
	}

	// public NodeInfo clone() {
	// return new NodeInfo(node.clone());
	// }

	// 全部属性参与计算
	public int hashCode() {
		return node.hashCode();
	}

	// public String getNodeName() {
	// return node.getNodeName();
	// }
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("NodeInf{");
		sb.append("nodeName:" + nodeName);
		sb.append(",state:" + state);
		sb.append(",stime:" + EStormConstant.sdf.format(new Date(stime)));
		sb.append(",nodeId:" + nodeId);
		sb.append(",nodeType:" + (nodeType == 1 ? "spout" : nodeType == 2 ? "bolt" : "unknown"));
		sb.append(",\nnode:{");
		sb.append(node.toString());
		sb.append("}");
		return sb.toString();
	}

	public InOutPO getNode() {
		return node;
	}

	// 原始配置的ID
	public long getNodeId() {
		return node.getNodeId();
	}

	public void updateToZk(ZooKeeperWatcher watcher) throws IOException, KeeperException {
		String path = ZKUtil.joinZNode(watcher.estormNodeZNode, this.nodeName);
		byte[] data = EStormConstant.Serialize(this);
		ZKUtil.createSetData(watcher, path, data);
	}

	public void deleteToZk(ZooKeeperWatcher watcher) {
		String path = ZKUtil.joinZNode(watcher.estormNodeZNode, this.nodeName);
		try {
			ZKUtil.deleteNode(watcher, path);
		} catch (Exception e) {
		}
	}

	public static final String SERVERNAME_SEPARATOR = ",";
	public static final Pattern SERVERNAME_PATTERN = Pattern.compile("[^" + SERVERNAME_SEPARATOR + "]+" + SERVERNAME_SEPARATOR
			+ Addressing.VALID_PORT_REGEX + "$");

	public abstract static class InOutPO implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = -6748814314335566446L;
		public List<FieldPO> fields;// 自身输出字段
		public String INPUT_FILTER;// 过虑表达式,用于输入
		public String OUTPUT_FILTER;// 输出过滤表达式
		protected boolean inited = false;

		public FieldPO[] nodeOutFields = null;
		public Map<String, Integer> fieldNameRel = null;

		// public abstract InOutPO clone();

		public abstract int CompareBase(InOutPO po);

		public int modifyType;// 1:新增 2：修改 远端处理用，本地写入ZK后置0

		// public abstract String getNodeName();

		public abstract long getNodeId();

		public void init() throws NodeParseException {
			if (this.fields != null && this.fields.size() > 0) {
				nodeOutFields = new FieldPO[this.fields.size()];
				fieldNameRel = new HashMap<String, Integer>();
				int index = 0;
				for (index = 0; index < this.fields.size(); index++) {
					FieldPO f = this.fields.get(index);
					nodeOutFields[index] = f;
					fieldNameRel.put(f.FIELD_NAME, index);
					f.filedType = convertDataTypeToByte(f.FIELD_DATA_TYPE);
				}
			}
		}

		public String toString() {
			try {
				return EStormConstant.objectMapper.writeValueAsString(this);
			} catch (Exception e) {
				return super.toString();
			}
		}
	}

	public static byte convertDataTypeToByte(String fIELDDATATYPE) {
		String type = fIELDDATATYPE.toUpperCase();
		if (type.equals("NUMBER")) {
			return 2;
		} else if (type.startsWith("NUMBER")) {
			return 4;
		} else if (type.equals("DATE") || type.equals("TIME_LONG") || type.startsWith("TIME_STR")) {
			return 8;
		} else {
			return 1;
		}
	}

	public static class FieldPO implements Serializable {
		private static final long serialVersionUID = 1230093713707286563L;
		// 定义输入对象
		public long FIELD_ID;
		public String FIELD_NAME;
		/**
		 * 字段类型 VARCHAR --字符串 NUMBER --整数（包括负数） NUMBER(2) --小数，括号中的值标示小数位数 TIME_STR(yyyyMMddHHmmss) 时间字符串，括号里面标示字符格式，无括号时，默认格式即此示例 TIME_LONG
		 * 时间毫秒，展示或存储时需要转换
		 */
		public String FIELD_DATA_TYPE;
		public byte filedType;
		public int ORDER_ID;// 顺序
	}

	// 任务节点对象
	public static class NodePO extends InOutPO {
		private static final long serialVersionUID = -7131991359879133036L;
		public long NODE_ID;// 节点ID
		public String NODE_NAME;// 节点名称
		public String NODE_DESC;
		public int WORKER_NUM;// 工作数
		public int MAX_PARALLELISM_NUM;// 节点并发线程数（整个集群）
		public int MAX_PENDING;// 控制节点最大等待消息数
		public int IS_DEBUG;// 是否调试0否，1是
		public int LOG_LEVEL;// 0,不记录 1,记录异常 2,记录详细（接收，计算，结果……）
		public String MODIFY_TIME;
		public String NODE_PARAMS;

		public List<NodeInput> inputs;// 输入对象 NodePO MsgPO
		public List<NodeJoinPO> joins;// 输入对象 NodeJoinPO
		// public List<InOutPO> outputInfos;// 输出对象
		public List<InOutPO> storeInfos;// 存储对象
		public List<InOutPO> pushInfos;// 订阅对象
		public int maxBufferSize = 1000;

		public boolean haveGroupByCalc = false;
		public int outputMsgType = 0;// 0无输出：1增量 2全量 3增全量

		public int[] destGroupFileds;// 目标数据的分组计算主键
		public int[] busKeyFileds;// 目标数据的查询主键

		// public String getNodeName() {
		// return NODE_NAME;
		// }

		public long getNodeId() {
			return NODE_ID;
		}

		// 跨级关联数据
		// 当前允许跨级关联 2级 使用 输入和1级的字段来关联，必需字段名唯一
		public boolean parseJoinParentRel(InputFieldRel inputFRel, NodeJoinPO ppo, String colName) {
			if (ppo.parJoinPO != null) {// 有父级
				Integer index = ppo.parJoinPO.joinPO.fieldNameRel.get(colName);
				if (index != null) {
					inputFRel.inputIndex = index;
					inputFRel.inputPO = ppo.parJoinPO.joinPO;// JoinPO
					inputFRel.inputName = colName;
					return true;
				} else {
					return parseJoinParentRel(inputFRel, ppo.parJoinPO, colName);
				}
			} else {// 从输入中顺序查找，所以要求字段名唯一，不能在多个输入中同时存在
				Integer index = null;
				for (NodeInput input : this.inputs) {
					Integer _index = input.inputPo.fieldNameRel.get(colName);
					if (index != null && _index != null) {
						LOG.warn("与父级的关联字段名[" + colName + "]在多个输入中同时存在，默认使用第一个，请最好明确指定唯一名称");
					} else if (_index != null) {
						index = _index;
						inputFRel.inputIndex = index;
						inputFRel.inputPO = input.inputPo;// MsgPO NodePO
						inputFRel.inputName = colName;
					}
				}
				if (index != null)
					return true;
				else
					return false;
			}
		}

		public void reInit() throws NodeParseException {
			inited = false;
			init();
		}

		// 解析初始节点对象
		public void init() throws NodeParseException {
			if (inited)
				return;
			try {
				haveGroupByCalc = false;

				super.init();
				// 先解析输入字段
				if (this.inputs == null || this.inputs.size() == 0)
					throw new NodeParseException("节点[" + this.NODE_ID + "]规则配置错误，无对应的输入配置");
				HashMap<Long, NodeInput> nodePOInputRel = new HashMap<Long, NodeInput>();
				int inputSize = 0;
				for (NodeInput input : this.inputs) {
					input.index = inputSize++;
					if (input.inputPo == null) {
						throw new NodeParseException("节点[" + this.NODE_ID + "]规则配置错误，输入配置[" + input.INPUT_ID + "]对应的输入ID["
								+ input.INPUT_SRC_ID + "]未关联到输入项配置");
					}
					input.inputPo.init();
					nodePOInputRel.put(input.INPUT_ID, input);
				}
				// ===begin parse join=======================================

				// 判断每一个节点的关联表关联链是否完整
				HashMap<Long, NodeJoinPO> nodePOJoinRel = new HashMap<Long, NodeJoinPO>();
				Map<Long, Long> joinRelInput = new HashMap<Long, Long>();
				if (this.joins != null && this.joins.size() > 0) {
					for (NodeJoinPO joinPO : this.joins) {
						nodePOJoinRel.put(joinPO.joinPO.JOIN_ID, joinPO);
					}
					for (NodeJoinPO joinPO : this.joins) {
						String joinjf[] = joinPO.JOIN_FIELD.split(",");
						String parjf[] = joinPO.PAR_JOIN_FIELD.split(",");
						if (joinjf.length != parjf.length) {
							throw new NodeParseException("节点[" + this.NODE_ID + "]规则配置错误，关联配置[" + joinPO.JOIN_ID + "]的关联字段不对待["
									+ joinPO.JOIN_FIELD + " ==== " + joinPO.PAR_JOIN_FIELD + "]");
						}
						String[] thisBusKeys = joinPO.joinPO.BUS_PRIMARYS.split(";");
						boolean findBusKey = false;
						for (int n = 0; n < thisBusKeys.length; n++) {
							if (thisBusKeys[n].equals(joinPO.JOIN_FIELD)) {
								findBusKey = true;
								joinPO.useBusKeyIndex = n;
								break;
							}
						}
						if (joinPO.PAR_JOIN_ID == 0) {
							// 判断是否能关联上输入
							long joinInputId = 0;
							for (int n = 0; n < parjf.length; n++) {// INPUT_ID1.field1，INPUT_ID2.field2,
								String tmp[] = parjf[n].split("\\.");
								if (tmp.length != 2) {
									throw new NodeParseException("节点[" + this.NODE_ID + "]关联数据规则[" + joinPO.JOIN_ID + "]配置错误，父级关联字段格式配置错误");
								}
								long inputId = Long.parseLong(tmp[0]);
								NodeInput input = nodePOInputRel.get(inputId);
								if (input == null) {
									throw new NodeParseException("节点[" + this.NODE_ID + "]关联数据规则[" + joinPO.JOIN_ID + "]配置错误，关联字段["
											+ joinjf[n] + "]对应的父级关联字段[" + joinjf[n] + "]在输入中不存在");
								}
								Integer index = input.inputPo.fieldNameRel.get(tmp[1]);
								if (index == null) {
									throw new NodeParseException("节点[" + this.NODE_ID + "]关联数据规则[" + joinPO.JOIN_ID + "]的配置错误，与父关联的关联字段["
											+ parjf[n] + "]不在父关联输出字段中");
								}
								if (joinInputId == 0) {
									joinInputId = inputId;
								} else if (joinInputId != inputId) {
									throw new NodeParseException("节点[" + this.NODE_ID + "]关联数据规则[" + joinPO.JOIN_ID
											+ "]的配置错误，与父关联的关联字段必需来源于一个输入[" + joinInputId + "," + inputId + "]");
								}
								joinRelInput.put(joinPO.JOIN_ID, joinInputId);
								joinPO.parJoinRel[n] = new InputFieldRel();
								joinPO.parJoinRel[n].inputName = joinjf[n];//
								joinPO.parJoinRel[n].inputPO = input.inputPo;// MsgPO NodePO
								joinPO.parJoinRel[n].inputIndex = index;
								joinPO.parJoinRel[n].srcType = input.INPUT_TYPE;
								joinPO.parJoinRel[n].srcId = input.INPUT_SRC_ID;
							}
						} else {
							NodeJoinPO ppo = nodePOJoinRel.get(joinPO.PAR_JOIN_ID);
							if (findBusKey == false || parjf.length != joinjf.length || ppo == null) {
								if (findBusKey == false) {
									throw new NodeParseException("节点[" + this.NODE_ID + "]关联数据规则[" + joinPO.JOIN_ID + "]配置错误，关联字段不在业务主键列表中");
								} else if (ppo == null) {
									throw new NodeParseException("节点[" + this.NODE_ID + "]关联数据规则[" + joinPO.JOIN_ID + "]配置错误，父关联["
											+ joinPO.PAR_JOIN_ID + "]不在此节点的输入关联列表中");
								} else {
									throw new NodeParseException("节点[" + this.NODE_ID + "]关联数据规则[" + joinPO.JOIN_ID
											+ "]的关联关系字段配置错误，关联字段与父表[" + joinPO.PAR_JOIN_ID + "]关联字段数不一致");
								}
							}

							NodeJoinPO parJPO = ppo;
							while (parJPO.PAR_JOIN_ID != 0) {
								parJPO = nodePOJoinRel.get(parJPO.PAR_JOIN_ID);
							}
							joinRelInput.put(joinPO.JOIN_ID, joinRelInput.get(parJPO.JOIN_ID));

							joinPO.parJoinPO = ppo;
							joinPO.parJoinRel = new InputFieldRel[joinjf.length];
							for (int n = 0; n < joinjf.length; n++) {
								Integer index = ppo.joinPO.fieldNameRel.get(parjf[n]);
								joinPO.parJoinRel[n] = new InputFieldRel();
								if (index == null) {// 当前允许跨级关联 2级 使用 输入和1级的字段来关联，必需字段名唯一
									if (parseJoinParentRel(joinPO.parJoinRel[n], ppo, parjf[n]) == false)
										throw new NodeParseException("节点[" + this.NODE_ID + "]关联数据规则[" + joinPO.JOIN_ID
												+ "]的配置错误，与父关联的关联字段[" + parjf[n] + "]不在父关联及其上级输出字段中");
								} else {
									joinPO.parJoinRel[n].inputName = joinjf[n];//
									joinPO.parJoinRel[n].inputPO = ppo.joinPO;// JoinPO
									joinPO.parJoinRel[n].inputIndex = index;
									joinPO.parJoinRel[n].srcType = 2;
									joinPO.parJoinRel[n].srcId = ppo.JOIN_ID;
								}
							}
						}
						joinPO.joinPO.init();
					}
				}
				// ===end parse join=======================================

				// 解析输出字段与输入字段规则
				Map<Long, NodeFieldPO> groupbyFileds = new HashMap<Long, NodeFieldPO>();
				int busKeyLen = 0;
				for (int i = 0; i < this.fields.size(); i++) {
					NodeFieldPO nfpo = (NodeFieldPO) this.fields.get(i);
					nfpo.calcMethod = GroupMethod.valueOf(nfpo.GROUP_METHOD);
					if (nfpo.calcMethod == GroupMethod.NONE) {
						groupbyFileds.put(nfpo.FIELD_ID, nfpo);
					}
					if (nfpo.IS_BUS_KEY != 0)
						busKeyLen++;
				}
				busKeyFileds = new int[busKeyLen];
				busKeyLen = 0;
				if (groupbyFileds.size() != this.fields.size()) {
					haveGroupByCalc = true;
					for (int i = 0; i < this.fields.size(); i++) {
						NodeFieldPO nfpo = (NodeFieldPO) this.fields.get(i);
						if (nfpo.calcMethod == GroupMethod.NONE) {
							if (nfpo.IS_BUS_KEY == 0)
								throw new NodeParseException("节点[" + this.NODE_ID + "]业务主键与Groupby字段配置不一致");
						}
					}
				}
				for (int idx = 0; idx < this.fields.size(); idx++) {
					NodeFieldPO nfpo = (NodeFieldPO) this.fields.get(idx);
					if (nfpo.IS_BUS_KEY != 0)
						busKeyFileds[busKeyLen++] = idx;
					// 先解析 INPUT_FIELD_MAPPING
					Map<String, InputFieldRel> inputFieldMacMap = new HashMap<String, InputFieldRel>();
					if (nfpo.INPUT_FIELD_MAPPING != null && !nfpo.INPUT_FIELD_MAPPING.trim().equals("")) {
						/**
						 * 输入映射，【宏变量名：来源类型：来源ID：来源字段】，格式如下： a：0：1 ： field1 来源类型：0消息，1节点输出字段，2关联表输出字段 防止各输入中有字段重复，无重复进可以不配置此项
						 */
						String rtmp[] = nfpo.INPUT_FIELD_MAPPING.split(",");
						for (String tmp : rtmp) {
							String tmps[] = tmp.split(":");
							String key = tmps[0];
							if (tmps.length < 4) {
								LOG.warn("节点[" + this.NODE_ID + "]字段[" + nfpo.FIELD_ID + ":" + nfpo.FIELD_NAME + "]输入映射配置存在错误:" + " 输入变量："
										+ key + "的来源格式错误:" + tmp);
								continue;
							}
							// a：0：1 ： field1
							InputFieldRel value = null;
							if (tmps[1].equals("0") || tmps[1].equals("1")) {
								for (NodeInput input : this.inputs) {
									if (input.INPUT_TYPE == Integer.parseInt(tmps[1]) && Long.parseLong(tmps[2]) == input.INPUT_SRC_ID) {
										Integer index = input.inputPo.fieldNameRel.get(tmps[3]);
										if (index != null) {
											value = new InputFieldRel();
											value.inputPO = input.inputPo;
											value.inputName = key;
											value.inputIndex = index;
											value.srcType = input.INPUT_TYPE;
											value.srcId = input.INPUT_SRC_ID;
											if (haveGroupByCalc && nfpo.calcMethod == GroupMethod.NONE) {
												if (input.groupFields == null)
													input.groupFields = new ArrayList<String>();
												if (input.groupFields.contains(tmps[3]) == false)
													input.groupFields.add(tmps[3]);
											}
											break;
										}
									}
								}
								if (value == null) {
									LOG.warn("节点[" + this.NODE_ID + "]字段[" + nfpo.FIELD_ID + ":" + nfpo.FIELD_NAME + "]输入映射配置存在错误:"
											+ " 输入变量：" + key + "的来源字段在输入消息或节点中不存在:");
									continue;
								} else {
									inputFieldMacMap.put(key, value);
								}
							} else if (tmps[1].equals("2")) {
								for (NodeJoinPO join : this.joins) {
									Integer index = join.joinPO.fieldNameRel.get(key);
									if (index != null) {
										value = new InputFieldRel();
										value.inputPO = join.joinPO;
										value.inputName = key;
										value.inputIndex = index;
										value.srcType = 2;
										value.srcId = join.joinPO.JOIN_ID;
										break;
									}
								}
								if (value == null) {
									LOG.warn("节点[" + this.NODE_ID + "]字段[" + nfpo.FIELD_ID + ":" + nfpo.FIELD_NAME + "]输入映射配置存在错误:"
											+ " 输入变量：" + key + "的来源字段在关联表输出字段中不存在:");
									continue;
								} else {
									inputFieldMacMap.put(key, value);
								}
							}
							inputFieldMacMap.put(key, value);
						}
					}
					if (nfpo.IS_BUS_KEY != 0 && this.inputs.size() > 1) {// 业务主键,如果有Group,要求与业务主键一致 不同业务主键的消息不合并,做关联
						nfpo.InputRels = new InputFieldRel[this.inputs.size()][];
						nfpo.calcExprs = new String[this.inputs.size()];
						String expStrs[] = nfpo.CALC_EXPR.split(";");
						if (expStrs.length != this.inputs.size()) {
							throw new NodeParseException("节点[" + this.NODE_ID + "]字段[" + nfpo.FIELD_ID + "]配置错误,"
									+ "业务主键字段的计算必需规则数必需与输入来源数相同 计算规则:" + nfpo.CALC_EXPR + " 输入来源数:" + this.inputs.size());
						}
						for (int x = 0; x < expStrs.length; x++) {// 与各输入对应
							String CALC_EXPR = expStrs[x];
							String tmp[] = CALC_EXPR.split(":");
							// 查找input
							int inputIndex = 0;
							for (inputIndex = 0; inputIndex < this.inputs.size(); inputIndex++) {
								if (this.inputs.get(inputIndex).INPUT_ID == Long.parseLong(tmp[0])) {
									break;
								}
							}

							if (nfpo.CALC_EXPR.startsWith("class:")) {//
								nfpo.calcType = 3;
								// 判断类是否存在
								if (tmp.length < 4) {
									throw new NodeParseException("节点[" + this.NODE_ID + "]字段[" + nfpo.FIELD_ID
											+ "]计算类配置错误,格式如下：INPUTID:class:classname:inputfield,inputfield");
								}
								if (NodeColumnCalc.getNodeColumnCalc(tmp[2]) == null) {
									throw new NodeParseException("计算类配置错误,类[" + tmp[2] + "]不存在，请检查是否存在编译错误");
								}
								nfpo.calcExprs[inputIndex] = tmp[2];
								// 解析输入参数列表
								String[] inputFields = tmp[3].split(",");
								nfpo.calcExprs[inputIndex] = tmp[3];
								nfpo.InputRels[inputIndex] = new InputFieldRel[inputFields.length];
								for (int j = 0; j < inputFields.length; j++) {
									InputFieldRel fr = inputFieldMacMap.get(inputFields[j]);
									if (fr != null) {// 存在字段映射
										nfpo.InputRels[inputIndex][j] = fr;
									} else {
										nfpo.InputRels[inputIndex][j] = new InputFieldRel();
										NodeInput input = this.inputs.get(inputIndex);
										Integer index = input.inputPo.fieldNameRel.get(inputFields[j]);
										if (index != null) {
											nfpo.InputRels[inputIndex][j].inputIndex = index;
											nfpo.InputRels[inputIndex][j].inputPO = input.inputPo;// MsgPO NodePO
											nfpo.InputRels[inputIndex][j].inputName = inputFields[j];
											nfpo.InputRels[inputIndex][j].srcType = input.INPUT_TYPE;
											nfpo.InputRels[inputIndex][j].srcId = input.INPUT_SRC_ID;
											if (nfpo.IS_BUS_KEY != 0 || (haveGroupByCalc && nfpo.calcMethod == GroupMethod.NONE)) {
												if (input.groupFields == null)
													input.groupFields = new ArrayList<String>();
												if (input.groupFields.contains(inputFields[j]) == false)
													input.groupFields.add(inputFields[j]);
											}
										}

										if (index == null) {
											throw new NodeParseException("节点[" + this.NODE_ID + "]业务主键字段[" + nfpo.FIELD_ID + "]配置错误,输入字段["
													+ inputFields[j] + "]在输入[" + input.INPUT_ID + "]中不存在，请检查是否存在编译错误");
										}
									}
								}
							} else {
								CALC_EXPR = CALC_EXPR.substring(tmp[0].length() + 1);// 去除输入ID
								nfpo.calcExprs[inputIndex] = CALC_EXPR;
								String[] colMacs = AviatorEvaluator.compile(CALC_EXPR, true).getVariableNames().toArray(new String[0]);
								if (colMacs.length == 0) {// 0:常量 1single 2:calc expr 3:class
									nfpo.calcType = 0;// 暂时不考虑动态时间
									nfpo.constant = AviatorEvaluator.compile(CALC_EXPR, true).execute();
								} else if (colMacs.length == 1 && colMacs[0].length() == CALC_EXPR.trim().length()) {
									nfpo.calcType = 1;
								} else {// 0:常量 1single 2:calc expr 3:class
									nfpo.calcType = 2;
								}
								if (colMacs.length > 0) {
									nfpo.InputRels[inputIndex] = new InputFieldRel[colMacs.length];
									for (int j = 0; j < colMacs.length; j++) {
										InputFieldRel fr = inputFieldMacMap.get(colMacs[j]);
										if (fr != null) {// 存在字段映射
											nfpo.InputRels[inputIndex][j] = fr;
										} else {
											nfpo.InputRels[inputIndex][j] = new InputFieldRel();
											NodeInput input = this.inputs.get(inputIndex);
											Integer index = input.inputPo.fieldNameRel.get(colMacs[j]);
											if (index != null) {
												nfpo.InputRels[inputIndex][j].inputIndex = index;
												nfpo.InputRels[inputIndex][j].inputPO = input.inputPo;// MsgPO NodePO
												nfpo.InputRels[inputIndex][j].inputName = colMacs[j];
												nfpo.InputRels[inputIndex][j].srcType = input.INPUT_TYPE;
												nfpo.InputRels[inputIndex][j].srcId = input.INPUT_SRC_ID;
												if (nfpo.IS_BUS_KEY != 0 || (haveGroupByCalc && nfpo.calcMethod == GroupMethod.NONE)) {
													if (input.groupFields == null)
														input.groupFields = new ArrayList<String>();
													if (input.groupFields.contains(colMacs[j]) == false)
														input.groupFields.add(colMacs[j]);
												}
											}

											if (index == null) {
												throw new NodeParseException("节点[" + this.NODE_ID + "]业务主键字段[" + nfpo.FIELD_ID
														+ "]配置错误,输入字段[" + colMacs[j] + "]在输入[" + input.INPUT_ID + "]中不存在，请检查是否存在编译错误");
											}
										}
									}
								}
								// AviatorEvaluator.compile(indexRule.ROWKEY_RULE, true);
							}
						}
					} else {
						nfpo.InputRels = new InputFieldRel[1][];
						nfpo.calcExprs = new String[1];
						if (nfpo.CALC_EXPR.startsWith("class:")) {//
							nfpo.calcType = 3;
							// 判断类是否存在
							String tmp[] = nfpo.CALC_EXPR.split(":");
							if (tmp.length < 3) {
								throw new NodeParseException("节点[" + this.NODE_ID + "]字段[" + nfpo.FIELD_ID
										+ "]计算类配置错误,格式如下：class:classname:inputfield,inputfield");
							}
							if (NodeColumnCalc.getNodeColumnCalc(tmp[1]) == null) {
								throw new NodeParseException("计算类配置错误,类[" + tmp[1] + "]不存在，请检查是否存在编译错误");
							}
							nfpo.calcExprs[0] = tmp[1];
							// 解析输入参数列表
							String[] inputFields = tmp[2].split(",");
							nfpo.InputRels[0] = new InputFieldRel[inputFields.length];
							for (int j = 0; j < inputFields.length; j++) {
								InputFieldRel fr = inputFieldMacMap.get(inputFields[j]);
								if (fr != null) {// 存在字段映射
									nfpo.InputRels[0][j] = fr;
								} else {
									nfpo.InputRels[0][j] = new InputFieldRel();
									Integer index = null;
									for (NodeInput input : this.inputs) {
										Integer _index = input.inputPo.fieldNameRel.get(inputFields[j]);
										if (index != null && _index != null) {
											LOG.warn("输入字段名[" + inputFields[j] + "]在多个输入中同时存在，默认使用第一个，请最好明确指定唯一名称"
													+ "或者在字段[INPUT_FIELD_MAPPING]中配置映射规则");
										} else if (_index != null) {
											index = _index;
											nfpo.InputRels[0][j].inputIndex = index;
											nfpo.InputRels[0][j].inputPO = input.inputPo;// MsgPO NodePO
											nfpo.InputRels[0][j].inputName = inputFields[j];
											nfpo.InputRels[0][j].srcType = input.INPUT_TYPE;
											nfpo.InputRels[0][j].srcId = input.INPUT_SRC_ID;
											if (nfpo.IS_BUS_KEY != 0 || (haveGroupByCalc && nfpo.calcMethod == GroupMethod.NONE)) {
												if (input.groupFields == null)
													input.groupFields = new ArrayList<String>();
												if (input.groupFields.contains(inputFields[j]) == false)
													input.groupFields.add(inputFields[j]);
											}
										}
									}
									// 从Join中解析
									if (index == null) {
										throw new NodeParseException("节点[" + this.NODE_ID + "]字段[" + nfpo.FIELD_ID + "]计算类配置错误,输入字段["
												+ inputFields[j] + "]在输入中不存在，请检查是否存在编译错误");
									}
								}
							}
						} else {
							nfpo.calcExprs[0] = nfpo.CALC_EXPR;
							String[] colMacs = AviatorEvaluator.compile(nfpo.CALC_EXPR, true).getVariableNames().toArray(new String[0]);
							if (colMacs.length == 0) {// 0:常量 1single 2:calc expr 3:class
								nfpo.calcType = 0;// 暂时不考虑动态时间
								nfpo.constant = AviatorEvaluator.compile(nfpo.CALC_EXPR, true).execute();
							} else if (colMacs.length == 1 && colMacs[0].length() == nfpo.CALC_EXPR.trim().length()) {
								nfpo.calcType = 1;
							} else {// 0:常量 1single 2:calc expr 3:class
								nfpo.calcType = 2;
							}
							if (colMacs.length > 0) {
								nfpo.InputRels[0] = new InputFieldRel[colMacs.length];
								for (int j = 0; j < colMacs.length; j++) {
									InputFieldRel fr = inputFieldMacMap.get(colMacs[j]);
									if (fr != null) {// 存在字段映射
										nfpo.InputRels[0][j] = fr;
									} else {
										nfpo.InputRels[0][j] = new InputFieldRel();
										Integer index = null;
										for (NodeInput input : this.inputs) {
											Integer _index = input.inputPo.fieldNameRel.get(colMacs[j]);
											if (index != null && _index != null) {
												LOG.warn("与父级的关联字段名[" + colMacs[j] + "]在多个输入中同时存在，默认使用第一个，请最好明确指定唯一名称");
											} else if (_index != null) {
												index = _index;
												nfpo.InputRels[0][j].inputPO = input.inputPo;// MsgPO NodePO
												nfpo.InputRels[0][j].inputIndex = index;
												nfpo.InputRels[0][j].inputName = colMacs[j];
												nfpo.InputRels[0][j].srcType = input.INPUT_TYPE;
												nfpo.InputRels[0][j].srcId = input.INPUT_SRC_ID;
												if (nfpo.IS_BUS_KEY != 0 || (haveGroupByCalc && nfpo.calcMethod == GroupMethod.NONE)) {
													if (input.groupFields == null)
														input.groupFields = new ArrayList<String>();
													if (input.groupFields.contains(colMacs[j]) == false)
														input.groupFields.add(colMacs[j]);
												}
											}
										}
										// 从Join中解析
										if (index == null) {
											throw new NodeParseException("节点[" + this.NODE_ID + "]字段[" + nfpo.FIELD_ID + "]计算类配置错误,输入字段["
													+ colMacs[j] + "]在输入中不存在，请检查是否存在编译错误");
										}
									}
								}
							}
							// AviatorEvaluator.compile(indexRule.ROWKEY_RULE, true);
						}
					}
				}

				Map<String, Boolean> allInputJoinFileds = new HashMap<String, Boolean>();
				if (this.joins != null && this.joins.size() > 0) {// 存在数据关联
					for (NodeJoinPO jpo : this.joins) {
						if (jpo.PAR_JOIN_ID == 0) {
							String tmp[] = jpo.PAR_JOIN_FIELD.split(",");
							for (String fd : tmp)
								allInputJoinFileds.put(fd, true);
							// jpo.
						}
					}
				}
				if (allInputJoinFileds.size() > 0) {
					for (NodeInput input : this.inputs) {
						for (FieldPO fpo : input.inputPo.fields) {
							if (allInputJoinFileds.containsKey(fpo.FIELD_NAME)) {
								if (input.groupFields == null)
									input.groupFields = new ArrayList<String>();
								if (input.groupFields.contains(fpo.FIELD_NAME) == false)
									input.groupFields.add(fpo.FIELD_NAME);
							}
						}
					}
				}
				if (this.inputs.size() > 1) {// 判断各输入是否有相同的Groupby字段
					for (NodeInput input : this.inputs) {
						if (input.groupFields == null) {
							throw new NodeParseException("节点[" + this.NODE_ID + "]输入[" + input.INPUT_ID + "]的分组字段解析不正确 ，请检查是否存在编译错误");
						}
					}
				}

				if (haveGroupByCalc) {
					destGroupFileds = new int[groupbyFileds.size()];
					int index = 0;
					Map<Long, Long> msgRelInput = new HashMap<Long, Long>();
					for (NodeInput input : this.inputs) {
						msgRelInput.put(Long.parseLong(input.INPUT_TYPE + "" + input.INPUT_SRC_ID), input.INPUT_ID);
					}
					for (int i = 0; i < this.fields.size(); i++) {
						NodeFieldPO nfpo = (NodeFieldPO) this.fields.get(i);
						if (nfpo.calcMethod == GroupMethod.NONE) {
							destGroupFileds[index++] = i;
						}
						// Group 主键不能从关联表计算, 分组计算时,一个计算字段必需来源于一批次输入(一个输入以及其能关联出的数据)
						if (this.inputs.size() > 1) {
							long thisFiledInputId = 0;
							for (int c = 0; c < nfpo.InputRels.length; c++) {
								for (InputFieldRel ifr : nfpo.InputRels[c]) {
									if (nfpo.calcMethod == GroupMethod.NONE && ifr.srcType == 2) {
										throw new NodeParseException("节点[" + this.NODE_ID
												+ "]配置错误,存在多个输入时不允许将关联出的字段用来做Group By字段,请将此类业务拆分成二个Bolt实现,一个合并输入,一个汇总");
									}
									long _thisFiledInputId = 0;
									if (ifr.srcType == 1 || ifr.srcType == 0) {
										_thisFiledInputId = msgRelInput.get(Long.parseLong(ifr.srcType + "" + ifr.srcId));
									} else {// ifr.srcType == 2
										_thisFiledInputId = joinRelInput.get(ifr.srcId);
									}
									if (thisFiledInputId == 0) {
										thisFiledInputId = _thisFiledInputId;
									} else {
										if (thisFiledInputId != _thisFiledInputId) {
											throw new NodeParseException("节点[" + this.NODE_ID
													+ "]配置错误,存在多个输入时不允许计算字段来源于多个输入或与输入对应的关联表,请将此类业务拆分成二个Bolt实现,一个合并输入,一个汇总");
										}
									}
								}
							}
						}
					}
					for (NodeInput input : this.inputs) {
						input.destGroupFileds = new int[input.groupFields.size()];
						for (int c = 0; c < input.destGroupFileds.length; c++) {
							input.destGroupFileds[c] = input.inputPo.fieldNameRel.get(input.groupFields.get(c));
						}
					}
				} else {
					int glen = 0;
					for (int i = 0; i < this.fields.size(); i++) {
						NodeFieldPO nfpo = (NodeFieldPO) this.fields.get(i);
						if (nfpo.IS_BUS_KEY > 0)
							glen++;
					}
					destGroupFileds = new int[glen];
					int index = 0;
					for (int i = 0; i < this.fields.size(); i++) {
						NodeFieldPO nfpo = (NodeFieldPO) this.fields.get(i);
						if (nfpo.IS_BUS_KEY > 0)
							destGroupFileds[index++] = i;
					}
				}
				// 需要额外检查: 单输入,BUSKEY 可以来自于JOIN 表, 多输入时BUSKEY不能来自于JOIN表, 各输入都需要包含BUSKEY的计算

				inited = true;
			} catch (Exception e) {
				throw new NodeParseException(e);
			}
		}

		@Override
		public int CompareBase(InOutPO po) {
			// TODO Auto-generated method stub
			return 0;
		}

		// // 全部参数
		// public int hashCode() {
		// HashCodeBuilder builder = new HashCodeBuilder();
		// builder.append(NODE_ID);
		// builder.append(NODE_NAME);
		// builder.append(NODE_DESC);
		// builder.append(NODE_ID);
		// }
		// @Override
		// public NodePO clone() {
		// // // NodePo po = new NodePo();
		// // // po.NODE_ID = this.NODE_ID;
		// // // po.NODE_NAME=this.NODE_NAME;// 节点名称
		// // // po.NODE_DESC=this.NODE_DESC;
		// // // po.WORKER_NUM=this.WORKER_NUM;// 工作数
		// // // po.MAX_PARALLELISM_NUM=this.MAX_PARALLELISM_NUM;// 节点并发线程数（整个集群）
		// // // po.MAX_PENDING=this.MAX_PENDING;// 控制节点最大等待消息数
		// // // po.IS_DEBUG=this.IS_DEBUG;// 是否调试0否，1是
		// // // po.LOG_LEVEL=this.LOG_LEVEL;// 0,不记录 1,记录异常 2,记录详细（接收，计算，结果……）
		// // // po.MODIFY_TIME=this.MODIFY_TIME;
		// // // po.INPUT_FILTER=this.INPUT_FILTER;
		// // // po.OUTPUT_FILTER=this.OUTPUT_FILTER;
		// // //
		// // //
		// // // public List<Field> fields;// 自身输出字段
		// // // public List<InOutPo> inputInfos;// 输入对象 NodePo MsgPo NodeJoinPo 三种
		// // // public List<InOutPo> outputInfos;// 输出对象
		// // // public List<InOutPo> storeInfos;// 存储对象
		// // // public List<InOutPo> pushInfos;// 存储对象
		// //
		// return this;
		// }

	}

	public static class InputFieldRel implements Serializable {
		private static final long serialVersionUID = -3750155708086499823L;
		public String inputName = null;
		public InOutPO inputPO = null;
		public int inputIndex = -1;
		public int srcType = 0;// 0 msgpo 1nodepo 2 joinpo
		public long srcId = 0;// 0 msgpo 1nodepo 2 joinpo
	}

	public static enum GroupMethod {
		NONE, COUNT, MAX, MIN, SUM, AVG, FIRST, LAST;
	}

	// 节点输出字段
	public static class NodeFieldPO extends FieldPO {
		private static final long serialVersionUID = 2872118846580820100L;
		public long NODE_ID;
		public String FIELD_CN_NAME;
		public int STATE;
		/**
		 * 单字段：field1； 计算表达式： a && 1>b；
		 * 
		 * 类：class: com.estorm.calc.Test: field1,field2； 复杂计算方法的实现代码，实现com.ery.estorm.INodeColumnCalc 接口，代码存CALC_JAVA_SOURCE_CODE字段
		 */
		public String CALC_EXPR;
		/**
		 * 输入映射，【宏变量名：来源类型：来源ID：来源字段】，格式如下： {a}：0：1 ： field1 来源类型：0消息，1节点输出字段，2关联表输出字段
		 */
		public String INPUT_FIELD_MAPPING;
		/**
		 * 汇总方法,NONE,COUNT,MAX,MIN,SUM,AVG.FIRST,LAST
		 */
		public String GROUP_METHOD;//
		public int IS_BUS_KEY;

		public GroupMethod calcMethod = GroupMethod.NONE;
		public int calcType = 0;// 计算类型 0:常量 1single 2:calc expr 3:class
		/**
		 * Boolean Integer Long Double Float String
		 */
		public Object constant = null;//
		public Expression[] expr = null;
		public NodeColumnCalc[] calcClass = null;
		// public InputFieldRel[] InputRel = null;// 输入字符名或者变量名
		public InputFieldRel[][] InputRels = null;// 输入字符名或者变量名,是业务主键的情况下多输入规则
		public String[] calcExprs = null;

	}

	public static class NodeInput implements Serializable {
		private static final long serialVersionUID = 1579438455498285011L;
		public long INPUT_ID;
		public int INPUT_TYPE;
		public long NODE_ID;
		public long INPUT_SRC_ID;
		public String FILTER_EXPR;
		public int MAX_PARALLELISM_NUM;
		public int MAX_PENDING;
		public int MSG_TYPE;
		public InOutPO inputPo;
		/**
		 * 输入分组字段,用于数据发射
		 */
		public List<String> groupFields;
		// 未初始化
		public int[] destGroupFileds;// 目标数据的来源查询主键
		public int index;
	}

	// 消息对象
	public static class MsgPO extends InOutPO {
		private static final long serialVersionUID = -5487723747212944561L;
		public long MSG_ID;// 消息流ID
		public String MSG_NAME;
		public String MSG_TAG_NAME;
		public String READ_OPS_PARAMS;
		public int MAX_PARALLELISM_NUM;

		public String getNodeName() {
			return MSG_NAME;
		}

		public long getNodeId() {
			return MSG_ID;
		}

		@Override
		public int CompareBase(InOutPO po) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void init() throws NodeParseException {
			if (this.inited)
				return;
			super.init();
			fieldNameRel = new HashMap<String, Integer>();
			int index = 0;
			for (index = 0; index < this.fields.size(); index++) {
				FieldPO f = this.fields.get(index);
				fieldNameRel.put(f.FIELD_NAME, index);
			}
		}
		// @Override
		// public InOutPO clone() {
		// // TODO Auto-generated method stub
		// return null;
		// }
	}

	// 消息字段
	public static class MsgFieldPO extends FieldPO {
		/**
		 * 
		 */
		private static final long serialVersionUID = -6994517719333421203L;
		public long MSG_ID;// 消息流ID
		public String FIELD_CN_NAME;
	}

	// 需要根据节点字段输入输出关系确定需要的字段来源并生成对应的结构体和数据
	public static class NodeJoinPO extends InOutPO {
		/**
		 * 
		 */
		private static final long serialVersionUID = 5632504350337454233L;
		public long JOIN_ID;
		public long NODE_ID;
		public long PAR_JOIN_ID;// 父关联表ID
		public String JOIN_FIELD;// 关联字段列表与PAR_JOIN_FIELD一一对应，属于JoinPO . BUS_PRIMARYS中的一组
		public String PAR_JOIN_FIELD;// 输入字段列表 如果PAR_JOIN_ID为0，标示自来来自节点，格式如下： INPUT_ID1.field1，INPUT_ID2.field2,
		// 如果PAR_JOIN_ID非0，标示上一个JOIN表的字段,格式如下：
		// field1，field2
		public InputFieldRel[] parJoinRel = null;

		public int useBusKeyIndex = -1;

		public JoinPO joinPO;
		public NodeJoinPO parJoinPO;

		public long getNodeId() {
			return JOIN_ID;
		}

		@Override
		public int CompareBase(InOutPO po) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void init() throws NodeParseException {
			if (this.inited)
				return;
			super.init();
			joinPO.init();
		}

		// @Override
		// public InOutPO clone() {
		// // TODO Auto-generated method stub
		// return null;
		// }
	}

	// 关联表定义，为了复用，需要单独线程加载和更新
	public static class JoinPO extends InOutPO {
		/**
		 * 
		 */
		private static final long serialVersionUID = 325531340423531677L;
		public long JOIN_ID;
		// long DATA_SOURCE_ID;// 数据源ID
		public String TABLE_NAME;// 表名或者表名列表，逗号分割
		public String TABLE_SQL;// 表名SQL，查询表名，优先级高于TABLE_NAME
		public String DATA_QUERY_SQL;// 查询SQL
		public int FLASH_TYPE;// 刷新类型： 1周期， 2事件
		public String FLASH_PARAM;// 周期：基准事件 间隔类型 间隔大小 事件：SQL事件,ZK监听，文件监听。（轮询时间框架参数配置）
		public String DATA_CHECK_RULE;// 数据验证规则
		public int INLOCAL_MEMORY;
		public String BUS_PRIMARYS;// 业务主键列表，对应关联查询索引
		public String JOIN_KEY_RULES;

		/**
		 * 数据加载状态,加载中则所有使用此关联数据的节点都装暂停处理消息 下一周期如果规则未变化，来源没变化则不在加载
		 */
		public LoadState loadState;
		public List<String> partions = new ArrayList<String>();
		public boolean needLoad = true;// 源与存储库相同不需要加载
		public Map<String, JoinPartition> partitionLoadInfo = null;// 完成加载后更新

		public DataSourcePO dataSourcePo = new DataSourcePO();

		public String[][] busKeyCols = null;
		public String[] busKeys = null;
		public int[][] busKeyColsIndex = null;
		public String[] busKeyRules = null;

		@Override
		public void init() throws NodeParseException {
			if (this.inited)
				return;
			try {
				if (this.dataSourcePo.DATA_SOURCE_TYPE >= 10 && this.dataSourcePo.DATA_SOURCE_TYPE < 20) {// 来源数据库
					boolean res = EStormConfigRead.buildJoinFields(this);
					if (res == false) {
						throw new NodeParseException("初始化关联表规则[" + this.JOIN_ID + "]，解析关联字段失败");
					}
				} else {// 非关系数据关联数据，Hbase FTP HDFS文件等,当前只支持关系数据库和Hbase
					String cols[] = this.DATA_QUERY_SQL.split(",");
					this.fields = new ArrayList<FieldPO>(cols.length);
					int index = 0;
					for (String col : cols) {
						JoinFieldPO fpo = new JoinFieldPO();
						String tmp[] = col.split(":");
						fpo.FIELD_NAME = tmp[0];
						if (tmp.length > 1)
							fpo.FIELD_DATA_TYPE = tmp[1];
						fpo.FIELD_ID = -1;
						fpo.ORDER_ID = index++;
						fields.add(fpo);
					}
				}
				super.init();
				busKeys = BUS_PRIMARYS.split(";");
				this.busKeyCols = new String[busKeys.length][];
				this.busKeyColsIndex = new int[busKeys.length][];
				for (int c = 0; c < busKeys.length; c++) {
					this.busKeyCols[c] = busKeys[c].split(",");
					this.busKeyColsIndex[c] = new int[this.busKeyCols[c].length];
					for (int i = 0; i < this.busKeyCols[c].length; i++) {
						Integer index = this.fieldNameRel.get(this.busKeyCols[c][i]);
						if (index == null) {
							throw new NodeParseException("初始化关联表规则[" + this.JOIN_ID + "]，解析关联字段失败");
						}
						this.busKeyColsIndex[c][i] = index;
					}
				}
				// 判断存储类型
				if (this.INLOCAL_MEMORY == 0) {
					if (this.JOIN_KEY_RULES != null) {
						busKeyRules = this.JOIN_KEY_RULES.split(";");
					}
					if (busKeyRules == null || busKeyRules.length != busKeys.length) {
						busKeyRules = new String[busKeys.length];
						for (int i = 0; i < busKeys.length; i++) {
							busKeyRules[i] = busKeys[i].replaceAll(",", "+','+");
						}
					}
				}
			} catch (Exception e) {
				throw new NodeParseException(e);
			}
		}

		public long getNodeId() {
			return JOIN_ID;
		}

		@Override
		public int CompareBase(InOutPO po) {
			// TODO Auto-generated method stub
			// joinPO.joinPO.modifyType = 1,2 新增, 修改
			return 0;
		}

		public static enum LoadState {
			// Normal,
			Initing, Loading, Locked, Success, Failed, Retry, Assign
		}

		public static class JoinPartition implements Serializable {
			private static final long serialVersionUID = -9078227718439201303L;
			public String partion;// 分区
			public LoadState state;// 状态
			public int rnum;// 记录数
			public long st;// 开始时间
			public long joinId;
			public Map<String, ServerLoadState> loadServer = new HashMap<String, ServerLoadState>();// hostName

			public static class ServerLoadState {
				public LoadState state;// 状态
				public int rnum;// 记录数
				public long st;// 开始时间
				public long et;
				public String msg;

				public ServerLoadState(LoadState state) {
					this.state = state;
				}
			}

			@Override
			public String toString() {
				return "joinId:" + joinId + " partion:" + partion + " startTime:" + (new Date(st)).toLocaleString() + " state:" + state
						+ " rowNum:" + rnum;
			}

			@Override
			public boolean equals(Object anObject) {
				if (anObject instanceof JoinPartition) {
					JoinPartition jpt = (JoinPartition) anObject;
					if (jpt.joinId == this.joinId && jpt.partion.equals(this.partion))
						return true;
				}
				return false;
			}

			public void deleteZk(ZooKeeperWatcher watcher, String priPath) throws KeeperException, IOException {
				String subPath = ZKUtil.joinZNode(priPath, "p_" + this.partion.hashCode());
				ZKUtil.deleteNode(watcher, subPath);
			}

			public void assginToServer(ZooKeeperWatcher watcher, String serverPath) throws IOException, KeeperException {
				byte[] data = EStormConstant.Serialize(this);
				ZKUtil.createSetData(watcher, serverPath, data);
			}

			public void updateToZk(ZooKeeperWatcher watcher, String priPath) throws IOException, KeeperException {
				String path = ZKUtil.joinZNode(priPath, "p_" + this.partion.hashCode());
				byte[] data = EStormConstant.Serialize(this);
				ZKUtil.createSetData(watcher, path, data);
			}
		}

		public void updateToZk(ZooKeeperWatcher watcher) throws IOException, KeeperException {
			String path = ZKUtil.joinZNode(watcher.estormJoinZNode, "join_" + this.JOIN_ID);
			byte[] data = EStormConstant.Serialize(this);
			ZKUtil.createSetData(watcher, path, data);
		}
	}

	// 关联字段
	public static class JoinFieldPO extends FieldPO {

		/**
		 * 
		 */
		private static final long serialVersionUID = 8293889715193903305L;
	}

	public static class DataStorePO extends InOutPO {
		/**
		 * 
		 */
		private static final long serialVersionUID = -5634828224043762723L;
		public long STORE_ID;
		public long NODE_ID;
		// long DATA_SOURCE_ID;// 数据源ID
		public String TARGET_NAME;// 目标名称存储源不一样，代表不同含义，可以是文件名，表名
		public String STORE_FIELD_MAPPING;// 存储字段映射： srcField:destField，srcField:destField，srcField:destField
		public String TARGET_PRIMARY_RULE;// 目标主键规则： HBASE：rowkey计算表达式 ORACLE,MYSQL：字段列表
		public int AUTO_REAL_FLUSH;// '是否自动刷新（自动写入到外部存储）',
		public long FLUSH_BUFFER_SIZE;// '刷新缓存大小，用于批量刷新，提高性能',
		public long FLUSH_TIME_INTERVAL;// '刷新时间间隔，单位 ms',
		public String STORE_EXPR;

		public short[] termIndex = null;
		public String[] colMacs = null;

		public int HashCode;
		public DataSourcePO dataSourcePo = new DataSourcePO();

		public String getNodeName() {
			return NODE_ID + "";
		}

		public long getNodeId() {
			return STORE_ID;
		}

		@Override
		public int CompareBase(InOutPO po) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void init() throws NodeParseException {
			if (this.inited)
				return;
			super.init();
			HashCode = (dataSourcePo.DATA_SOURCE_ID + "：" + " TAG:" + TARGET_NAME + ":" + STORE_FIELD_MAPPING + ":"
					+ dataSourcePo.DATA_SOURCE_USER + "/" + dataSourcePo.DATA_SOURCE_PASS + "@" + dataSourcePo.DATA_SOURCE_URL + ":" + STORE_EXPR)
					.hashCode();
		}
		// @Override
		// public InOutPO clone() {
		// // TODO Auto-generated method stub
		// return null;
		// }
	}

	// 输出存储字段
	public static class DataFieldPO extends FieldPO {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1182019776789768001L;
		public String SRC_FIELD;// 来源字段
	}

	// 订阅
	public static class PushPO extends InOutPO {
		/**
		 * 
		 */
		private static final long serialVersionUID = 984331953803727122L;
		public long PUSH_ID;
		public long NODE_ID;
		public int PUSH_TYPE;// 订阅类型 订阅类型// 0，WS // 1，SOCKET // 2，HTTP // 3，ZK // 4, kafka
		public String PUSH_URL;// 订阅地址
		public String PUSH_USER;
		public String PUSH_PASS;
		public String PUSH_EXPR;// 条件表达式
		public String PUSH_PARAM;// push参数，根据接口类型不一样，参数规则不一样
		public int pushMsgType = 0;

		public int SUPPORT_BATCH;
		public long PUSH_BUFFER_SIZE;
		public long PUSH_TIME_INTERVAL;
		public int HashCode;

		public short[] termIndex = null;
		public String[] colMacs = null;

		public String getNodeName() {
			return PUSH_ID + "";
		}

		public long getNodeId() {
			return PUSH_ID;
		}

		@Override
		public int CompareBase(InOutPO po) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public void init() throws NodeParseException {
			if (this.inited)
				return;
			super.init();
			HashCode = (PUSH_TYPE + "：" + PUSH_USER + "/" + PUSH_PASS + "@" + PUSH_URL + ":" + PUSH_EXPR + ":" + PUSH_PARAM).hashCode();
		}
		// @Override
		// public InOutPO clone() {
		// // TODO Auto-generated method stub
		// return null;
		// }
	}

	public static class DataSourcePO {
		public int DATA_SOURCE_ID;
		/**
		 * 数据源类型= 11,ORACLE 12,MYSQL 23,HBASE 34,FTP 45,HDFS
		 */
		public int DATA_SOURCE_TYPE;
		public String DATA_SOURCE_NAME;
		public String DATA_SOURCE_URL;
		public String DATA_SOURCE_USER;
		public String DATA_SOURCE_PASS;
		public String DATA_SOURCE_DESC;
		public String DATA_SOURCE_CFG;
	}
}
