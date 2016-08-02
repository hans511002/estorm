package com.ery.estorm.log;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ery.estorm.client.node.StormNodeConfig;
import com.ery.estorm.config.ConfigListenThread.ConfigInfo;
import com.ery.estorm.config.Configuration;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.log.LogMag.NodeSummaryTracker;
import com.ery.base.support.jdbc.DataAccess;

//节点日志
public class NodeLog {
	private static final Log LOG = LogFactory.getLog(NodeLog.class.getName());
	static DataAccess access = new DataAccess();
	static Connection con = null;// 获取数据库连接
	public static String configDbType;
	public static boolean inited = false;
	public static Configuration conf = null;

	public static void init(Configuration conf) {
		if (inited)
			return;
		NodeLog.conf = conf;
		ConfigInfo configInfo = new ConfigInfo(conf);
		if (configInfo.drive.indexOf("oracle") > 0) {
			configDbType = "oracle";
			NodeMsgLogPo.insertSql = "insert into ST_NODE_RUN_LOG(LOG_ID,NODE_ID, SRC_TYPE, SRC_ID, SRC_DATA"
					+ ", DEST_BEFORE_DATA, DEST_AFTER_DATA, PROCESS_TIME,USE_TIME, HOST_NAME,ERROR)"
					+ " values(SEQ_NODE_RUN_LOG_ID.nextval,?,?,?,?,?,?,?,?,?,?)";
			PushLogPo.insertSql = "insert into ST_NODE_PUSH_LOG(PUSH_ID, NODE_ID, PUSH_TIME, RESULT_DATA, ERROR_INFO, USE_TIME, HOST_NAME)"
					+ "values(SEQ_NODE_PUSH_LOG_ID.nextval,?,?,?,?,?,?,?)";
		} else if (configInfo.drive.indexOf("mysql") > 0) {// 自增字段
			configDbType = "mysql";
			NodeMsgLogPo.insertSql = "insert into ST_NODE_RUN_LOG(NODE_ID, SRC_TYPE, SRC_ID, SRC_DATA"
					+ ", DEST_BEFORE_DATA, DEST_AFTER_DATA, PROCESS_TIME,USE_TIME, HOST_NAME,ERROR) values(?,?,?,?,?,?,?,?,?,?)";
			PushLogPo.insertSql = "insert into ST_NODE_PUSH_LOG(PUSH_ID, NODE_ID, PUSH_TIME, RESULT_DATA, ERROR_INFO, USE_TIME, HOST_NAME)"
					+ "values(?,?,?,?,?,?,?)";
		} else {// 手动序列
			configDbType = "other";
			NodeMsgLogPo.insertSql = "insert into ST_NODE_RUN_LOG( LOG_ID, NODE_ID, SRC_TYPE, SRC_ID, SRC_DATA"
					+ ", DEST_BEFORE_DATA, DEST_AFTER_DATA, PROCESS_TIME,USE_TIME, HOST_NAME,ERROR)"
					+ " values(?,?,?,?,?,?,?,?,?,?,?)";
			PushLogPo.insertSql = "insert into ST_NODE_PUSH_LOG(LOG_ID, PUSH_ID, NODE_ID, PUSH_TIME, RESULT_DATA, ERROR_INFO, USE_TIME, HOST_NAME)"
					+ "values(?,?,?,?,?,?,?,?)";
		}
		NodeSummaryLogPo.insertSql = "insert into ST_NODE_LOG(LOG_ID,NODE_ID,USE_TIME,START_TIME,STOP_TIME"
				+ ",INPUT_TOTAL_NUM,OUTPUT_TOTAL_NUM,PUSH_NUM,INPUT_SRC_TOTAL_NUM,PUSH_OUT_TOTAL_NUM,HOST_NAME,ERROR)"
				+ "values(?,?,?,?,?,?,?,?,?,?,?,?)";
		NodeSummaryLogPo.modSql = "update ST_NODE_LOG set USE_TIME=? ,START_TIME=?,STOP_TIME=?"
				+ ",INPUT_TOTAL_NUM=?,OUTPUT_TOTAL_NUM=?,PUSH_NUM=?,INPUT_SRC_TOTAL_NUM=?,PUSH_OUT_TOTAL_NUM=?,HOST_NAME=?,ERROR=? "
				+ "where LOG_ID=?";
		inited = true;
	}

	static void open(ConfigInfo configInfo) throws Exception {
		if (con == null) {
			con = DriverManager.getConnection(configInfo.url, configInfo.user, configInfo.password);
			access.setConnection(con);
			access.beginTransaction();
		}
	}

	static void close() {
		try {
			if (con != null)
				con.close();
		} catch (SQLException e1) {
		}
		con = null;
	}

	// 根据类型写入数据库日志表
	public synchronized static void writeLog(List<LogPo> logs, Class<?> logClass) {
		try {
			ConfigInfo newConfig = ConfigInfo.pasreConofig(StormNodeConfig.confDbTracker.getString());
			if (!LogMag.configInfo.equals(newConfig) && newConfig != null) {
				close();
				LogMag.configInfo = newConfig;
			}
			open(LogMag.configInfo);
			for (LogPo log : logs) {
				log.writeLog(access);
			}
			access.commit();
			HashMap<Long, NodeSummaryLogPo> updateList = SummaryNodeLogPo(logs, logClass);
			if (updateList.size() > 0 && StormNodeConfig.isNode) {
				String path = StormNodeConfig.zkw.estormLogZNode + "/" + StormNodeConfig.hostName;
				NodeSummaryTracker.updateLog(StormNodeConfig.zkw, path, updateList);
			}
		} catch (Exception e) {
			LOG.error("写日志报错，日志类型：" + logClass, e);
		} finally {
			close();
		}
	}

	public static HashMap<Long, NodeSummaryLogPo> SummaryNodeLogPo(List<LogPo> logs, Class<?> logClass) {
		HashMap<Long, NodeSummaryLogPo> updateList = new HashMap<Long, NodeSummaryLogPo>();
		if (logClass.equals(NodeMsgLogPo.class)) {
			for (LogPo log : logs) {
				NodeSummaryLogPo nlogPo = updateList.get(log.NODE_ID);
				if (nlogPo == null) {
					nlogPo = new NodeSummaryLogPo();
					updateList.put(log.NODE_ID, nlogPo);
					nlogPo.NODE_ID = log.NODE_ID;
				}
				NodeMsgLogPo mlog = (NodeMsgLogPo) log;
				if (nlogPo.START_TIME == 0)
					nlogPo.START_TIME = mlog.PROCESS_TIME;
				if (nlogPo.STOP_TIME < mlog.PROCESS_TIME)
					nlogPo.STOP_TIME = mlog.PROCESS_TIME;
				nlogPo.USE_TIME += mlog.USE_TIME;
				nlogPo.INPUT_TOTAL_NUM++;
				nlogPo.STOP_TIME = mlog.PROCESS_TIME;
				// 需要在提交消息时增量
				nlogPo.OUTPUT_TOTAL_NUM += mlog.OUTPUT_NUM;
				String key = mlog.SRC_TYPE + "_" + mlog.SRC_ID;
				Long l = nlogPo.INPUT_SRC_TOTAL_NUM.get(mlog.SRC_TYPE + "_" + mlog.SRC_ID);
				if (l == null) {
					nlogPo.INPUT_SRC_TOTAL_NUM.put(key, 1l);
				} else {
					nlogPo.INPUT_SRC_TOTAL_NUM.put(key, l + 1);
				}
				l = nlogPo.HOST_NAME_SUM.get(StormNodeConfig.hostName);
				if (l != null) {
					nlogPo.HOST_NAME_SUM.put(StormNodeConfig.hostName, l + 1);
				} else {
					nlogPo.HOST_NAME_SUM.put(StormNodeConfig.hostName, 1l);
				}
			}
		} else if (logClass.equals(PushLogPo.class)) {
			for (LogPo log : logs) {
				NodeSummaryLogPo nlogPo = updateList.get(log.NODE_ID);
				if (nlogPo == null) {
					nlogPo = new NodeSummaryLogPo();
					updateList.put(log.NODE_ID, nlogPo);
					nlogPo.NODE_ID = log.NODE_ID;
				}
				PushLogPo plog = (PushLogPo) log;
				nlogPo.PUSH_NUM++;
				nlogPo.USE_TIME += plog.USE_TIME;
				Long l = nlogPo.PUSH_OUT_TOTAL_NUM.get(plog.PUSH_ID);
				if (l == null) {
					nlogPo.PUSH_OUT_TOTAL_NUM.put(plog.PUSH_ID, 1l);
				} else {
					nlogPo.PUSH_OUT_TOTAL_NUM.put(plog.PUSH_ID, l + 1);
				}
			}
		}
		return updateList;
	}

	public abstract static class LogPo implements Serializable {
		private static final long serialVersionUID = 6900911644385749987L;
		public long LOG_ID;
		public long NODE_ID;
		public long USE_TIME;// 用时

		public abstract void writeLog(DataAccess access) throws SQLException;

	}

	public static class NodeSummaryLogPo extends LogPo {
		private static final long serialVersionUID = -6773503249717249901L;
		public long START_TIME;// 启动时间
		public long STOP_TIME;// 停止日志

		public long INPUT_TOTAL_NUM;// 输入记录数
		public long OUTPUT_TOTAL_NUM;// 输出记录总数
		public long PUSH_NUM;// 对外Push记录总次数
		// 统计每个输入源的记录数，JSON
		public Map<String, Long> INPUT_SRC_TOTAL_NUM = new HashMap<String, Long>();
		// 对外PUSH每个源记录数
		public Map<Long, Long> PUSH_OUT_TOTAL_NUM = new HashMap<Long, Long>();
		public Map<String, Long> HOST_NAME_SUM = new HashMap<String, Long>();
		public String ERROR = null;
		static String insertSql = "LOG_ID,NODE_ID,USE_TIME,START_TIME,STOP_TIME"
				+ ",INPUT_TOTAL_NUM,OUTPUT_TOTAL_NUM,PUSH_NUM,INPUT_SRC_TOTAL_NUM,PUSH_OUT_TOTAL_NUM,HOST_NAME";

		static String modSql = "update   ST_NODE_LOG set USE_TIME=? ,START_TIME=?,STOP_TIME=?"
				+ ",INPUT_TOTAL_NUM=?,OUTPUT_TOTAL_NUM=?,PUSH_NUM=?,INPUT_SRC_TOTAL_NUM=?,PUSH_OUT_TOTAL_NUM=?,HOST_NAME=? "
				+ "where LOG_ID=?";

		public void writeLog(DataAccess access) throws SQLException {
			if (LOG.isDebugEnabled()) {
				LOG.debug("节点统计日志:" + this);
			}
			String _INPUT_SRC_TOTAL_NUM = null;
			String _PUSH_OUT_TOTAL_NUM = null;
			String _HOST_NAME_SUM = null;
			try {
				_INPUT_SRC_TOTAL_NUM = EStormConstant.objectMapper.writeValueAsString(INPUT_SRC_TOTAL_NUM);
				_PUSH_OUT_TOTAL_NUM = EStormConstant.objectMapper.writeValueAsString(PUSH_OUT_TOTAL_NUM);
				_HOST_NAME_SUM = EStormConstant.objectMapper.writeValueAsString(HOST_NAME_SUM);
			} catch (Exception e) {
				LOG.debug("节点统计日志JSON化异常:", e);
			}
			access.execUpdate(modSql, USE_TIME, new Timestamp(START_TIME), new Timestamp(
					STOP_TIME == START_TIME ? System.currentTimeMillis() : STOP_TIME), INPUT_TOTAL_NUM,
					OUTPUT_TOTAL_NUM, PUSH_NUM, _INPUT_SRC_TOTAL_NUM, _PUSH_OUT_TOTAL_NUM, _HOST_NAME_SUM, ERROR,
					LOG_ID);
		}

		public void insertLog(DataAccess access) {
			LOG.info("节点统计日志:" + this);
			if (LOG.isDebugEnabled()) {
				LOG.debug("节点统计日志:" + this);
			}
			access.execUpdate(insertSql, LOG_ID, NODE_ID, USE_TIME, new Timestamp(START_TIME),
					new Timestamp(STOP_TIME), INPUT_TOTAL_NUM, OUTPUT_TOTAL_NUM, PUSH_NUM, "", "", "", ERROR);
		}

		public void add(NodeSummaryLogPo log) {
			if (this.LOG_ID == 0)
				this.LOG_ID = log.LOG_ID;
			if (this.NODE_ID == 0)
				this.NODE_ID = log.LOG_ID;
			USE_TIME += log.USE_TIME;
			if (START_TIME > log.START_TIME) {
				START_TIME = log.START_TIME;// 启动时间
			}
			if (STOP_TIME < log.STOP_TIME) {
				STOP_TIME = log.STOP_TIME;// 停止日志
			}
			if (ERROR == null) {
				ERROR = log.ERROR;
			} else {
				if (log.ERROR != null) {
					ERROR = log.ERROR + "\n" + ERROR;
					if (ERROR.length() > 65535) {
						ERROR = ERROR.substring(0, 1 << 12);
					}
				}
			}
			INPUT_TOTAL_NUM += log.INPUT_TOTAL_NUM;
			OUTPUT_TOTAL_NUM += log.OUTPUT_TOTAL_NUM;
			PUSH_NUM += log.PUSH_NUM;
			if (this.INPUT_SRC_TOTAL_NUM.size() == 0) {
				INPUT_SRC_TOTAL_NUM.putAll(log.INPUT_SRC_TOTAL_NUM);
			} else {
				for (String mid : log.INPUT_SRC_TOTAL_NUM.keySet()) {
					if (INPUT_SRC_TOTAL_NUM.containsKey(mid)) {
						INPUT_SRC_TOTAL_NUM.put(mid, log.INPUT_SRC_TOTAL_NUM.get(mid) + INPUT_SRC_TOTAL_NUM.get(mid));
					} else {
						INPUT_SRC_TOTAL_NUM.put(mid, log.INPUT_SRC_TOTAL_NUM.get(mid));
					}
				}
			}
			if (this.PUSH_OUT_TOTAL_NUM.size() == 0) {
				PUSH_OUT_TOTAL_NUM.putAll(log.PUSH_OUT_TOTAL_NUM);
			} else {
				for (Long mid : log.PUSH_OUT_TOTAL_NUM.keySet()) {
					if (PUSH_OUT_TOTAL_NUM.containsKey(mid)) {
						PUSH_OUT_TOTAL_NUM.put(mid, log.PUSH_OUT_TOTAL_NUM.get(mid) + PUSH_OUT_TOTAL_NUM.get(mid));
					} else {
						PUSH_OUT_TOTAL_NUM.put(mid, log.PUSH_OUT_TOTAL_NUM.get(mid));
					}
				}
			}
			if (this.HOST_NAME_SUM.size() == 0) {
				HOST_NAME_SUM.putAll(log.HOST_NAME_SUM);
			} else {
				for (String mid : log.HOST_NAME_SUM.keySet()) {
					if (HOST_NAME_SUM.containsKey(mid)) {
						HOST_NAME_SUM.put(mid, log.HOST_NAME_SUM.get(mid) + HOST_NAME_SUM.get(mid));
					} else {
						HOST_NAME_SUM.put(mid, log.HOST_NAME_SUM.get(mid));
					}
				}
			}

		}
	}

	public static class NodeMsgLogPo extends LogPo {
		private static final long serialVersionUID = -2288942587789371586L;
		public long SRC_ID;// 来源ID
		public int SRC_TYPE;// 来源类型 0消息 1节点
		public String SRC_DATA;// JSON
		public String DEST_BEFORE_DATA;// 目标处理前数据
		public String DEST_AFTER_DATA;// 目标处理后数据
		public long PROCESS_TIME;
		public int OUTPUT_NUM;// 消息处理完后输出的消息数
		public String HOST_NAME;
		public String ERROR = null;

		static String insertSql = "";

		public void writeLog(DataAccess access) throws SQLException {
			if (LOG.isDebugEnabled())
				LOG.debug("消息日志:" + this);
			if (configDbType.equals("oracle")) {
				access.execUpdate(insertSql, NODE_ID, SRC_TYPE, SRC_ID, SRC_DATA, DEST_BEFORE_DATA, DEST_AFTER_DATA,
						new Timestamp(PROCESS_TIME), USE_TIME, HOST_NAME, ERROR);
			} else if (configDbType.equals("mysql")) {
				access.execUpdate(insertSql, NODE_ID, SRC_TYPE, SRC_ID, SRC_DATA, DEST_BEFORE_DATA, DEST_AFTER_DATA,
						new Timestamp(PROCESS_TIME), USE_TIME, HOST_NAME, ERROR);
			} else if (configDbType.equals("other")) {
				access.execUpdate(insertSql, LOG_ID, NODE_ID, SRC_TYPE, SRC_ID, SRC_DATA, DEST_BEFORE_DATA,
						DEST_AFTER_DATA, new Timestamp(PROCESS_TIME), USE_TIME, HOST_NAME, ERROR);
			}
		}

		public void reset() {
			NODE_ID = 0;
			SRC_ID = 0;
			SRC_TYPE = 0;
			SRC_DATA = null;
			DEST_BEFORE_DATA = null;
			DEST_AFTER_DATA = null;
			PROCESS_TIME = 0;
			USE_TIME = 0;
		}

	}

	public static class PushLogPo extends LogPo {
		public long PUSH_ID;
		public long PUSH_TIME;
		public String RESULT_DATA;
		public String ERROR_INFO;// 如果PUSH不成功，记录异常日志
		public String HOST_NAME;
		static String insertSql;

		public void writeLog(DataAccess access) throws SQLException {
			LOG.info("推送日志:" + this);
			// if (LOG.isDebugEnabled()) {
			// LOG.debug("推送日志:" + this);
			// }
			// if (configDbType.equals("oracle")) {
			// access.execUpdate(insertSql, PUSH_ID, NODE_ID, new
			// Timestamp(PUSH_TIME), RESULT_DATA, ERROR_INFO, USE_TIME,
			// HOST_NAME);
			// } else if (configDbType.equals("mysql")) {
			// access.execUpdate(insertSql, PUSH_ID, NODE_ID, new
			// Timestamp(PUSH_TIME), RESULT_DATA, ERROR_INFO, USE_TIME,
			// HOST_NAME);
			// } else if (configDbType.equals("other")) {
			// access.execUpdate(insertSql, LOG_ID, PUSH_ID, NODE_ID, new
			// Timestamp(PUSH_TIME), RESULT_DATA, ERROR_INFO, USE_TIME,
			// HOST_NAME);
			// }
		}

		public void reset() {
			NODE_ID = 0;
			PUSH_ID = 0;
			PUSH_TIME = 0;
			RESULT_DATA = null;
			ERROR_INFO = null;
			USE_TIME = 0;
		}

	}
}
