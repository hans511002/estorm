package com.ery.estorm.config;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ery.estorm.daemon.topology.NodeInfo.DataFieldPO;
import com.ery.estorm.daemon.topology.NodeInfo.DataSourcePO;
import com.ery.estorm.daemon.topology.NodeInfo.DataStorePO;
import com.ery.estorm.daemon.topology.NodeInfo.FieldPO;
import com.ery.estorm.daemon.topology.NodeInfo.JoinFieldPO;
import com.ery.estorm.daemon.topology.NodeInfo.JoinPO;
import com.ery.estorm.daemon.topology.NodeInfo.MsgFieldPO;
import com.ery.estorm.daemon.topology.NodeInfo.MsgPO;
import com.ery.estorm.daemon.topology.NodeInfo.NodeFieldPO;
import com.ery.estorm.daemon.topology.NodeInfo.NodeInput;
import com.ery.estorm.daemon.topology.NodeInfo.NodeJoinPO;
import com.ery.estorm.daemon.topology.NodeInfo.NodePO;
import com.ery.estorm.daemon.topology.NodeInfo.PushPO;
import com.ery.estorm.log.NodeLog;
import com.ery.base.support.jdbc.DataAccess;
import com.ery.base.support.jdbc.DataSourceImpl;
import com.ery.base.support.sys.DataSourceManager;
import com.ery.base.support.utils.MapUtils;

public class EStormConfigRead {
	private static long readCount = 0;
	private static final Log LOG = LogFactory.getLog(EStormConfigRead.class);

	/**
	 * read node config
	 * 
	 * @param con
	 * @return
	 * @throws SQLException
	 */
	public static List<NodePO> readBoltNodes(DataAccess access, String configTime) throws SQLException {
		List<NodePO> res = new ArrayList<NodePO>();
		String sql = "select NODE_ID ,NODE_NAME,NODE_DESC,MAX_PARALLELISM_NUM,MAX_PENDING"
				+ ",IS_DEBUG,LOG_LEVEL,OUTPUT_FILTER,DATE_FORMAT(MODIFY_TIME,'%Y-%m-%d %H:%i:%s') MODIFY_TIME,MAX_BUFFER_SIZE"
				+ ",NODE_PARAMS from ST_NODE where STATE=1 and MODIFY_TIME> STR_TO_DATE(?,'%Y-%m-%d %H:%i:%s')";
		if (readCount++ % 1000 == 0) {
			LOG.info("扫描变更节点[configTime=" + configTime + "]，SQL=" + sql);
			if (readCount > 1000)
				readCount = 0;
		}

		Map<String, Object>[] nodes = access.queryForArrayMap(sql, configTime);
		String nodeIds = "";
		for (Map<String, Object> node : nodes) {
			NodePO np = new NodePO();
			np.NODE_ID = MapUtils.getLongValue(node, "NODE_ID");
			np.NODE_NAME = MapUtils.getString(node, "NODE_NAME", "");
			np.NODE_DESC = MapUtils.getString(node, "NODE_DESC", "");
			np.MAX_PARALLELISM_NUM = MapUtils.getIntValue(node, "MAX_PARALLELISM_NUM", 0);
			np.MAX_PENDING = MapUtils.getIntValue(node, "MAX_PENDING", 0);
			np.MAX_PENDING = np.MAX_PENDING > 0 ? np.MAX_PENDING : np.MAX_PARALLELISM_NUM;
			np.IS_DEBUG = MapUtils.getIntValue(node, "IS_DEBUG", 0);
			np.LOG_LEVEL = MapUtils.getIntValue(node, "LOG_LEVEL", 0);
			np.MODIFY_TIME = node.get("MODIFY_TIME").toString();
			np.maxBufferSize = MapUtils.getIntValue(node, "MAX_BUFFER_SIZE", 1000);
			np.NODE_PARAMS = MapUtils.getString(node, "NODE_PARAMS", "");
			// np.IS_CALC = MapUtils.getIntValue(node, "IS_CALC", 0);
			res.add(np);
			if (nodeIds.length() > 0)
				nodeIds += ",";
			nodeIds += np.NODE_ID;
		}
		List<NodeFieldPO> ndFields = readBoltNodeFields(access, nodeIds);
		List<NodePO> nullField = new ArrayList<NodePO>();
		for (NodePO node : res) {
			boolean find = false;
			for (NodeFieldPO field : ndFields) {
				if (find == false && field.NODE_ID != node.NODE_ID)
					continue;
				else if (find == true && field.NODE_ID != node.NODE_ID)
					break;
				if (node.fields == null)
					node.fields = new ArrayList<FieldPO>();
				node.fields.add(field);
				find = true;
			}
			if (node.fields == null || node.fields.size() == 0) {
				LOG.error("消息配置错误，消息[" + node.NODE_ID + "]无字段");
				nullField.add(node);
			}
		}
		res.removeAll(nullField);
		return res;
	}

	// 输出字段信息
	public static List<NodeFieldPO> readBoltNodeFields(DataAccess access, String nodeIds) throws SQLException {
		List<NodeFieldPO> res = new ArrayList<NodeFieldPO>();
		if (nodeIds == null || nodeIds.equals(""))
			return res;
		String sql = " SELECT t.FIELD_ID,t.NODE_ID,t.FIELD_NAME,t.FIELD_CN_NAME,t.FIELD_DATA_TYPE" +
				",t.CALC_EXPR,t.INPUT_FIELD_MAPPING,t.GROUP_METHOD,IS_BUS_KEY FROM ST_NODE_OUTPUT_FIELDS t " +
				" WHERE  STATE=1 and t.NODE_ID IN (" + nodeIds + ")  ORDER BY t.NODE_ID,t.ORDER_ID,t.FIELD_ID ";
		if (readCount % 1000 == 1) {
			LOG.info("读取节点输出字段[nodeIds=" + nodeIds + "]，SQL=" + sql);
		}
		Map<String, Object>[] fileds = access.queryForArrayMap(sql);
		for (Map<String, Object> filed : fileds) {
			NodeFieldPO po = new NodeFieldPO();
			po.FIELD_ID = MapUtils.getLongValue(filed, "FIELD_ID");
			po.NODE_ID = MapUtils.getLongValue(filed, "NODE_ID");
			po.FIELD_NAME = MapUtils.getString(filed, "FIELD_NAME", "");
			po.FIELD_CN_NAME = MapUtils.getString(filed, "FIELD_CN_NAME", "");
			po.FIELD_DATA_TYPE = MapUtils.getString(filed, "FIELD_DATA_TYPE", "");
			po.CALC_EXPR = MapUtils.getString(filed, "CALC_EXPR", "");
			po.INPUT_FIELD_MAPPING = MapUtils.getString(filed, "INPUT_FIELD_MAPPING", "");
			po.GROUP_METHOD = MapUtils.getString(filed, "GROUP_METHOD", "");
			po.IS_BUS_KEY = MapUtils.getIntValue(filed, "IS_BUS_KEY", 0);
			res.add(po);
		}
		return res;
	}

	// 节点输入信息
	public static List<NodeInput> readNodeInputs(DataAccess access, String nodeIds) throws SQLException {
		List<NodeInput> res = new ArrayList<NodeInput>();
		if (nodeIds == null || nodeIds.equals(""))
			return res;
		String sql = " SELECT t.INPUT_ID,t.INPUT_TYPE,t.NODE_ID,t.INPUT_SRC_ID,t.FILTER_EXPR" +
				",t.MAX_PARALLELISM_NUM,t.MAX_PENDING,t.MSG_TYPE FROM ST_NODE_INPUT t where t.NODE_ID in (" + nodeIds +
				") order by t.NODE_ID,t.INPUT_ID";
		if (readCount % 1000 == 1) {
			LOG.info("读取节点输入配置[nodeIds=" + nodeIds + "]，SQL=" + sql);
		}
		Map<String, Object>[] fileds = access.queryForArrayMap(sql);
		for (Map<String, Object> filed : fileds) {
			NodeInput po = new NodeInput();
			po.INPUT_ID = MapUtils.getLongValue(filed, "INPUT_ID");
			po.INPUT_TYPE = MapUtils.getIntValue(filed, "INPUT_TYPE");
			po.NODE_ID = MapUtils.getLongValue(filed, "NODE_ID");
			po.INPUT_SRC_ID = MapUtils.getLongValue(filed, "INPUT_SRC_ID");
			po.FILTER_EXPR = MapUtils.getString(filed, "FILTER_EXPR", "");
			po.MAX_PARALLELISM_NUM = MapUtils.getIntValue(filed, "MAX_PARALLELISM_NUM");
			po.MAX_PENDING = MapUtils.getIntValue(filed, "MAX_PENDING");
			po.MSG_TYPE = MapUtils.getIntValue(filed, "MSG_TYPE");
			po.MAX_PENDING = po.MAX_PENDING > 0 ? po.MAX_PENDING : po.MAX_PARALLELISM_NUM;
			res.add(po);
		}
		return res;
	}

	// 读取消息配置
	public static List<MsgPO> readMsgNodes(DataAccess access, String msgIds) throws SQLException {
		List<MsgPO> res = new ArrayList<MsgPO>();
		if (msgIds == null || msgIds.equals(""))
			return res;
		String sql = "  SELECT  MSG_ID,MSG_NAME,MSG_TAG_NAME,READ_OPS_PARAMS FROM ST_MSG_STREAM WHERE MSG_ID IN (" +
				msgIds + ") ";
		if (readCount % 1000 == 1) {
			LOG.info("读取消息信息[msgIds=" + msgIds + "]，SQL=" + sql);
		}
		Map<String, Object>[] fileds = access.queryForArrayMap(sql);
		for (Map<String, Object> filed : fileds) {
			MsgPO po = new MsgPO();
			po.MSG_ID = MapUtils.getLongValue(filed, "MSG_ID");
			po.MSG_NAME = MapUtils.getString(filed, "MSG_NAME", "");
			po.MSG_TAG_NAME = MapUtils.getString(filed, "MSG_TAG_NAME", "");
			po.READ_OPS_PARAMS = MapUtils.getString(filed, "READ_OPS_PARAMS", "");
			res.add(po);
		}
		List<MsgFieldPO> msgFields = readMsgFields(access, msgIds);
		List<MsgPO> nullField = new ArrayList<MsgPO>();
		for (MsgPO msg : res) {
			boolean find = false;
			for (MsgFieldPO field : msgFields) {
				if (find == false && field.MSG_ID != msg.MSG_ID)
					continue;
				else if (find == true && field.MSG_ID != msg.MSG_ID)
					break;
				if (msg.fields == null)
					msg.fields = new ArrayList<FieldPO>();
				msg.fields.add(field);
				find = true;
			}
			if (msg.fields == null || msg.fields.size() == 0) {
				LOG.error("消息配置错误，消息[" + msg.MSG_ID + "]无字段");
				nullField.add(msg);
			}
		}
		res.removeAll(nullField);
		return res;
	}

	// 读取消息字段
	public static List<MsgFieldPO> readMsgFields(DataAccess access, String msgIds) throws SQLException {
		List<MsgFieldPO> res = new ArrayList<MsgFieldPO>();
		if (msgIds == null || msgIds.equals(""))
			return res;
		String sql = "SELECT FIELD_ID,MSG_ID,FIELD_NAME,FIELD_CN_NAME,FIELD_DATA_TYPE,ORDER_ID " +
				" FROM ST_MSG_FIELDS WHERE MSG_ID IN(" + msgIds + ") ORDER BY MSG_ID,order_id,FIELD_ID";
		if (readCount % 1000 == 1) {
			LOG.info("读取消息字段信息[msgIds=" + msgIds + "]，SQL=" + sql);
		}
		Map<String, Object>[] fileds = access.queryForArrayMap(sql);
		for (Map<String, Object> filed : fileds) {
			MsgFieldPO po = new MsgFieldPO();
			po.FIELD_ID = MapUtils.getLongValue(filed, "FIELD_ID");
			po.MSG_ID = MapUtils.getLongValue(filed, "MSG_ID");
			po.FIELD_NAME = MapUtils.getString(filed, "FIELD_NAME", "");
			po.FIELD_CN_NAME = MapUtils.getString(filed, "FIELD_CN_NAME", "");
			po.FIELD_DATA_TYPE = MapUtils.getString(filed, "FIELD_DATA_TYPE", "");
			po.ORDER_ID = MapUtils.getIntValue(filed, "ORDER_ID");
			res.add(po);
		}
		return res;
	}

	// 读取节点与关联数据的关联关系
	public static List<NodeJoinPO> readNodeJoinRel(DataAccess access, String nodeIds) throws SQLException {
		List<NodeJoinPO> res = new ArrayList<NodeJoinPO>();
		if (nodeIds == null || nodeIds.equals(""))
			return res;
		String sql = "SELECT JOIN_ID,NODE_ID,PAR_JOIN_FIELD,PAR_JOIN_ID,JOIN_FIELD " +
				"FROM  ST_NODE_JOIN_REL WHERE NODE_ID IN (" + nodeIds + ") ORDER BY node_id,par_join_id,join_id";
		if (readCount % 1000 == 1) {
			LOG.info("读取节点关联数据关联信息[nodeIds=" + nodeIds + "]，SQL=" + sql);
		}
		Map<String, Object>[] fileds = access.queryForArrayMap(sql);
		for (Map<String, Object> filed : fileds) {
			NodeJoinPO po = new NodeJoinPO();
			po.JOIN_ID = MapUtils.getLongValue(filed, "JOIN_ID");
			po.NODE_ID = MapUtils.getLongValue(filed, "NODE_ID");
			po.PAR_JOIN_FIELD = MapUtils.getString(filed, "PAR_JOIN_FIELD", "");
			po.PAR_JOIN_ID = MapUtils.getLongValue(filed, "PAR_JOIN_ID");
			po.JOIN_FIELD = MapUtils.getString(filed, "JOIN_FIELD", "");
			res.add(po);
		}
		return res;
	}

	public static List<Long> readJoins(DataAccess access) throws SQLException {
		List<Long> res = new ArrayList<Long>();
		String sql = "SELECT JOIN_ID  FROM ST_NODE_JOIN t , ST_DATA_SOURCE d WHERE t.DATA_SOURCE_ID=d.DATA_SOURCE_ID AND "
				+ "d.STATE=1 and DATA_SOURCE_TYPE>=10 and DATA_SOURCE_TYPE<20 and t.STATE=1 ";
		sql += "  order by t.DATA_SOURCE_ID,JOIN_ID";
		Map<String, Object>[] joins = access.queryForArrayMap(sql);
		for (Map<String, Object> join : joins) {
			res.add(MapUtils.getLongValue(join, "JOIN_ID"));
		}
		return res;
	}

	// 读取关联数据信息,当前只支持数据库表关联(ORACLE/MYSQL)
	public static List<JoinPO> readJoins(DataAccess access, String joinIds) throws SQLException {
		List<JoinPO> res = new ArrayList<JoinPO>();
		if (joinIds == null || joinIds.equals(""))
			return res;
		String sql = "SELECT JOIN_ID,t.DATA_SOURCE_ID,TABLE_NAME,TABLE_SQL,DATA_QUERY_SQL"
				+ ",FLASH_TYPE,FLASH_PARAM,DATA_CHECK_RULE,INLOCAL_MEMORY,JOIN_KEY_RULES,d.DATA_SOURCE_NAME,DATA_SOURCE_TYPE"
				+ ",DATA_SOURCE_URL,DATA_SOURCE_USER,DATA_SOURCE_PASS,DATA_SOURCE_DESC,DATA_SOURCE_CFG"
				+ " FROM ST_NODE_JOIN t , ST_DATA_SOURCE d WHERE t.DATA_SOURCE_ID=d.DATA_SOURCE_ID AND d.STATE=1  and t.STATE=1 ";
		// and DATA_SOURCE_TYPE>=10 and DATA_SOURCE_TYPE<20
		sql += " and JOIN_ID in (" + joinIds + ")";
		sql += "  order by t.DATA_SOURCE_ID,JOIN_ID";
		if (readCount % 1000 == 1) {
			LOG.info("读取关联数据信息[joinIds=" + joinIds + "]，SQL=" + sql);
		}
		Map<String, Object>[] joins = access.queryForArrayMap(sql);
		for (Map<String, Object> join : joins) {
			JoinPO po = new JoinPO();
			po.JOIN_ID = MapUtils.getLongValue(join, "JOIN_ID");
			po.TABLE_NAME = MapUtils.getString(join, "TABLE_NAME", "");
			po.TABLE_SQL = MapUtils.getString(join, "TABLE_SQL", "");
			po.DATA_QUERY_SQL = MapUtils.getString(join, "DATA_QUERY_SQL", "");
			po.FLASH_TYPE = MapUtils.getIntValue(join, "FLASH_TYPE");
			po.FLASH_PARAM = MapUtils.getString(join, "FLASH_PARAM", "");
			po.DATA_CHECK_RULE = MapUtils.getString(join, "DATA_CHECK_RULE", "");
			po.INLOCAL_MEMORY = MapUtils.getIntValue(join, "INLOCAL_MEMORY");
			po.JOIN_KEY_RULES = MapUtils.getString(join, "JOIN_KEY_RULES");
			po.dataSourcePo.DATA_SOURCE_ID = MapUtils.getIntValue(join, "DATA_SOURCE_ID");
			po.dataSourcePo.DATA_SOURCE_NAME = MapUtils.getString(join, "DATA_SOURCE_NAME", "");
			po.dataSourcePo.DATA_SOURCE_TYPE = MapUtils.getIntValue(join, "DATA_SOURCE_TYPE");
			po.dataSourcePo.DATA_SOURCE_URL = MapUtils.getString(join, "DATA_SOURCE_URL", "");
			po.dataSourcePo.DATA_SOURCE_USER = MapUtils.getString(join, "DATA_SOURCE_USER", "");
			po.dataSourcePo.DATA_SOURCE_PASS = MapUtils.getString(join, "DATA_SOURCE_PASS", "");
			po.dataSourcePo.DATA_SOURCE_DESC = MapUtils.getString(join, "DATA_SOURCE_DESC", "");
			po.dataSourcePo.DATA_SOURCE_CFG = MapUtils.getString(join, "DATA_SOURCE_CFG", "");
			res.add(po);
		}
		return res;
	}

	public static boolean buildJoinFields(JoinPO joinPo) throws IOException {
		DataAccess joinAccess = null;
		try {
			joinAccess = getDataAccess(joinPo.dataSourcePo);
			List<String> tables = new ArrayList<String>();
			if (joinPo.TABLE_SQL != null && !joinPo.TABLE_SQL.trim().equals("")) {
				String sql = joinPo.TABLE_SQL;
				sql = EStormConstant.macroProcess(sql);
				Object[][] tabs = joinAccess.queryForArray(sql, false, null);
				for (Object[] tab : tabs) {
					tables.add(tab[0].toString());
				}
			} else {
				String joinTabls = joinPo.TABLE_NAME;
				joinTabls = EStormConstant.macroProcess(joinTabls);
				if (joinTabls != null && !joinTabls.trim().equals("")) {
					String[] js = joinTabls.split(",");
					for (String t : js)
						tables.add(t);
				}
			}
			if (tables.size() == 0) {
				LOG.error("关联数据[" + joinPo.JOIN_ID + "]配置异常，无关联表:" + joinPo);
				return false;
			}
			String sql = joinPo.DATA_QUERY_SQL;
			sql = EStormConstant.macroProcess(sql);
			sql = sql.replaceAll("(?i)\\{JOIN_TABLE\\}", tables.get(0));
			Map<String, FieldPO> fields = getDataType(joinAccess, sql);
			if (fields == null) {
				LOG.error("关联数据[" + joinPo.JOIN_ID + "]配置异常，无法正常读取字段列表:" + joinPo);
				return false;
			}
			joinPo.fields = new ArrayList<FieldPO>();
			for (String key : fields.keySet()) {
				JoinFieldPO fpo = new JoinFieldPO();
				FieldPO f = fields.get(key);
				fpo.FIELD_DATA_TYPE = f.FIELD_DATA_TYPE;
				fpo.FIELD_ID = f.FIELD_ID;
				fpo.FIELD_NAME = f.FIELD_NAME;
				fpo.ORDER_ID = f.ORDER_ID;
				joinPo.fields.add(fpo);
			}
			return true;
		} finally {
			try {
				if (joinAccess != null)
					joinAccess.getConnection().close();
			} catch (SQLException e) {
			}
		}
	}

	public static DataAccess getDataAccess(DataSourcePO po) throws IOException {
		String dataSourceId = po.DATA_SOURCE_ID + "";
		if (!DataSourceManager.containKey(dataSourceId)) {
			try {
				DataSourceImpl ds = new com.ery.base.support.jdbc.DataSourceImpl(po.DATA_SOURCE_CFG,
						po.DATA_SOURCE_URL, po.DATA_SOURCE_USER, po.DATA_SOURCE_PASS);
				ds.setMinIdle(2);
				DataSourceManager.addDataSource(dataSourceId, ds);
			} catch (SQLException e) {
				throw new IOException(e);
			}
		}
		return new DataAccess(DataSourceManager.getConnection(dataSourceId));
	}

	public static Map<String, FieldPO> getDataType(DataAccess access, String sql) {
		ResultSet rs = null;
		try {
			rs = access.execQuerySql(sql);
			ResultSetMetaData resultSetMetaData = rs.getMetaData();
			int colsCount = resultSetMetaData.getColumnCount(); // 取得结果集列数
			Map<String, FieldPO> res = new HashMap<String, FieldPO>();
			for (int i = 0; i < colsCount; i++) {
				FieldPO fpo = new FieldPO();
				fpo.FIELD_ID = -1;
				fpo.FIELD_NAME = resultSetMetaData.getColumnName(i + 1).toUpperCase();
				fpo.FIELD_DATA_TYPE = resultSetMetaData.getColumnTypeName(i + 1).toUpperCase();
				fpo.ORDER_ID = i;
				res.put(fpo.FIELD_NAME, fpo);
			}
			return res;
		} catch (Exception e) {
			return null;
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
				}
		}
	}

	// 读取消息配置
	public static List<DataStorePO> readDataStores(DataAccess access, String nodeIds) throws IOException {
		List<DataStorePO> res = new ArrayList<DataStorePO>();
		if (nodeIds == null || nodeIds.equals(""))
			return res;
		String sql = " SELECT NODE_ID,TARGET_NAME,STORE_FIELD_MAPPING,TARGET_PRIMARY_RULE,AUTO_REAL_FLUSH" +
				",FLUSH_BUFFER_SIZE,FLUSH_TIME_INTERVAL,d.DATA_SOURCE_ID,d.DATA_SOURCE_NAME,DATA_SOURCE_TYPE" +
				",DATA_SOURCE_URL,DATA_SOURCE_USER,DATA_SOURCE_PASS,DATA_SOURCE_DESC,DATA_SOURCE_CFG,STORE_ID,STORE_EXPR " +
				" FROM ST_NODE_STORE t , ST_DATA_SOURCE d " +
				" WHERE t.DATA_SOURCE_ID=d.DATA_SOURCE_ID AND d.STATE=1 and NODE_ID in(" + nodeIds +
				") order by  NODE_ID ";
		if (readCount % 1000 == 1) {
			LOG.info("读取节点数据转储配置[nodeIds=" + nodeIds + "]，SQL=" + sql);
		}
		Map<String, Object>[] fileds = access.queryForArrayMap(sql);
		for (Map<String, Object> filed : fileds) {
			DataStorePO po = new DataStorePO();
			po.NODE_ID = MapUtils.getLongValue(filed, "NODE_ID");
			po.STORE_ID = MapUtils.getLongValue(filed, "STORE_ID");
			po.TARGET_NAME = MapUtils.getString(filed, "TARGET_NAME", "");
			po.STORE_FIELD_MAPPING = MapUtils.getString(filed, "STORE_FIELD_MAPPING", "");
			po.TARGET_PRIMARY_RULE = MapUtils.getString(filed, "TARGET_PRIMARY_RULE", "");
			po.STORE_EXPR = MapUtils.getString(filed, "STORE_EXPR", "");
			po.AUTO_REAL_FLUSH = MapUtils.getIntValue(filed, "AUTO_REAL_FLUSH");
			po.FLUSH_BUFFER_SIZE = MapUtils.getLongValue(filed, "FLUSH_BUFFER_SIZE");
			po.FLUSH_TIME_INTERVAL = MapUtils.getLongValue(filed, "FLUSH_TIME_INTERVAL");
			po.dataSourcePo.DATA_SOURCE_ID = MapUtils.getIntValue(filed, "DATA_SOURCE_ID");
			po.dataSourcePo.DATA_SOURCE_NAME = MapUtils.getString(filed, "DATA_SOURCE_NAME", "");
			po.dataSourcePo.DATA_SOURCE_TYPE = MapUtils.getIntValue(filed, "DATA_SOURCE_TYPE");
			po.dataSourcePo.DATA_SOURCE_URL = MapUtils.getString(filed, "DATA_SOURCE_URL", "");
			po.dataSourcePo.DATA_SOURCE_USER = MapUtils.getString(filed, "DATA_SOURCE_USER", "");
			po.dataSourcePo.DATA_SOURCE_PASS = MapUtils.getString(filed, "DATA_SOURCE_PASS", "");
			po.dataSourcePo.DATA_SOURCE_DESC = MapUtils.getString(filed, "DATA_SOURCE_DESC", "");
			po.dataSourcePo.DATA_SOURCE_CFG = MapUtils.getString(filed, "DATA_SOURCE_CFG", "");
			parseDataStoreFields(po);
			res.add(po);
		}
		return res;
	}

	public static void parseDataStoreFields(DataStorePO po) throws IOException {
		Map<String, FieldPO> fields = null;
		if (po.dataSourcePo.DATA_SOURCE_TYPE / 10 == 1) {// 数据库
			DataAccess access = getDataAccess(po.dataSourcePo);
			String sql = "select * from " + po.TARGET_NAME;
			fields = getDataType(access, sql);
		}
		po.fields = new ArrayList<FieldPO>();
		String[] fds = po.STORE_FIELD_MAPPING.split(",");
		for (int i = 0; i < fds.length; i++) {
			String field = fds[i];
			DataFieldPO fpo = new DataFieldPO();
			String tmp[] = field.split(":");
			if (tmp.length != 2)
				continue;
			fpo.FIELD_ID = -1;
			fpo.SRC_FIELD = tmp[0];
			fpo.FIELD_NAME = tmp[1];
			fpo.ORDER_ID = i;
			if (fields != null && fields.containsKey(fpo.FIELD_NAME)) {
				fpo.FIELD_DATA_TYPE = fields.get(fpo.FIELD_NAME).FIELD_DATA_TYPE;
			} else {
				fpo.FIELD_DATA_TYPE = "STRING";
			}
			po.fields.add(fpo);
		}
	}

	// 读取消息配置
	public static List<PushPO> readNodePushs(DataAccess access, String nodeIds) throws SQLException {
		List<PushPO> res = new ArrayList<PushPO>();
		if (nodeIds == null || nodeIds.equals(""))
			return res;
		String sql = " SELECT t.PUSH_ID,t.NODE_ID,t.PUSH_TYPE,t.PUSH_URL,t.PUSH_USER" +
				",t.PUSH_PASS,t.PUSH_EXPR,t.PUSH_PARAM,SUPPORT_BATCH,PUSH_BUFFER_SIZE" +
				",PUSH_TIME_INTERVAL,PUSH_MSG_TYPE  FROM ST_NODE_PUSH t WHERE t.NODE_ID IN (" + nodeIds +
				") order by t.NODE_ID,t.PUSH_ID";
		if (readCount % 1000 == 1) {
			LOG.info("读取节点数据订阅配置[nodeIds=" + nodeIds + "]，SQL=" + sql);
		}
		Map<String, Object>[] fileds = access.queryForArrayMap(sql);
		for (Map<String, Object> filed : fileds) {
			PushPO po = new PushPO();
			po.PUSH_ID = MapUtils.getLongValue(filed, "PUSH_ID");
			po.NODE_ID = MapUtils.getLongValue(filed, "NODE_ID");
			po.PUSH_TYPE = MapUtils.getIntValue(filed, "PUSH_TYPE");
			po.PUSH_URL = MapUtils.getString(filed, "PUSH_URL", "");
			po.PUSH_USER = MapUtils.getString(filed, "PUSH_USER", "");
			po.PUSH_PASS = MapUtils.getString(filed, "PUSH_PASS", "");
			po.PUSH_EXPR = MapUtils.getString(filed, "PUSH_EXPR", "");
			po.PUSH_PARAM = MapUtils.getString(filed, "PUSH_PARAM", "");
			po.pushMsgType = MapUtils.getIntValue(filed, "PUSH_MSG_TYPE");
			po.SUPPORT_BATCH = MapUtils.getIntValue(filed, "SUPPORT_BATCH");
			po.PUSH_TIME_INTERVAL = MapUtils.getLongValue(filed, "PUSH_TIME_INTERVAL");
			po.PUSH_BUFFER_SIZE = MapUtils.getLongValue(filed, "PUSH_BUFFER_SIZE");
			res.add(po);
		}
		return res;
	}

	// read seq
	public synchronized static long readSeq(DataAccess access, Configuration conf, String seqName) {
		if (!NodeLog.inited) {
			NodeLog.init(conf);
		}
		if (NodeLog.configDbType.equals("oracle")) {
			return access.queryForLongByNvl("select " + seqName + ".nextval from dual", 1);
		} else if (NodeLog.configDbType.equals("other")) {
			long l = access.queryForLongByNvl("select seq from sys_seq where SEQ_NAME=?", 0, seqName);
			if (l == 0) {
				access.execUpdate("insert into sys_seq (SEQ_NAME,SEQ) values(?,?)", seqName, 1);
			} else {
				access.execUpdate("update   sys_seq  set  SEQ=? where SEQ_NAME=?", l + 1, seqName);
			}
			return l;
		}
		return 0;
	}

	// read seq
	public synchronized static long readMaxSeq(DataAccess access, Configuration conf, String tableName, String filedName) {
		if (!NodeLog.inited) {
			NodeLog.init(conf);
		}
		return access.queryForLongByNvl("select max(" + filedName + ") maxF from " + tableName + " ", 0);
	}
}
