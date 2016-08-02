package com.ery.estorm.client.node.prostore;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

import com.ery.estorm.client.node.BoltNode;
import com.ery.estorm.client.node.BoltNode.BufferRow;
import com.ery.estorm.client.node.BoltNode.ProcData;
import com.ery.estorm.config.Configuration;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.config.HadoopConf;
import com.ery.estorm.daemon.topology.NodeInfo;
import com.ery.estorm.daemon.topology.NodeInfo.NodeFieldPO;
import com.ery.estorm.daemon.topology.NodeInfo.NodeJoinPO;
import com.ery.estorm.daemon.topology.NodeInfo.NodePO;
import com.ery.estorm.exceptions.ConfigException;
import com.ery.estorm.join.DataSerializable;
import com.ery.estorm.join.JoinData;
import com.ery.estorm.log.NodeLog.NodeMsgLogPo;
import com.ery.estorm.util.DataInputBuffer;
import com.ery.estorm.util.DataOutputBuffer;
import com.ery.estorm.util.ToolUtil;
import com.ery.base.support.jdbc.DataAccess;

public class ProStoreConnection {
	public static final Log LOG = LogFactory.getLog(BoltNode.class);
	public final String procStoreType;
	public String[] procStorePar;
	final String tableName;
	final NodeInfo nodeInfo;
	final NodePO nodePo;
	public Configuration conf = null;

	public HTable table;
	public final DataAccess access = new DataAccess();
	final BoltNode owner;
	public final int keyType;
	public final boolean isJoin;

	public byte[] _Family = "f".getBytes();
	public byte[] _Qualifier = "q".getBytes();
	public final StoreDbConfig storeDbConfig;

	public static byte[] StoreFamily = "f".getBytes();
	public static byte[] StoreQualifier = "q".getBytes();

	public static Pattern DbUrlPattern = Pattern.compile("([\\w_]+)/([\\w_]+)@([\\w\\.:]+)");
	public static final char storeKeyColSplitChar = ':';

	private final int reTryTimes;

	public static class StoreDbConfig {
		public String drive;
		public String url;
		public String user;
		public String password;

		StoreDbConfig(String[] pars) throws ConfigException {
			if (pars == null)
				return;
			if (pars.length > 0) {
				Matcher m = DbUrlPattern.matcher(pars[0]);
				if (m.find()) {
					user = m.group(1);
					password = m.group(2);
					url = m.group(3);
				} else {
					throw new ConfigException("数据库连接串格式不正确:" + pars[0]);
				}
			}
			if (pars.length > 1) {
				drive = pars[1];
			}
		}
	}

	public ProStoreConnection(BoltNode owner, Configuration conf) throws Exception {
		this.owner = owner;
		isJoin = false;
		procStoreType = conf.get(EStormConstant.ESTORM_NODE_PROCDATA_CACHE_STORE_TYPE);
		procStorePar = conf.get(EStormConstant.ESTORM_NODE_PROCDATA_CACHE_STORE_PARAMS).split(";");
		reTryTimes = conf.getInt(EStormConstant.ESTORM_DATA_RETRY_TIMES, 3);
		BoltNode.LOG.info("初始化数据存储链接:TYPE=" + procStoreType);
		BoltNode.LOG.info("PARAMS=" + conf.get(EStormConstant.ESTORM_NODE_PROCDATA_CACHE_STORE_PARAMS));
		this.nodeInfo = owner.nodeInfo;
		nodePo = (NodePO) nodeInfo.node;
		tableName = "node_" + nodeInfo.getNodeId();
		if (this.procStoreType.equals("hbase")) {// 缓存SCAN设置
			keyType = 1;
			HadoopConf hconf = HadoopConf.getConf(conf);
			if (procStorePar != null) {
				for (String par : procStorePar) {
					String[] tmp = par.split("=");
					hconf.set(tmp[0], tmp[1]);
				}
			}
			try {
				createStoreHTable(hconf, tableName, _Family);
				table = new HTable(hconf, tableName);
				this.table.setAutoFlush(false, false);
			} catch (IOException e) {
				LOG.error("连接Hbase表[" + tableName + "]失败,参数:" +
						conf.get(EStormConstant.ESTORM_NODE_PROCDATA_CACHE_STORE_PARAMS) + " msg:" + e.getMessage());
				throw e;
			}
			storeDbConfig = null;
		} else if (this.procStoreType.equals("oracle") || this.procStoreType.equals("mysql")) {// RDBMS库,缓存读取SQL
			keyType = 2;
			try {
				storeDbConfig = new StoreDbConfig(procStorePar);
				openConn();
				// 连接数据,预编译SQL
			} catch (ConfigException e) {
				LOG.error("连接数据库表[" + tableName + "]失败,参数:" +
						conf.get(EStormConstant.ESTORM_NODE_PROCDATA_CACHE_STORE_PARAMS) + " msg:" + e.getMessage());
				throw e;
			}
			table = null;

		} else {
			throw new IOException("不支持的数据存储配置");
		}
	}

	public ProStoreConnection(BoltNode owner, Configuration conf, NodeJoinPO joinPo) throws Exception {
		this.owner = owner;
		isJoin = true;
		_Family = JoinData.JoinFamily;
		_Qualifier = JoinData.JoinQualifier;
		procStoreType = conf.get(EStormConstant.ESTORM_JOINDATA_STORE_TYPE);
		procStorePar = conf.get(EStormConstant.ESTORM_JOINDATA_STORE_PARAMS).split(";");
		reTryTimes = conf.getInt(EStormConstant.ESTORM_DATA_RETRY_TIMES, 3);
		BoltNode.LOG.info("初始化关联数据存储链接:TYPE=" + procStoreType);
		BoltNode.LOG.info("PARAMS=" + conf.get(EStormConstant.ESTORM_NODE_PROCDATA_CACHE_STORE_PARAMS));
		this.nodeInfo = owner.nodeInfo;
		nodePo = (NodePO) nodeInfo.node;
		if (this.procStoreType.equals("hbase")) {// 缓存SCAN设置
			keyType = 1;
			tableName = "join_" + joinPo.JOIN_ID + "_" + Math.abs(joinPo.JOIN_FIELD.hashCode());
			HadoopConf hconf = HadoopConf.getConf(conf);
			if (procStorePar != null) {
				for (String par : procStorePar) {
					String[] tmp = par.split("=");
					hconf.set(tmp[0], tmp[1]);
				}
			}
			try {
				createStoreHTable(hconf, tableName, _Family);
				table = new HTable(hconf, tableName);
				this.table.setAutoFlush(false, false);
			} catch (IOException e) {
				LOG.error("连接Hbase表[" + tableName + "]失败,参数:" +
						conf.get(EStormConstant.ESTORM_NODE_PROCDATA_CACHE_STORE_PARAMS) + " msg:" + e.getMessage());
				throw e;
			}
			storeDbConfig = null;
		} else if (this.procStoreType.equals("oracle") || this.procStoreType.equals("mysql") ||
				this.procStoreType.equals("ob")) {// RDBMS库,缓存读取SQL
			keyType = 2;
			tableName = "join_" + joinPo.JOIN_ID;// 使用索引
			try {
				storeDbConfig = new StoreDbConfig(procStorePar);
				openConn();
				// 连接数据,预编译SQL
			} catch (ConfigException e) {
				LOG.error("连接数据库表[" + tableName + "]失败,参数:" +
						conf.get(EStormConstant.ESTORM_NODE_PROCDATA_CACHE_STORE_PARAMS) + " msg:" + e.getMessage());
				throw e;
			}
			table = null;
		} else {
			throw new Exception("不支持的数据存储配置");
		}
	}

	public static void createStoreHTable(HadoopConf conf, String tableName, byte[] _Family) throws IOException {
		createStoreHTable(conf, tableName, _Family, 512);
	}

	public static void createStoreHTable(HadoopConf conf, String tableName, byte[] _Family, int maxSizeMB)
			throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		System.out.println("Result==" + Result.class.getCanonicalName());
		if (admin.tableExists(tableName)) {
			admin.close();
			return;
		}
		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		HColumnDescriptor col = new HColumnDescriptor(_Family);
		tableDesc.setMaxFileSize(maxSizeMB * (1 << 20));
		col.setMaxVersions(1);
		col.setCompressionType(Algorithm.SNAPPY);
		col.setBlocksize(1 << 13);// 8K
		col.setCompactionCompressionType(Algorithm.SNAPPY);
		tableDesc.addFamily(col);
		admin.createTable(tableDesc);
		admin.close();
	}

	public void openConn() throws SQLException, IOException {
		if (keyType == 1) {
			HadoopConf hconf = HadoopConf.getConf(conf, procStorePar);
			createStoreHTable(hconf, tableName, _Family);
			table = new HTable(hconf, tableName);
			this.table.setAutoFlush(false, false);
		} else {
			Connection con = DriverManager.getConnection(storeDbConfig.url, storeDbConfig.user, storeDbConfig.password);
			access.setConnection(con);
		}
	}

	public void close() {
		try {
			if (keyType == 1) {
				this.table.close();
				this.table = null;
			} else {
				access.getConnection().close();
			}
		} catch (Exception e) {
			LOG.error("", e);
		}
	}

	// /////////////////Hbase//////////////
	private Result getResult(byte[] rowKey) throws IOException {
		int retry = 0;
		while (retry++ < reTryTimes) {
			try {
				Get get = new Get(rowKey);
				get.addColumn(_Family, _Qualifier);
				return table.get(get);
			} catch (IOException e) {
				if (retry >= reTryTimes)
					throw e;
			}
		}
		return null;
	}

	DataInputBuffer inputBuffer = new DataInputBuffer();

	public byte[][] readData(byte[] rowKey) throws IOException {
		if (keyType != 1)
			throw new IOException("错误调用 非Hbase存储调用了Hbase数据存储接口");
		Result r = getResult(rowKey);
		if (r == null)
			return null;
		byte[] data = r.getValue(_Family, _Qualifier);
		if (data == null)
			return null;
		return DataSerializable.Deserialization(data, inputBuffer);

	}

	// ////////////End Hbase//////////////
	// /////////////////Rdbms//////////////
	public byte[][] readData(String[] cols) throws IOException {
		if (keyType != 2)
			throw new IOException("错误调用 ,非Rdbms存储调用了Rdbms数据存储接口");
		return null;
	}

	// ////////////End Rdbms//////////////
	// 读取原基准数据,当节点无计算时不需要读取
	public BufferRow readData(BufferRow drow, NodeMsgLogPo msgLog) throws IOException {
		BufferRow bufRow = new BufferRow();
		if (keyType == 1) {
			// this.nodePo.nodeOutFields
			bufRow.key = drow.key;
			byte[][] rowData = readData(bufRow.key.getBytes());
			if (rowData == null)
				return bufRow;
			if (rowData.length < drow.data.length) {
				List<String> obj = new ArrayList<String>();
				for (byte[] val : rowData) {
					obj.add(new String(val));
				}
				msgLog.ERROR = "过程结果字段数比目标数据字段数少,将丢弃此过程数据,读取字段[" + rowData.length + "],目标字段[" + drow.data.length +
						"],读取值:" + EStormConstant.objectMapper.writeValueAsString(obj);
				return bufRow;
			}
			bufRow.data = new Object[drow.data.length];
			for (int i = 0; i < drow.data.length; i++) {
				bufRow.data[i] = convertToObject(rowData[i], i);
			}
			if (rowData.length > drow.data.length) {// PROCEDURE_DATA_COLNAME =
													// "__PROC_DATA__";// is map
													// ser
				bufRow.procData = ProcData.deserialize(rowData[drow.data.length], inputBuffer);
				// EStormConstant.castObject(rowData[drow.data.length]);
			}
			return bufRow;
		} else {// RDBMS库,缓存读取SQL

		}
		return bufRow;
	}

	public String getBusKey(Object[] drow) {
		StringBuffer inputKey = new StringBuffer();
		for (int i : this.nodePo.busKeyFileds) {
			if (inputKey.length() > 0) {
				inputKey.append(storeKeyColSplitChar);
			}
			inputKey.append(drow[i].toString());
		}
		return inputKey.toString();
	}

	DataOutputBuffer outputBuffer = new DataOutputBuffer(1024);
	DataOutputBuffer procOutBuffer = new DataOutputBuffer(1024);

	// Object[] destRow, Map<String, Object> procData
	public void writeData(BufferRow destRow, String key) throws IOException {
		if (keyType == 1) {
			// this.nodePo.nodeOutFields
			byte[][] row = new byte[destRow.data.length + (destRow.procData == null ? 0 : 1)][];
			for (int i = 0; i < destRow.data.length; i++) {
				row[i] = destRow.data[i].toString().getBytes();
			}
			if (destRow.procData != null) {
				row[destRow.data.length] = destRow.procData.serializable(procOutBuffer);
				// EStormConstant.Serialize(destRow.procData);
			}
			Put put = new Put(key.getBytes());
			put.add(_Family, _Qualifier, DataSerializable.Serializable(row, outputBuffer));
			this.table.put(put);
		} else {// RDBMS库,缓存读取SQL

		}
	}

	private Object convertToObject(byte[] data, int index) {
		NodeFieldPO fpo = (NodeFieldPO) this.nodePo.nodeOutFields[index];
		String val = new String(data);
		switch (fpo.filedType) {
		case 1:
			return val;
		case 2:// CHAR字符，NUMBER(2)
			try {
				if (val.indexOf('.') > 0)
					return Double.parseDouble(val);
				else
					return Long.parseLong(val);
			} catch (Exception e) {
				return ToolUtil.toLong(val);
			}
		case 4:
			try {
				return Double.parseDouble(val);
			} catch (Exception e) {
				return ToolUtil.toDouble(val);
			}
		case 8:// DATE
			return Long.parseLong(val);
		default:
			return val;
		}
	}

	public void submit() throws Exception {
		if (keyType == 1) {
			this.table.flushCommits();
		} else {
			this.access.commit();
		}
	}

}
