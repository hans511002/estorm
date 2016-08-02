package com.ery.estorm.join;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ery.estorm.client.node.StormNodeConfig;
import com.ery.estorm.config.EStormConfigRead;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.topology.NodeInfo.JoinPO;
import com.ery.estorm.daemon.topology.NodeInfo.JoinPO.LoadState;
import com.ery.estorm.util.Bytes;
import com.ery.estorm.util.ClassSize;
import com.ery.estorm.util.HasThread;
import com.ery.estorm.util.HeapSize;
import com.ery.base.support.jdbc.DataAccess;

/**
 * 
 * =======内存关联数据放静态对象====需要在Node运行的JVM上存储，无法判断内存大小，请谨慎配置内存关联====================
 * 
 * @author hans
 * 
 */
public class JoinData {
	private static final Log LOG = LogFactory.getLog(JoinData.class);
	// 必需数据加载完成再添加到列表中
	public static Map<Long, JoinDataTable> JoinTables = new HashMap<Long, JoinDataTable>();
	public static long heapSize = ClassSize.OBJECT;
	public static byte[] JoinFamily = "f".getBytes();
	public static byte[] JoinQualifier = "q".getBytes();
	public static final char joinKeyColSplitChar = '\0';

	public static class JoinDataTable implements HeapSize {
		public static final long MUTATION_OVERHEAD = ClassSize.align(
		// This
				ClassSize.OBJECT +
				// jpo+heapSize
						ClassSize.REFERENCE + Bytes.SIZEOF_LONG +
						// table
						ClassSize.REFERENCE +
						// table
						ClassSize.TREEMAP);
		long heapSize = MUTATION_OVERHEAD;
		public JoinPO jpo = null;

		public JoinDataTable(JoinPO jpo) {
			this.jpo = jpo;
		}

		/**
		 * bus_keys {col_keys,row}
		 */
		Map<String, Map<String, byte[][]>> table = new HashMap<String, Map<String, byte[][]>>();

		public Map<String, byte[][]> getBusTable(String busCols) {
			// String busKey = buildBusKey(busCols);
			// if (busKey == null)
			// return null;
			return table.get(busCols);
		}

		public List<String> getAllBusKey() {
			return new ArrayList<String>(table.keySet());
		}

		public void put(byte[][] row) {
			synchronized (table) {
				StringBuffer sb = new StringBuffer();
				int incSize = 0;
				for (int i = 0; i < jpo.busKeyColsIndex.length; i++) {
					sb.setLength(0);
					for (int c = 0; c < jpo.busKeyColsIndex[i].length; c++) {
						sb.append(new String(row[jpo.busKeyColsIndex[i][c]]));
						sb.append(JoinData.joinKeyColSplitChar);
					}
					Map<String, byte[][]> busRel = table.get(jpo.busKeys[i]);
					if (busRel == null) {
						busRel = new HashMap<String, byte[][]>();
						table.put(jpo.busKeys[i], busRel);
						incSize += ClassSize.align(ClassSize.MAP_ENTRY + ClassSize.REFERENCE);
					}
					String key = sb.toString();
					busRel.put(key, row);
					incSize += ClassSize.align(ClassSize.MAP_ENTRY + ClassSize.STRING + key.length());
					if (i == 0)
						incSize += getHeapSize(row);
					else
						incSize += ClassSize.REFERENCE;
				}
				JoinData.heapSize += incSize;
				heapSize += incSize;
			}
		}

		public void clear() {
			JoinData.heapSize = JoinData.heapSize - heapSize + MUTATION_OVERHEAD;
			heapSize = MUTATION_OVERHEAD;
			table.clear();
			this.jpo = null;
		}

		public void setJpo(JoinPO jpo) {
			this.jpo = jpo;
			clear();
		}

		/**
		 * @return Calculate what Mutation adds to class heap size.
		 */
		@Override
		public long heapSize() {
			return this.heapSize;
		}
	}

	public static long getHeapSize(byte[][] row) {
		long heapsize = ClassSize.align(ClassSize.ARRAY + row.length * ClassSize.ARRAY);
		heapsize += ClassSize.align(row.length);
		for (byte[] obj : row) {
			heapsize += obj.length;
		}
		return heapsize;
	}

	public static synchronized void remove(JoinPO jpo) {
		JoinDataTable jdt = JoinTables.remove(jpo.JOIN_ID);
		heapSize -= jdt.heapSize;
	}

	// private static String buildBusKey(String... busCols) {
	// if (busCols == null || busCols.length == 0)
	// return null;
	// String busKey = "";
	// for (String k : busCols) {
	// busKey += k + ",";
	// }
	// return busKey;
	// }

	// 开子线程加载数据
	public static void loadJoinData(JoinPO jpo) {
		if (jpo.needLoad == false)
			return;
		JoinDataTable jdt = JoinTables.get(jpo.JOIN_ID);// jpo状态不为正常可用
		if (jdt == null) {
			jdt = new JoinDataTable(jpo);
			JoinTables.put(jpo.JOIN_ID, jdt);
		} else {
			synchronized (jdt) {
				jdt.setJpo(jpo);
			}
		}
		int maxJoinDataHeapsize = StormNodeConfig.conf.getInt(
				EStormConstant.ESTORM_JOINDATA_STORE_LOCALMEMORY_HEAPSIZE, 1024 * 1024 * 1024);// 1G
		if (heapSize > maxJoinDataHeapsize - 1024 * 1024) {
			LOG.error("no enough local memory to store join data ,used " + heapSize + " config " + maxJoinDataHeapsize);
		}
		new loadThread(jdt, jpo).setDaemon(true).start();
	}

	public static class loadThread extends HasThread {
		final JoinDataTable jdt;
		final JoinPO jpo;
		int loadCount = 0;

		loadThread(JoinDataTable jdt, JoinPO jpo) {
			this.jdt = jdt;
			this.jpo = jpo;
		}

		public void run() {
			DataAccess joinAccess = null;
			ResultSet rs = null;
			try {
				joinAccess = EStormConfigRead.getDataAccess(jpo.dataSourcePo);
				List<String> tables = new ArrayList<String>();
				if (jpo.TABLE_SQL != null && !jpo.TABLE_SQL.trim().equals("")) {
					String sql = jpo.TABLE_SQL;
					sql = EStormConstant.macroProcess(sql);
					LOG.info("关联数据加载[" + jpo.JOIN_ID + "]--执行关联表查询SQL=" + sql);
					Object[][] tabs = joinAccess.queryForArray(sql, false, null);
					for (Object[] tab : tabs) {
						tables.add(tab[0].toString());
					}
				} else {
					String joinTabls = jpo.TABLE_NAME;
					joinTabls = EStormConstant.macroProcess(joinTabls);
					if (joinTabls != null && !joinTabls.trim().equals("")) {
						String[] js = joinTabls.split(",");
						for (String t : js)
							tables.add(t);
					}
				}
				if (tables.size() == 0) {
					throw new IOException("关联数据[" + jpo.JOIN_ID + "]配置异常，无关联表:" + jpo);
				}
				LOG.info("关联数据加载[" + jpo.JOIN_ID + "]--关联数据表" + tables);
				String sql = jpo.DATA_QUERY_SQL;
				sql = EStormConstant.macroProcess(sql);
				jpo.partions = tables;
				for (String table : tables) {
					if (table == null || table.trim().equals(""))
						continue;
					String selSql = sql.replaceAll("(?i)\\{JOIN_TABLE\\}", table);
					rs = joinAccess.execQuerySql(selSql);
					if (rs == null)
						throw new IOException("执行关联数据查询失败：" + selSql);
					while (rs.next()) {
						byte[][] row = new byte[jpo.nodeOutFields.length][];
						for (int i = 1; i <= jpo.nodeOutFields.length; i++) {
							Object obj = rs.getObject(i);
							if (obj instanceof String) {
								row[i - 1] = ((String) obj).getBytes();
							} else if (obj instanceof Date) {
								row[i - 1] = EStormConstant.sdf.format((Date) obj).getBytes();
							} else {
								row[i - 1] = obj.toString().getBytes();
							}
						}
						jdt.put(row);
					}
					rs.close();
					rs = null;
				}
				jpo.loadState = LoadState.Success;
			} catch (Exception e) {
				jpo.loadState = LoadState.Failed;
				LOG.error("加载关联规则[" + jpo.JOIN_ID + "]数据到内存失败", e);
			} finally {
				if (rs != null) {
					joinAccess.close(rs);
				}
				loadCount++;
				try {
					if (joinAccess != null)
						joinAccess.getConnection().close();
					int retryCount = StormNodeConfig.conf.getInt(EStormConstant.ESTORM_JOIN_LOAD_RETRY_TIMES, 3);
					if (loadCount < retryCount) {
						this.run();
					}
				} catch (SQLException e) {
					LOG.error("加载完关联 数据，关联数据库连接失败", e);
				}
			}
		}
	}

}
