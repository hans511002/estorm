package com.ery.estorm.join;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.zookeeper.KeeperException;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.ery.estorm.client.node.StormNodeConfig;
import com.ery.estorm.client.node.prostore.ProStoreConnection;
import com.ery.estorm.config.Configuration;
import com.ery.estorm.config.EStormConfigRead;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.config.HadoopConf;
import com.ery.estorm.daemon.DaemonMaster;
import com.ery.estorm.daemon.topology.NodeInfo.JoinPO;
import com.ery.estorm.daemon.topology.NodeInfo.JoinPO.JoinPartition;
import com.ery.estorm.daemon.topology.NodeInfo.JoinPO.JoinPartition.ServerLoadState;
import com.ery.estorm.daemon.topology.NodeInfo.JoinPO.LoadState;
import com.ery.estorm.util.DataOutputBuffer;
import com.ery.estorm.util.HasThread;
import com.ery.estorm.util.StringUtils;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperNodeTracker;
import com.ery.estorm.zk.ZooKeeperWatcher;
import com.ery.base.support.jdbc.DataAccess;

public class JoinOrderTracker extends ZooKeeperNodeTracker {
	protected static final Log LOG = LogFactory.getLog(JoinOrderTracker.class);
	final DaemonMaster master;
	Map<JoinPartition, JoinLoadhandler> running = new HashMap<JoinPartition, JoinLoadhandler>();

	public JoinOrderTracker(ZooKeeperWatcher watcher, String node, DaemonMaster master) {
		super(watcher, node, master);
		this.master = master;
		this.start();
	}

	// public static void createNode(ZooKeeperWatcher watcher, String node,
	// byte[] data) {
	// try {
	// ZKUtil.createNodeIfNotExistsAndWatch(watcher, node, data);
	// } catch (KeeperException e) {
	// LOG.error("创建关联数据命令节点错误:", e);
	// } catch (IOException e) {
	// LOG.error("创建关联数据命令节点错误:", e);
	// }
	// }

	@Override
	public synchronized void nodeCreated(String path) {
		if (!path.equals(node))
			return;
		super.nodeCreated(path);
		try {// 启动子线程执行命令 删除命令节点
			if (this.data != null) {
				JoinPartition jpt = EStormConstant.castObject(data);
				synchronized (running) {
					if (!running.containsKey(jpt)) {
						JoinLoadhandler handler = new JoinLoadhandler(this, jpt);
						running.put(jpt, handler);
						handler.setDaemon(true);
						handler.start();
					}
				}
			}
			ZKUtil.deleteNode(this.watcher, path);
		} catch (Exception e) {
			LOG.error("删除命令节点失败：" + path, e);
		}
	}

	public static class JoinLoadhandler extends HasThread {
		final JoinOrderTracker joinOrderTracker;
		public final JoinPartition jpt;
		final DaemonMaster master;
		public final String priPath;
		final ZooKeeperWatcher watcher;
		final JoinListTracker joinListTracker;
		final JoinPO jpo;
		final JoinDataStoreConfig jdsc;
		final Configuration conf;
		int loadCount = 0;

		public JoinLoadhandler(JoinOrderTracker joinOrderTracker, JoinPartition jpt) {
			this.joinOrderTracker = joinOrderTracker;
			this.jpt = jpt;
			this.master = joinOrderTracker.master;
			this.watcher = joinOrderTracker.watcher;
			this.joinListTracker = master.getJoinDataManage().joinListTracker;
			priPath = ZKUtil.joinZNode(watcher.estormJoinZNode, "join_" + jpt.joinId);
			jpo = master.getJoinDataManage().joinListTracker.joins.get(jpt.joinId);
			conf = master.getConfiguration();
			jdsc = new JoinDataStoreConfig(conf);

		}

		@Override
		public void run() {
			// 更新分区状态
			if (jpt.st == 0)
				jpt.st = System.currentTimeMillis();
			ServerLoadState sls = jpt.loadServer.get(master.serverName.getHostname());
			try {
				if (sls == null) {
					jpt.loadServer.put(master.serverName.getHostname(), new ServerLoadState(LoadState.Initing));
					sls = jpt.loadServer.get(master.serverName.getHostname());
				}
				sls.state = LoadState.Initing;
				sls.st = System.currentTimeMillis();
				if (jpo == null)
					throw new IOException("在关联对象列表中未找到数据加载命令中的关联ID[" + jpt.joinId + "]");
				// 提交 ZK
				jpt.updateToZk(watcher, priPath);
				loadData(sls);
			} catch (Exception e) {
				sls.msg = StringUtils.printStackTrace(e);
				sls.state = LoadState.Failed;
				jpt.state = LoadState.Failed;
				sls.et = System.currentTimeMillis();
			} finally {
				synchronized (joinOrderTracker.running) {
					joinOrderTracker.running.remove(jpt);
				}
				try {
					jpt.rnum = sls.rnum;
					jpt.updateToZk(watcher, priPath);
				} catch (Exception e) {
					LOG.error("更新数据关联[" + jpt.joinId + "]加载分区[" + jpt.partion + "]状态失败," + jpt, e);
				}
			}
		}

		public static class JoinDataStoreConfig {
			String joinDataStoreType;
			String[] joinDataStoreParams;
			int printInterval;

			JoinDataStoreConfig(Configuration conf) {
				joinDataStoreType = conf.get(EStormConstant.ESTORM_JOINDATA_STORE_TYPE).toUpperCase();
				joinDataStoreParams = conf.get(EStormConstant.ESTORM_JOINDATA_STORE_PARAMS).split(";");
				printInterval = conf.getInt(EStormConstant.ESTORM_JOINDATA_LOAD_UPDATEINFO_RECORDSIZE, 100000);
			}
		}

		/**
		 * 加载关联分区数据
		 */
		public void loadData(ServerLoadState sls) throws IOException, KeeperException {

			// 分数据来源类型加载数据到特定库中
			// 先根据目标类型分,再按来源分
			if (jdsc.joinDataStoreType.equals("HBASE")) {
				/**
				 * 11,ORACLE 12,MYSQL 13,OB 23,HBASE 34,FTP 45,HDFS
				 */
				loadDataToHbase(sls);
			} else if (jdsc.joinDataStoreType.equals("ORACLE") || jdsc.joinDataStoreType.equals("MYSQL") ||
					jdsc.joinDataStoreType.equals("OB")) {

			} else if (jdsc.joinDataStoreType.equals("FILE")) {// HDFS 文件
				// loadDataToHdfsFile(sls);
			} else {
				throw new IOException("关联 数据存储配置错误，不支持的类型：" + jdsc.joinDataStoreType);
			}

		}

		private void loadDataToHdfsFile(ServerLoadState sls) throws IOException {
			// 使用SEQFILE索引
			sls.rnum = 0;
			if (jpo.dataSourcePo.DATA_SOURCE_TYPE >= 10 && jpo.dataSourcePo.DATA_SOURCE_TYPE < 20) {// rmdbs
				DataAccess joinAccess = EStormConfigRead.getDataAccess(jpo.dataSourcePo);
				ResultSet rs = null;
				org.apache.hadoop.fs.FileSystem fs = FileSystem.get(HadoopConf.getConf(conf));
				Path files[] = new Path[jpo.busKeyRules.length];
				for (int i = 0; i < files.length; i++) {

				}
				Map<String, Object> env = new HashMap<String, Object>();
				try {
					LOG.info("开始加载关联[" + jpo.JOIN_ID + "]数据分区" + jpt.partion);
					String sql = jpo.DATA_QUERY_SQL;
					sql = EStormConstant.macroProcess(sql);
					String selSql = sql.replaceAll("(?i)\\{JOIN_TABLE\\}", jpt.partion);
					rs = joinAccess.execQuerySql(selSql);
					if (rs == null)
						throw new IOException("执行关联数据查询失败：" + selSql);
					DataOutputBuffer buffer = new DataOutputBuffer(128);
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
						byte[] _row = DataSerializable.Serializable(row, buffer);
						// 计算ROWKEY
						// for (int i = 0; i < table.length; i++) {
						// env.clear();
						// for (int c = 0; c < jpo.busKeyCols[i].length; c++) {
						// env.put(jpo.busKeyCols[i][c], new
						// String(row[jpo.busKeyColsIndex[i][c]]));
						// }
						// byte[] rkey =
						// busKeyExps[i].execute(env).toString().getBytes();
						// Put put = new Put(rkey);
						// put.add(JoinData.JoinFamily, JoinData.JoinQualifier,
						// _row);
						// table[i].put(put);
						// }
						sls.rnum++;
						if (sls.rnum % jdsc.printInterval == 0) {
							jpt.updateToZk(watcher, priPath);
							LOG.info("load partition[" + jpt.joinId + "] data " + sls.rnum + " rows");
						}
					}
					// for (int i = 0; i < table.length; i++) {
					// table[i].flushCommits();
					// table[i].close();
					// }
					rs.close();
					LOG.info("load partition[" + jpt.joinId + "] data " + sls.rnum + " rows end");
					rs = null;
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
						joinAccess.getConnection().close();
						int retryCount = StormNodeConfig.conf.getInt(EStormConstant.ESTORM_JOIN_LOAD_RETRY_TIMES, 3);
						if (loadCount < retryCount) {
							this.loadDataToHdfsFile(sls);
						}
					} catch (SQLException e) {
						LOG.error("加载完关联 数据，关联数据库连接失败", e);
					}
				}
			} else {// 其他来源

			}

		}

		public void loadDataToHbase(ServerLoadState sls) throws IOException, KeeperException {
			sls.rnum = 0;
			if (jpo.dataSourcePo.DATA_SOURCE_TYPE >= 10 && jpo.dataSourcePo.DATA_SOURCE_TYPE < 20) {// rmdbs
				DataAccess joinAccess = EStormConfigRead.getDataAccess(jpo.dataSourcePo);
				ResultSet rs = null;
				HTable table[] = new HTable[jpo.busKeyRules.length];
				Expression busKeyExps[] = new Expression[jpo.busKeyRules.length];
				for (int i = 0; i < table.length; i++) {
					String tableName = "join_" + jpo.JOIN_ID + "_" + Math.abs(jpo.busKeyRules[i].hashCode());
					HadoopConf hconf = HadoopConf.getConf(conf);
					ProStoreConnection.createStoreHTable(hconf, tableName, JoinData.JoinFamily, 256);
					if (jdsc.joinDataStoreParams != null) {
						for (String par : jdsc.joinDataStoreParams) {
							String[] tmp = par.split("=");
							hconf.set(tmp[0], tmp[1]);
						}
					}
					table[i] = new HTable(hconf, tableName);
					table[i].setAutoFlush(false, false);
					table[i].setWriteBufferSize((long) ((5 * Math.random() + 5) * (1 << 20)));//
					busKeyExps[i] = AviatorEvaluator.compile(jpo.busKeyRules[i], true);
				}
				Map<String, Object> env = new HashMap<String, Object>();
				int loadRowCount = 0;
				try {
					LOG.info("开始加载关联[" + jpo.JOIN_ID + "]数据分区" + jpt.partion);
					String sql = jpo.DATA_QUERY_SQL;
					sql = EStormConstant.macroProcess(sql);
					String selSql = sql.replaceAll("(?i)\\{JOIN_TABLE\\}", jpt.partion);
					rs = joinAccess.execQuerySql(selSql);
					if (rs == null)
						throw new IOException("执行关联数据查询失败：" + selSql);
					DataOutputBuffer buffer = new DataOutputBuffer(128);
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
						byte[] _row = DataSerializable.Serializable(row, buffer);
						// 计算ROWKEY
						for (int i = 0; i < table.length; i++) {
							env.clear();
							for (int c = 0; c < jpo.busKeyCols[i].length; c++) {
								env.put(jpo.busKeyCols[i][c],
										EStormConstant.castAviatorObject(row[jpo.busKeyColsIndex[i][c]]));
							}
							byte[] rkey = busKeyExps[i].execute(env).toString().getBytes();
							Put put = new Put(rkey);
							put.add(JoinData.JoinFamily, JoinData.JoinQualifier, _row);
							table[i].put(put);
						}
						loadRowCount++;
						if (loadRowCount % jdsc.printInterval == 0) {
							jpt.updateToZk(watcher, priPath);
							LOG.info("load partition[" + jpt.joinId + "] data " + loadRowCount + " rows");
						}
					}
					for (int i = 0; i < table.length; i++) {
						table[i].flushCommits();
						table[i].close();
					}
					rs.close();
					LOG.info("load partition[" + jpt.joinId + "] data " + loadRowCount + " rows end");
					rs = null;
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
						joinAccess.getConnection().close();
						int retryCount = StormNodeConfig.conf.getInt(EStormConstant.ESTORM_JOIN_LOAD_RETRY_TIMES, 3);
						if (loadCount < retryCount) {
							this.loadDataToHbase(sls);
						}
					} catch (SQLException e) {
						LOG.error("加载完关联 数据，关联数据库连接失败", e);
					}
				}
			} else {// 其他来源

			}
		}
	}

	@Override
	public synchronized void nodeDataChanged(String path) {
		if (path.equals(node)) {
			nodeCreated(path);
		}
	}
}
