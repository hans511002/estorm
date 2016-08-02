package com.ery.estorm.client.node;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import backtype.storm.generated.SupervisorInfo;
import backtype.storm.utils.Utils;

import com.ery.estorm.NodeColumnCalc;
import com.ery.estorm.ServerInfo;
import com.ery.estorm.client.node.tracker.BackMasterTracker;
import com.ery.estorm.client.node.tracker.MasterTracker;
import com.ery.estorm.client.node.tracker.NodeConfigTracker;
import com.ery.estorm.client.node.tracker.SupervisorTracker;
import com.ery.estorm.client.push.PushManage;
import com.ery.estorm.client.store.StoreManage;
import com.ery.estorm.config.ConfigListenThread.ConfigDBTracker;
import com.ery.estorm.config.ConfigListenThread.ConfigInfo;
import com.ery.estorm.config.Configuration;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.NullAbortable;
import com.ery.estorm.join.JoinListTracker;
import com.ery.estorm.log.LogMag;
import com.ery.estorm.log.LogMag.LogWriter;
import com.ery.estorm.log.NodeLog;
import com.ery.estorm.log.NodeLog.NodeMsgLogPo;
import com.ery.estorm.log.NodeLog.PushLogPo;
import com.ery.estorm.socket.Connection;
import com.ery.estorm.socket.OrderHeader;
import com.ery.estorm.util.DNS;
import com.ery.estorm.util.Strings;
import com.ery.estorm.zk.ZooKeeperWatcher;

/**
 * <pre>
 * storm top任务节点配置信息
 * @author .hans
 * 
 */
public class StormNodeConfig {
	private static final Log LOG = LogFactory.getLog(StormNodeConfig.class);
	public static Map<ServerInfo, Connection> conns = new HashMap<ServerInfo, Connection>();
	public static Configuration conf;
	public static ZooKeeperWatcher zkw = null;// 不能序列化
	public static MasterTracker masterTracker = null;// 监听Master变化
	public static boolean inited = false;
	public static StormNodeListenOrder listen;
	public static NodeConfigTracker nodeConfig = null;// 监听配置变化及命令
	public static SpoutQuery spoutQuery = null;
	public static String hostName = "";
	public static ServerInfo localhost = null;
	public static SupervisorTracker Supervisor;
	public static BackMasterTracker backMasters;
	public static ConfigDBTracker confDbTracker;
	public static LogWriter nodeMsgLogWriter = null;
	public static LogWriter nodePushLogWriter = null;
	public static boolean isNode = false;
	public static JoinListTracker joinListTracker = null;
	public static PushManage pm = null;
	public static StoreManage sm = null;

	// 定时传回管理服务器端,服务端可以针对特定TOP传输命令到特定客户端,使用对Storm的分配信息解析
	// public static List<String> StormIds = new ArrayList<String>();

	public static synchronized void init(Configuration conf, int port) {
		// if (EStormConstant.DebugType > 0 && (EStormConstant.DebugType & 1) !=
		// 1)
		Utils.sleep(10000);
		if (inited && zkw != null && masterTracker != null && confDbTracker != null && nodeConfig != null)
			return;
		isNode = true;
		NodeLog.init(conf);
		StormNodeConfig.conf = conf;
		NullAbortable nullServer = new NullAbortable();
		try {
			zkw = new ZooKeeperWatcher(conf, "test master ", nullServer);
			Supervisor = new SupervisorTracker(zkw);
			confDbTracker = new ConfigDBTracker(zkw, new NullAbortable());
			NodeColumnCalc.start(confDbTracker);
			hostName = Strings.domainNamePointerToHostName(DNS.getDefaultHost(
					conf.get("estorm.master.dns.interface", "default"),
					conf.get("estorm.master.dns.nameserver", "default")));
			if ((EStormConstant.DebugType & 1) == 1) {
				hostName = "wxdb01";
				port = 6800;
			}
			Supervisor.start();
			for (String hostCode : Supervisor.supervisors.keySet()) {
				SupervisorInfo sinfo = Supervisor.supervisors.get(hostCode);
				if (sinfo.get_hostname().equals(hostName)) {
					localhost = getServerName(sinfo, port);
					if (localhost != null) {
						OrderHeader.hostName = localhost;
						break;
					}
				}
			}
			if (localhost == null) {
				OrderHeader.hostName = localhost = new ServerInfo(hostName, 0, System.currentTimeMillis());
			}
		} catch (IOException e) {
			LOG.error("init zookeeper connection fail", e);
		} catch (KeeperException e) {
			LOG.error("init zookeeper connection fail", e);
		}
		if (localhost == null) {
			LOG.equals("获取Storm运行Supervisor主机失败");
		}
		LogMag.setLogFlushInterval(conf.getInt(EStormConstant.NODE_LOG_FLUSH_SIZE, 10000));
		LogMag.setLogFlushSize(conf.getInt(EStormConstant.NODE_LOG_FLUSH_INTERVAL, 600) * 1000);
		nodeConfig = new NodeConfigTracker(zkw);
		masterTracker = new MasterTracker(zkw);
		backMasters = new BackMasterTracker(zkw);
		int maxThreads = conf.getInt(EStormConstant.ESTORM_WORKER_ORDER_EXEC_THREADS, 10);
		nodeMsgLogWriter = new LogWriter(nullServer, LogMag.nodeMsgLogs, NodeMsgLogPo.class);
		nodePushLogWriter = new LogWriter(nullServer, LogMag.pushLogs, PushLogPo.class);
		nodeMsgLogWriter.start();
		nodePushLogWriter.start();
		joinListTracker = new JoinListTracker(zkw, nullServer);
		joinListTracker.start();
		joinListTracker.runListen();
		listen = new StormNodeListenOrder(maxThreads);

		pm = new PushManage();
		sm = new StoreManage();
		LogMag.configInfo = ConfigInfo.pasreConofig(confDbTracker.getString());
		LogMag.start(conf);
		backMasters.start();
		listen.start();
		pm.start();
		sm.start();
		inited = true;
	}

	public static ServerInfo getServerName(SupervisorInfo sinfo, int port) {
		List<Long> lsq = sinfo.get_meta();// LazySeq 端口列表
		String hostName = sinfo.get_hostname();
		long sTime = sinfo.get_time_secs();
		for (long obj : lsq) {
			int _port = (int) obj;
			if (port == _port)
				return new ServerInfo(hostName, port, sTime);
		}
		return null;
	}

	public static Connection getConnection(ServerInfo server) {
		Connection conn = conns.get(server);
		if (conn == null) {
			conn = new Connection(server, conf.getInt(EStormConstant.NODE_SOCKET_RECONNECT_SLEEP_TIME, 1000));
			conns.put(server, conn);
		}
		return conn;
	}

	public static void close(ServerInfo server) {
		Connection conn = conns.get(server);
		if (conn != null) {
			conn.close();
		}
		conns.remove(server);
	}

	public static void close(Connection conn) {
		if (conn != null) {
			conn.close();
			conns.remove(conn.server);
		}
	}

	public static void close() {
		for (ServerInfo server : conns.keySet()) {
			Connection conn = conns.get(server);
			if (conn != null) {
				conn.close();
			}
		}
		conns.clear();
	}

	// public static void addStorm(String stormId) {
	// if (!StormIds.contains(stormId)) {
	// StormIds.add(stormId);
	// Connection con = getConnection(StormNodeConfig.masterTracker.master);
	// try {
	// synchronized (con.output) {// 向主服务器报告运行的TOP
	// OrderHeader ping = OrderHeader.createPing();
	// ping.data.put("ServerName", StormNodeConfig.localhost);
	// ping.data.put("runtops", StringUtils.join(",",
	// StormNodeConfig.StormIds));
	// ping.sendHeader(con.output, con.outputBuffer);
	// }
	// } catch (IOException e) {
	// e.printStackTrace();
	// }
	// }
	// }
}
