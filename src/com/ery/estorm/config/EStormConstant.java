package com.ery.estorm.config;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.jackson.map.ObjectMapper;

import com.googlecode.aviator.AviatorEvaluator;
import com.ery.estorm.daemon.topology.NodeInfo.InOutPO;

public abstract class EStormConstant {
	public static final String RESOURCES_KEY = "rescource.configuration.files";
	public static final String CONFIG_DB_DRIVER_KEY = "config.db.driver";
	public static final String CONFIG_DB_URL_KEY = "config.db.connecturl";
	public static final String CONFIG_DB_USER_KEY = "config.db.connectuser";
	public static final String CONFIG_DB_PASS_KEY = "config.db.connectpass";
	public static final String MASTER_PORT_KEY = "estorm.master.port";
	public static final String MASTER_IPC_ADDRESS_KEY = "estorm.master.ipc.address";
	public static final String MAX_CLIENT_SIZE_KEY = "estorm.max.client.size";
	public static final String MASTER_INFO_PORT_KEY = "estorm.master.info.port";
	public static final String MASTER_INFO_BINDADDRESS_KEY = "estorm.master.info.bindAddress";

	public static final String ZK_SESSION_TIMEOUT = "zookeeper.session.timeout";
	public static final int DEFAULT_ZK_SESSION_TIMEOUT = 60000;

	public static final String ZOOKEEPER_BASE_ZNODE = "estorm.zookeeper.baseznode";// /estorm

	public static final String DEFAULT_ZOOKEEPER_ZNODE_PARENT = "/estorm";
	public static final String STORM_BASE_ZNODE = "storm.zookeeper.baseznode";// /storm
	public static final String STORM_MASTER_ZNODE = "zookeeper.znode.storm.master";
	public static final String STORM_BACK_MASTER_ZNODE = "zookeeper.znode.storm.basckmaster";
	public static final String STORM_SERVERS_ZNODE = "zookeeper.znode.storm.servers";

	public static final String STORM_TOPOLOGY_ZNODE = "zookeeper.znode.storm.topology";
	public static final String STORM_TASKNODE_ZNODE = "zookeeper.znode.storm.tasknode";
	public static final String STORM_JOINDATA_NODE_ZNODE = "zookeeper.znode.storm.joindata.znode";
	public static final String STORM_MAG_MASTER_ZNODE = "zookeeper.znode.estorm.master";
	public static final String STORM_MAG_BACKMASTER_ZNODE = "zookeeper.znode.backup.masters";
	public static final String ESTORM_CLUSTER_STATUS_ZNODE = "zookeeper.znode.cluster.status";
	public static final String STORM_MSG_KAFKA_ZNODE = "zookeeper.znode.storm.kafkanode";

	public static final String STORM_MSG_KAFKA_ROOT_ZNODE = "storm.kafka.root.znode";// /kafka
	public static final String STORM_MSG_KAFKA_STATE_UPDATE_INTERVAL = "storm.kafka.msg.state.update.interval";//
	public static final String STORM_MSG_KAFKA_FORCE_START = "storm.kafka.msg.force.start";//
	public static final String STORM_MSG_KAFKA_REFRESH_INTERVAL = "storm.kafka.msg.partition.refresh.intervalSess";//

	public static final String STORM_MSG_KAFKA_SOCKET_TIMEOUT = "storm.kafka.msg.socket.timeout";//
	public static final String STORM_MSG_PUSH_THREAD_THREADS = "storm.msg.push.exec.threads";// 10
	public static final String STORM_MSG_PUSH_RETRY_TIMES = "storm.msg.push.retry.times";// 3
	public static final String STORM_MSG_STORE_THREAD_THREADS = "storm.msg.store.exec.threads";// 10
	public static final String STORM_MSG_STORE_RETRY_TIMES = "storm.msg.store.retry.times";// 3

	public static final String NODE_LOG_FLUSH_SIZE = "estorm.node.log.flush.size";
	public static final String NODE_LOG_FLUSH_INTERVAL = "estorm.node.log.flush.interval";

	public static final String CONFIG_LISTEN_INTERVAL = "estorm.config.listen.interval";
	public static final String NODE_SOCKET_RECONNECT_SLEEP_TIME = "estorm.config.listen.interval";

	public static final String ZOOKEEPER_QUORUM = "estorm.zookeeper.quorum";
	public static final String STORM_HOME = "storm.home";
	public static final String ESTORM_HOME = "estorm.home";
	public static final String STORM_MANAGE_NIMBUS_HOST = "storm.nimbus.host";
	public static final String STORM_BOLT_TO_MQ_EXCESS = "estorm.bolt2mq.excess";
	public static final String KAFKA_MSGSERVER_ADDRESS = "kafka.msgserver.address";
	public static final String KAFKA_MSG_TAGNAME = "kafka.msg.topic";
	public static final String ZOOKEEPER_USEMULTI = "zookeeper.useMulti";
	public static final int SOCKET_RETRY_WAIT_MS = 200;

	public static final String ZK_CFG_PROPERTY_PREFIX = "hbase.zookeeper.property.";
	public static final String ZOOKEEPER_MAX_CLIENT_CNXNS = ZK_CFG_PROPERTY_PREFIX + "maxClientCnxns";

	public static final String STORM_STOP_MINWAIT_CONFIRM_MILLIS = "storm.stop.minwait.confim.millis";

	public static final String ESTORM_ASSIGNMENT_THREADS = "estorm.assignment.threads.max";
	public static final String ESTORM_ASSIGNMENT_ZKEVENT_THREADS = "estorm.assignment.zkevent.workers";
	public static final String ESTORM_WORKER_ORDER_EXEC_THREADS = "estorm.worker.order.exec.threads";

	public static final String ESTORM_BOLT_NODE_LOGID_KEY = "estorm.bolt.node.logid";
	public static final String ESTORM_JOIN_LOAD_RETRY_TIMES = "estorm.join.load.retry.times";
	public static final String ESTORM_SERVER_JOINLOAD_PROCESSES = "estorm.server.joinload.processes";
	public static final int ESTORM_SERVER_JOINLOAD_PROCESSES_DEFAULT = 4;

	public static final String ESTORM_JOINDATA_STORE_LOCALMEMORY_HEAPSIZE = "estorm.joindata.store.localmemery.heapsize";
	public static final String ESTORM_JOINDATA_STORE_TYPE = "estorm.joindata.store.type";
	public static final String ESTORM_JOINDATA_STORE_PARAMS = "estorm.joindata.store.params";
	public static final String ESTORM_JOINDATA_LOAD_UPDATEINFO_RECORDSIZE = "estorm.joindata.load.update.info.record.size";

	public static final String ESTORM_NODE_PROCDATA_CACHE_STORE_TYPE = "estorm.node.data.procedure.cache.type";
	public static final String ESTORM_NODE_PROCDATA_CACHE_STORE_PARAMS = "estorm.node.data.procedure.cache.params";
	public static final String ESTORM_DATA_RETRY_TIMES = "estorm.node.data.retry.tmes";

	public static ObjectMapper objectMapper = new ObjectMapper();
	public static java.text.SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static long utsTiime = new Date(70, 1, 1).getTime();
	public static String utsTiimeString = EStormConstant.sdf.format(new Date(70, 0, 1));

	/**
	 * <pre>
	 * 1:调试网络连接，不检查Storm进程,模拟Node网络连接
	 * 2：本地模式运行TOP
	 * 4:内置启动top
	 * </pre>
	 */
	public static int DebugType = 0;

	public static String getUserName() {
		return System.getProperty("user.name");
	}

	public static String getUserDir() {
		return System.getProperty("user.dir");
	}

	public static String getClassPath() {
		return System.getProperty("java.class.path");
	}

	public static String getHostName() {
		return System.getProperty("java.class.path");
	}

	public static <T> T castObject(byte[] data) throws IOException {
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
		ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
		try {
			T a = (T) objectInputStream.readObject();
			return a;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new IOException(e);
		} finally {
			objectInputStream.close();
			byteArrayInputStream.close();
		}
	}

	public static byte[] Serialize(Object obj) throws IOException {
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
		objectOutputStream.writeObject(obj);
		byte[] bts = byteArrayOutputStream.toByteArray();
		objectOutputStream.close();
		byteArrayOutputStream.close();
		return bts;
	}

	public static <T> T Clone(Object obj) throws IOException {
		return castObject(Serialize(obj));
	}

	/**
	 * 常规时间宏变量，使用动态表达式执行
	 * 
	 */
	public static final Pattern evalPattern = Pattern.compile("eval\\(.*\\)");

	/**
	 * 常规时间宏变量
	 * 
	 * @param str
	 * @return
	 */
	public static String macroProcess(String str) {
		Matcher m = evalPattern.matcher(str);
		while (m.find()) {
			String exp = m.group(1);
			Object obj = AviatorEvaluator.execute(exp);
			str = m.replaceFirst(obj.toString());
			m = evalPattern.matcher(str);
		}
		return str;
	}

	public static Object convertToExecEnvObj(String value) {
		try {
			if (value.indexOf('.') >= 0) {
				return Double.parseDouble(value);
			} else {
				return Long.parseLong(value);
			}
		} catch (NumberFormatException e) {
			return value;
		}
	}

	public static <T extends InOutPO> HashMap<Long, T> converToMap(List<T> list) {
		HashMap<Long, T> res = new HashMap<Long, T>();
		if (list != null)
			for (T t : list) {
				res.put(t.getNodeId(), t);
			}
		return res;

	}

	public static Object castAviatorObject(String value) {
		try {
			if (value.indexOf('.') >= 0) {
				return Double.parseDouble(value);
			} else {
				return Long.parseLong(value);
			}
		} catch (NumberFormatException e) {
			return value;
		}
	}

	public static Object castAviatorObject(byte[] bt) {
		return castAviatorObject(new String(bt));
	}

	public static boolean getAviatorBoolean(Object obj) throws IOException {
		if (obj instanceof Boolean) {
			return (Boolean) obj;
		} else if (obj instanceof Integer) {
			return ((Integer) obj > 0);
		} else if (obj instanceof Long) {
			return ((Long) obj > 0);
		} else if (obj instanceof Double) {
			return ((Double) obj > 0.000001);
		} else if (obj instanceof Float) {
			return ((Float) obj > 0.000001);
		} else if (obj instanceof String) {
			if (obj != null && !obj.toString().trim().equals(""))
				return true;
			else
				return false;
		} else {
			return obj != null;
		}
	}

	public static enum SeqType {
		NodeLogId,
	}
}
