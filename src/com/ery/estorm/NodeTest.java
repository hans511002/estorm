package com.ery.estorm;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.SupervisorSummary;
import backtype.storm.utils.Utils;

import com.googlecode.aviator.AviatorEvaluator;
import com.ery.estorm.client.node.StormNodeConfig;
import com.ery.estorm.config.Configuration;
import com.ery.estorm.config.EStormConfiguration;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.NullAbortable;
import com.ery.estorm.daemon.topology.TopologyInfo;
import com.ery.estorm.exceptions.ZooKeeperConnectionException;
import com.ery.estorm.util.HasThread;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperWatcher;

public class NodeTest extends HasThread {
	private static final Log LOG = LogFactory.getLog(NodeTest.class.getName());
	TopologyInfo top;

	public NodeTest(TopologyInfo top) {
		this.top = top;
	}

	@Override
	public void run() {
		Config stormConf = new Config();
		LocalCluster cluster = new LocalCluster();
		List<SupervisorSummary> a = cluster.getClusterInfo().get_supervisors();
		for (SupervisorSummary s : a) {
			System.out.println(s.get_host() + " workers: " + s.get_num_workers() + "");
		}
		cluster.submitTopology(top.getTopName().topName, stormConf, top.st);
		while (true) {
			Utils.sleep(100000);
		}

	}

	static NodeTest nodeRun;

	public static int runTop(String path) {
		NullAbortable nullServer = new NullAbortable();
		Configuration conf = EStormConfiguration.create();
		try {
			ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "submit_topology", nullServer, false);
			LOG.info("========ZooKeeperWatcher=============path=" + path);
			byte data[] = ZKUtil.getData(zkw, path);
			LOG.info("========ZKUtil.getData==========data====" + data == null ? "null" : data.length);
			if (data != null) {
				TopologyInfo top = EStormConstant.castObject(data);
				LOG.info("本地线程模拟TOP:" + top.getTopName() + " path=" + path);
				Utils.sleep(10000);
				ZKUtil.setData(zkw, path, data);

				nodeRun = new NodeTest(top);
				nodeRun.start();
				return (0);
			} else {
				return (1);// 重新写ZK
			}
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace(System.out);
		} catch (IOException e) {
			e.printStackTrace(System.out);
		} catch (KeeperException e) {
			e.printStackTrace(System.out);
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
		return (4);// 重试

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, NoSuchMethodException, SecurityException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException, InstantiationException, InterruptedException {
		List<String> colMacs = AviatorEvaluator.compile("'ddf'", true).getVariableNames();

		System.err.println(colMacs.size());
		if (colMacs != null)
			return;
		Configuration conf = EStormConfiguration.create();
		LOG.info("启动测试进程");
		if ((EStormConstant.DebugType & 2) != 2) {
			StormNodeConfig.init(conf, 6800);
			while (true) {
				Utils.sleep(100000);
			}
		} else {
			NodeTest.runTop(args[0]);
			if (nodeRun != null)
				nodeRun.join();
		}
	}

}
