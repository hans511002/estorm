package com.ery.estorm.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;

import com.ery.estorm.config.Configuration;
import com.ery.estorm.config.EStormConfiguration;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.NullAbortable;
import com.ery.estorm.daemon.topology.NodeInfo;
import com.ery.estorm.daemon.topology.NodeInfo.NodePO;
import com.ery.estorm.daemon.topology.TopologyInfo;
import com.ery.estorm.exceptions.ZooKeeperConnectionException;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperWatcher;

public class TopologySubmit {

	public static int runTop(TopologyInfo top) {
		try {
			Config topConf = new Config();
			topConf.setNumWorkers(top.workerNum);

			List<NodeInfo> spouts = new ArrayList<NodeInfo>();
			List<NodeInfo> bolts = new ArrayList<NodeInfo>();
			TopologyInfo.deserializeNodeInfos(top.st, spouts, bolts);

			for (NodeInfo ndpo : bolts) {
				NodePO npo = (NodePO) ndpo.node;
				if (npo.NODE_PARAMS != null && !npo.NODE_PARAMS.equals("")) {
					String[] pars = npo.NODE_PARAMS.split("\n|\r");
					for (String par : pars) {
						if (par == null || par.length() == 0)
							continue;
						String tmp[] = par.split("=");
						if (topConf.containsKey(tmp[0])) {
							Object obj = topConf.get(tmp[0]);
							try {
								long lv = Long.parseLong(obj.toString());
								long _lv = Long.parseLong(par.substring(tmp[0].length() + 1));
								if (_lv > lv) {
									topConf.put(tmp[0], _lv);
								}
							} catch (Exception e) {
								String sv = obj.toString();
								if (par.substring(tmp[0].length() + 1).compareToIgnoreCase(sv) > 0) {
									topConf.put(tmp[0], par.substring(tmp[0].length() + 1));
								}
							}
						} else {
							topConf.put(tmp[0], par.substring(tmp[0].length() + 1));
						}
					}
				}
			}
			// String estormHome = conf.get(EStormConstant.ESTORM_HOME,
			// System.getenv("ESTORM_HOME"));
			// topConf.put("topology.worker.childopts",
			// topConf.get("topology.worker.childopts") + " -classpath " +
			// estormHome
			// + "/estorm-exec.jar!/conf ");
			topConf.setDebug(true);
			System.out.println("======================start " + top.assignTopName.topName);
			StormSubmitter.submitTopology(top.assignTopName.topName, topConf, top.st);
			return 0;
		} catch (AlreadyAliveException e) {
			e.printStackTrace(System.out);
			return (2);// 不再重试
		} catch (InvalidTopologyException e) {
			e.printStackTrace(System.out);
			return (3);// 不再重试
		} catch (Exception e) {
			e.printStackTrace(System.out);
			return 4;
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args == null || args.length == 0) {
			System.out.println("无效参数");
			return;
		}
		String path = args[0];
		NullAbortable nullServer = new NullAbortable();
		Configuration conf = EStormConfiguration.create();
		int res = 1;
		try {
			System.out.println("========ZooKeeperWatcher===estorm.zookeeper.quorum=" +
					conf.get(EStormConstant.ZOOKEEPER_QUORUM));
			ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "submit_topology", nullServer, false);
			System.out.println("========ZooKeeperWatcher=============path=" + path);
			byte data[] = ZKUtil.getData(zkw, path);
			System.out.println("========ZKUtil.getData==========data====" + data == null ? "null" : data.length);
			if (data != null) {
				TopologyInfo top = EStormConstant.castObject(data);
				res = runTop(top);
			}
			System.exit(res);
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace(System.out);
		} catch (IOException e) {
			e.printStackTrace(System.out);
		} catch (KeeperException e) {
			e.printStackTrace(System.out);
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
		System.exit(5);// 重试
	}
}
