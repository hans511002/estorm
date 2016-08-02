/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ery.estorm.daemon;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;

import backtype.storm.utils.Utils;
import clojure.lang.PersistentArrayMap;

import com.ery.estorm.ServerInfo;
import com.ery.estorm.cluster.tracker.ClusterStatusTracker;
import com.ery.estorm.config.Configuration;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.util.DNS;
import com.ery.estorm.util.ServerCommandLine;
import com.ery.estorm.util.StringUtils;
import com.ery.estorm.util.Strings;
import com.ery.estorm.zk.ZKUtil;
import com.ery.estorm.zk.ZooKeeperWatcher;

public class EMasterCommandLine extends ServerCommandLine {
	private static final Log LOG = LogFactory.getLog(EMasterCommandLine.class);

	private static final String USAGE = "Usage: Master start|stop|clear\n start  Start Master. \n"
			+ " stop   Shutdown cluster .\n clear  Delete the master znode in ZooKeeper after a master crashes\n";

	private final Class<? extends DaemonMaster> masterClass;

	public EMasterCommandLine(Class<? extends DaemonMaster> masterClass) {
		this.masterClass = masterClass;
	}

	protected String getUsage() {
		return USAGE;
	}

	public int run(String args[]) throws Exception {
		Options opt = new Options();// 参数解析 --param
		opt.addOption("debug", true, "debug type");// EStormConstant.DebugType
		CommandLine cmd;
		try {
			cmd = new GnuParser().parse(opt, args);
		} catch (ParseException e) {
			LOG.error("Could not parse: ", e);
			usage(null);
			return 1;
		}
		// if (test() > 0)
		// return 1;
		String debug = cmd.getOptionValue("debug");
		if (debug != null) {
			EStormConstant.DebugType = Integer.parseInt(debug);
		}
		List<String> remainingArgs = cmd.getArgList();
		if (remainingArgs.size() < 1) {
			usage(null);
			return 1;
		}

		String command = remainingArgs.get(0);

		if ("start".equals(command)) {
			return startMaster();
		} else if ("stop".equals(command)) {
			return stopMaster();
		} else if ("stopCluster".equals(command)) {
			return stopCluster();
		} else {
			usage("Invalid command: " + command);
			return 1;
		}
	}

	public static ServerInfo getMasterServerName(Configuration conf) throws IOException {
		String hostname = Strings
				.domainNamePointerToHostName(DNS.getDefaultHost(conf.get("estorm.master.dns.interface", "default"),
						conf.get("estorm.master.dns.nameserver", "default")));
		int _listenPort = conf.getInt(EStormConstant.MASTER_PORT_KEY, 3000);
		return new ServerInfo(hostname, _listenPort, System.currentTimeMillis());
	}

	private int startMaster() {
		Configuration conf = getConf();
		try {
			logProcessInfo(getConf());
			DaemonMaster master = DaemonMaster.constructMaster(masterClass, conf);
			if (master.isStopped()) {
				LOG.info("Won't bring the Master up as a shutdown is requested");
				return 1;
			}
			master.start();
			master.join();
			if (master.isAborted())
				throw new RuntimeException("Estorm Master Aborted");
		} catch (Throwable t) {
			LOG.error("Estorm Master exiting", t);
			return 1;
		}
		return 0;
	}

	// 停止集群
	private int stopCluster() {
		ZooKeeperWatcher zkw;
		try {
			zkw = new ZooKeeperWatcher(getConf(), "stop Cluster", new Abortable() {
				@Override
				public void abort(String why, Throwable e) {
				}

				@Override
				public boolean isAborted() {
					return false;
				}
			});
			ClusterStatusTracker clusterStatusTracker = new ClusterStatusTracker(zkw, null);
			clusterStatusTracker.setClusterDown();
		} catch (IOException e) {
			LOG.error("Can't connect to zookeeper to read the master znode", e);
			return 1;
		} catch (KeeperException e) {
			LOG.error("delete master znode", e);
			return 1;
		}
		return 0;
	}

	// 停止本机
	private int stopMaster() {
		ZooKeeperWatcher zkw;
		try {
			zkw = new ZooKeeperWatcher(getConf(), "stop master", new Abortable() {
				@Override
				public void abort(String why, Throwable e) {
				}

				@Override
				public boolean isAborted() {
					return false;
				}
			});
			ClusterStatusTracker clusterStatusTracker = new ClusterStatusTracker(zkw, null);
			clusterStatusTracker.setClusterDown();
		} catch (IOException e) {
			LOG.error("Can't connect to zookeeper to read the master znode", e);
			return 1;
		} catch (KeeperException e) {
			LOG.error("delete master znode", e);
			return 1;
		}
		return 0;
	}

	public static void main(String[] args) {
		StringUtils.startupShutdownMessage(DaemonMaster.class, args, LOG);
		new EMasterCommandLine(DaemonMaster.class).doMain(args);
	}

	private void printPersistentArrayMap(PersistentArrayMap map, int level) {
		String identStr = "";
		for (int i = 0; i < level; i++) {
			identStr += "\t";
		}
		for (Object obj : map.keySet()) {
			Object val = map.get(obj);
			if (val instanceof PersistentArrayMap) {
				System.out.println(identStr + "key=" + obj + " value={PersistentArrayMap}");
				PersistentArrayMap vm = (PersistentArrayMap) val;
				printPersistentArrayMap(vm, level + 1);
			} else {
				System.out.println(identStr + "key=" + obj + " value=" + val);
			}
		}
	}

	class sgd extends Thread {
		ServerInfo sn;

		public sgd(ServerInfo sn) {
			this.sn = sn;
		}

		@Override
		public void run() {
			while (true) {
				System.out.println(sn.getHostAndPort());
				Utils.sleep(1000);
			}

		}
	}

	private int test() {
		ZooKeeperWatcher zkw;
		try {
			Map<String, Object> res = new HashMap<String, Object>();
			System.out.println(EStormConstant.objectMapper.writeValueAsString(res));
			res.put("ss", null);
			System.out.println(EStormConstant.objectMapper.writeValueAsString(res));
			String ex = EStormConstant.objectMapper.writeValueAsString(new Exception());

			ServerInfo sn = new ServerInfo("hadoop", 2, 1);
			new Thread(new sgd(sn)).start();

			Utils.sleep(2000);
			sn = new ServerInfo("a", 2, 1);
			Utils.sleep(2000);
			// sn=null;

			Exception e = (Exception) EStormConstant.objectMapper.readValue(ex, Exception.class);
			e.printStackTrace();
			System.err.println(Strings.domainNamePointerToHostName(DNS.getDefaultHost("default", "default")));
			if (e != null)
				return 0;
			NullAbortable nullAbt = new NullAbortable();
			zkw = new ZooKeeperWatcher(getConf(), "test master", new NullAbortable());
			// NodeName nn = new NodeName("a");
			// System.out.println(nn.hashCode());
			// nn.nameName = "b";
			// System.out.println(ToolUtil.serialObject(nullAbt));
			//
			// if (nn != null)
			// return 0;
			// byte[] data = ZKUtil.getData(zkw,
			// "/storm0.9/supervisors/33169ab4-4831-4079-8e24-6dd9f36ef9a4");//
			// StormBase
			byte[] data = ZKUtil.getData(zkw,
					"/storm0.9/workerbeats/sspout-6-1390278342/11cbb075-19e9-469e-8f4b-2db9480bf19f-6801");
			System.err.println(new String(data));
			// byte[] data = ZKUtil.getData(zkw,
			// "/storm0.9/supervisors/c08e9d5b-a5a5-4b17-8b25-3f6456bb82b7");//SupervisorInfo
			// ByteArrayInputStream byteArrayInputStream = new
			// ByteArrayInputStream(data);
			// ObjectInputStream objectInputStream = new
			// ObjectInputStream(byteArrayInputStream);
			try {

				// backtype.storm.daemon.common.SupervisorInfo sinfo =
				// EStormConstant.castObject(data);
				//
				// System.err.println(sinfo.hostname);
				// System.err.println(sinfo.time_secs);// 启动时间
				// System.err.println(sinfo.uptime_secs);// 启动时长
				// LazySeq lsq = (LazySeq) sinfo.meta;// LazySeq 端口列表
				// System.err.println(lsq);
				// System.err.println(sinfo.used_ports);
				// System.err.println("=======meta=====");
				// for (Object obj : lsq) {
				// System.err.println(obj);
				// }
				// System.err.println("======end=meta=====");
				//
				// System.err.println(sinfo.scheduler_meta);
				// System.err.println(sinfo.__meta);
				// System.err.println(sinfo.__extmap);
				// if (sinfo != null)
				// return 0;
				// backtype.storm.daemon.common.WorkerHeartbeat
				PersistentArrayMap hw = EStormConstant.castObject(data);
				// hw = EStormConstant.castObject(data);
				printPersistentArrayMap(hw, 0);
				//
				// SupervisorInfo supervisorInfo = (SupervisorInfo)
				// objectInputStream.readObject();
				// System.err.println(supervisorInfo.hostname);
				// System.err.println(supervisorInfo.time_secs);// 启动时间
				// System.err.println(supervisorInfo.uptime_secs);// 启动时长
				// LazySeq lsq = (LazySeq) supervisorInfo.meta;// LazySeq 端口列表
				// System.err.println(lsq);
				// System.err.println("=======meta=====");
				// for (Object obj : lsq) {
				// System.err.println(obj);
				// }
				// System.err.println("======end=meta=====");
				//
				// System.err.println(supervisorInfo.scheduler_meta);
				// System.err.println(supervisorInfo.__meta);
				// System.err.println(supervisorInfo.__extmap);

				// System.err.println(supervisorInfo.toString());

			} finally {
			}

		} catch (IOException e) {
			LOG.error("Can't connect to zookeeper to read the master znode", e);
		} catch (KeeperException e) {
			LOG.error("delete master znode", e);
		}
		return 1;
	}
}
