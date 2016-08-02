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
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.ery.estorm.ServerInfo;
import com.ery.estorm.config.Configuration;
import com.ery.estorm.daemon.topology.NodeInfo;
import com.ery.estorm.monitor.StateDumpServlet;
import com.ery.estorm.monitor.TaskMonitor;
import com.ery.estorm.util.ReflectionUtils;

public class MasterDumpServlet extends StateDumpServlet {
	private static final long serialVersionUID = 1L;
	private static final String LINE = "===========================================================";

	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		DaemonMaster master = (DaemonMaster) getServletContext().getAttribute(DaemonMaster.MASTER);
		assert master != null : "No Master in context!";

		response.setContentType("text/plain");
		OutputStream os = response.getOutputStream();
		PrintWriter out = new PrintWriter(os);

		out.println("Master status for " + master.getServerName() + " as of " + new Date());

		out.println("\n\nVersion Info:");
		out.println(LINE);
		dumpVersionInfo(out);

		out.println("\n\nTasks:");
		out.println(LINE);
		TaskMonitor.get().dumpAsText(out);

		out.println("\n\nServers:");
		out.println(LINE);
		dumpServers(master, out);

		out.println("\n\nNode-info:");
		out.println(LINE);
		dumpNodeInfo(master, out);

		out.println("\n\nExecutors:");
		out.println(LINE);
		dumpExecutors(master.getExecutorService(), out);

		out.println("\n\nStacks:");
		out.println(LINE);
		ReflectionUtils.printThreadInfo(out, "");

		out.println("\n\nMaster configuration:");
		out.println(LINE);
		Configuration conf = master.getConfiguration();
		out.flush();
		conf.writeXml(os);
		os.flush();

		out.flush();
	}

	private void dumpNodeInfo(DaemonMaster master, PrintWriter out) {
		Map<Long, NodeInfo> nodeInfos = master.getAssignmentManager().assignNodes;
		for (Entry<Long, NodeInfo> e : nodeInfos.entrySet()) {
			Long ninfo = e.getKey();
			NodeInfo rs = e.getValue();
			out.println("Node " + ninfo + ": " + rs);
		}
	}

	private void dumpServers(DaemonMaster master, PrintWriter out) {
		List<ServerInfo> servers = master.getServerManager().getOnlineServers();
		for (ServerInfo e : servers) {
			out.println(e);
		}
	}
}
