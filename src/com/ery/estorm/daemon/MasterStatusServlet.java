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
import java.io.PrintWriter;
import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ery.estorm.ServerInfo;
import com.ery.estorm.config.Configuration;
import com.ery.estorm.config.EStormConstant;
import com.ery.estorm.daemon.stormcluster.StormServerManage;
import com.ery.estorm.daemon.topology.TopologyInfo;
import com.ery.estorm.daemon.topology.TopologyInfo.TopologyName;
import com.ery.estorm.util.ToolUtil;

/**
 * The servlet responsible for rendering the index page of the master.
 */
public class MasterStatusServlet extends HttpServlet {
	private static final Log LOG = LogFactory.getLog(MasterStatusServlet.class);
	private static final long serialVersionUID = 1L;

	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		DaemonMaster master = (DaemonMaster) getServletContext().getAttribute(DaemonMaster.MASTER);
		assert master != null : "No Master in context!";

		Configuration conf = master.getConfiguration();
		List<ServerInfo> servers = master.getBackMasterTracker().getOnlineServers();// backMasters
		List<ServerInfo> deadServs = master.getServerManager().getDeadServers();

		Map<String, TopologyInfo> onlineTopologys = master.getAssignmentManager().assignTops;
		Map<String, TopologyName> runTopNames = master.getAssignmentManager().runTopNames;
		StormServerManage stormServer = master.getStormServer();

		if (master.isInitialized()) {
			if (master.getServerManager() == null) {
				response.sendError(503, "Master not ready");
				return;
			}
		}
		if (!master.isActiveMaster()) {
			// 跳转
			master.getActiveMaster();
		}
		AssignmentManager assign = master.getAssignmentManager();
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		String order = request.getParameter("order");
		String topName = request.getParameter("top");
		if (order == null) {

		} else {
			if (order.equals("activate")) {
			} else if (order.equals("deactivate")) {
			} else if (order.equals("rebalance")) {
			} else if (order.equals("stop")) {
				TopologyName runTopName = TopologyName.parseServerName(topName);
				// TopologyName assTopName = tops.get(runTopName.topName);
				TopologyInfo tpl = onlineTopologys.get(runTopName.topName);
				TopologyName assTopName = tpl.assignTopName;
				String waitSec = request.getParameter("waitSec");
				int w = 10;
				if (waitSec != null) {
					w = (int) ToolUtil.toLong(waitSec);
					if (w < 1)
						w = 5;
				}
				assign.StopTopologyNodes(tpl, w);
				out.flush();
				out.close();
				return;
			}
		}

		out.println("<html>");
		out.println("<head>");
		out.println("<script type='text/javascript'> ");
		out.println("function ensureInt(n) {\n var isInt = /^\\d+$/.test(n);"
				+ "if (!isInt) {\n alert(\"'\" + n + \"' is not integer.\");}\n  return isInt;\n}");

		out.println("function stop(topName){\n" +
				"var waitSecs = prompt('Do you really want to stop topology \"' + topName + '\"? If yes, please, specify wait time in seconds:',30);\n" +
				" if (waitSecs != null && waitSecs != '' && ensureInt(waitSecs)) {\n" + "window.open('" +
				request.getServletPath()
				// request.getContextPath()
				+ "?order=stop&top='+topName,'');   " + "\n} else {" + " \nreturn false;\n}" + "\n}");

		out.println("</script>");

		out.println("</head>");
		out.println("<body>");
		out.println("<center>EStorm Master Info</center><br/>");
		out.println("<hr/>");
		out.println("Active Master<br/>");
		out.println(master.getActiveMaster());
		out.println("<br/>");
		out.println("<hr/>");
		out.println("Back Masters<br/>");
		for (ServerInfo serv : servers) {
			out.println(serv);
			out.println("=================servStatus=");
			out.println("<br/>");
		}

		out.println("<hr/>");
		out.println("dead Masters<br/>");
		if (deadServs.size() > 0) {
			for (ServerInfo serv : deadServs) {
				out.println(serv);
				out.println("<br/>");
			}
		}

		out.println("<hr/>");
		out.println("online Topologys<br/>");
		if (onlineTopologys.size() > 0) {
			out.println("TopName &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; "
					+ "StartTime &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; "
					+ "RunningTopName &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; Control");
			out.println("<br/>");
			for (String tppName : onlineTopologys.keySet()) {
				TopologyName tn = runTopNames.get(tppName);
				out.println(tn.topName);
				out.println("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;");
				out.println(EStormConstant.sdf.format(new Date(tn.stime)));
				out.println("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; ");
				if (stormServer.stormsTracker.stormInfos.containsKey(tn.topName)) {
					if (stormServer.mInfo.uiPort != 0) {
						out.println("<a href='http://" + stormServer.mInfo.hostName + ":" + stormServer.mInfo.uiPort +
								"/topology/" + stormServer.stormsTracker.stormInfos.get(tn.topName) + "'");
						out.println(">");
						out.println(stormServer.stormsTracker.stormInfos.get(tn.topName));
						out.println("</a>");
					} else {
						out.println(stormServer.stormsTracker.stormInfos.get(tn.topName));
					}
					out.println("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; ");
					out.println("<input type='button' value ='activate' /> ");
					out.println("&nbsp;&nbsp;&nbsp;");
					out.println("<input type='button' value ='deactivate' /> ");
					out.println("&nbsp;&nbsp;&nbsp;");
					out.println("<input type='button' value ='rebalance' /> ");
					out.println("&nbsp;&nbsp;&nbsp;");
					out.println("<input type='button' value ='stop' onclick=\"javascript:stop('" +
							stormServer.stormsTracker.stormInfos.get(tn.topName) + "')\"/> ");
				} else {
					out.println("not online");
					out.println("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; ");
					out.println("");
				}
				out.println("<br/>");
			}
		}

		if (runTopNames.size() > 0) {
			out.println("<hr/>");
			out.println("online Topologys<br/>");
			for (String tn : runTopNames.keySet()) {
				out.println(tn);
				out.println("&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;time:");
				out.println(runTopNames.get(tn).stime);
				out.println("<br/>");
			}
		}

		out.println("<hr/>");
		out.println("assign infos<br/>");
		if (master.isActiveMaster()) {
			out.println("<P>assignTops=" + assign.assignTops);
			out.println("<P/>");
			// out.println("<P>assignNodes=" + assign.assignNodes);
			out.println("<P/>");
			// out.println("<P>assignMQs=" + assign.assignMQs);
			out.println("<P/>");
		}
		out.println("</body></html>");
		out.flush();
		out.close();
	}
}
