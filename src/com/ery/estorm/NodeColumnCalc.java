package com.ery.estorm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import backtype.storm.utils.Utils;

import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.ery.estorm.config.ConfigListenThread.ConfigDBTracker;
import com.ery.estorm.config.ConfigListenThread.ConfigInfo;
import com.ery.base.support.jdbc.DataAccess;
import com.ery.base.support.utils.MapUtils;

public abstract class NodeColumnCalc extends AbstractFunction {
	private static Map<String, NodeColumnCalc> objCache = new HashMap<String, NodeColumnCalc>();

	public abstract Object calc(String... f2);

	public static NodeColumnCalc getNodeColumnCalc(String className) {
		return objCache.get(className);
	}

	public static NodeColumnCalc getNodeColumnCalc(String className, String code) {
		NodeColumnCalc cc = objCache.get(className);
		if (cc == null) {
			addNodeColumnCalc(className, code);
			cc = objCache.get(className);
		}
		return cc;
	}

	public synchronized static void addNodeColumnCalc(NodeColumnCalc nodeColCalc) {
		objCache.put(nodeColCalc.getClass().getCanonicalName(), nodeColCalc);
	}

	private synchronized static void addNodeColumnCalc(String className, String code) {
		JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
		JavaFileObject fileObject = new JavaStringObject(className.substring(className.lastIndexOf('.') + 1), code);
		CompilationTask task = javaCompiler.getTask(null, null, null,
				Arrays.asList("-d", ClassLoader.getSystemClassLoader().getResource("").getPath()), null,
				Arrays.asList(fileObject));
		boolean success = task.call();
		if (!success) {
			System.out.println(className + "编译失败");
		} else {
			System.out.println(className + "编译成功");
		}
		ClassLoader ccl = ClassLoader.getSystemClassLoader();
		try {
			Class classl = ccl.loadClass(className);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		// if (classl.isAssignableFrom(NodeColumnCalc.class)) {
		// objCache.put(className, (NodeColumnCalc) classl.newInstance());
		// }

	}

	private static boolean dynamicCodeLisenRun = false;

	public synchronized static void start(final ConfigDBTracker confDbTracker) {
		if (dynamicCodeLisenRun) {
			return;
		}
		new Thread(new Runnable() {
			ConfigInfo configInfo = null;
			Connection con;// 长连接
			public DataAccess access = new DataAccess();
			long MOD_TIME = 0;

			@Override
			public void run() {
				Thread.currentThread().setName("NodeColumnCalc:loadDynamicCode");

				while (confDbTracker.getString() == null) {
					Utils.sleep(1000);
				}
				configInfo = ConfigInfo.pasreConofig(confDbTracker.getString());
				if (configInfo == null) {
					dynamicCodeLisenRun = false;
					return;
				}
				while (!confDbTracker.abortable.isAborted()) {
					try {
						ConfigInfo newConfig = ConfigInfo.pasreConofig(confDbTracker.getString());
						if (!configInfo.equals(newConfig) && newConfig != null) {
							close();
							configInfo = newConfig;
						}
						open();
						loadDynamicCode();
						Utils.sleep(10000);
					} catch (Exception e) {
						if (e instanceof com.ery.base.support.jdbc.JdbcException) {
							synchronized (access) {
								close();
							}
						}
					}
				}
			}

			void loadDynamicCode() {
				String sql = "SELECT CLASS_NAME,CLASS_CODE,MOD_TIME FROM ST_DYNAMIC_CODE WHERE STATE=1 and MOD_TIME>? order by MOD_TIME";
				Map<String, Object>[] rows = access.queryForArrayMap(sql, MOD_TIME);
				for (Map<String, Object> row : rows) {
					if (row.get("CLASS_NAME") == null || row.get("CLASS_CODE") == null)
						continue;
					String className = row.get("CLASS_NAME").toString();
					String classCode = row.get("CLASS_CODE").toString();
					if (className.trim().equals("") || classCode.trim().equals(""))
						continue;
					long modTime = MapUtils.getLongValue(row, "MOD_TIME", 0);
					if (MOD_TIME < modTime)
						MOD_TIME = modTime;
					addNodeColumnCalc(className, classCode);
				}
			}

			void open() throws SQLException {
				if (con == null) {
					con = DriverManager.getConnection(configInfo.url, configInfo.user, configInfo.password);
					access.setConnection(con);
				}
			}

			void close() {
				try {
					if (con != null)
						con.close();
				} catch (SQLException e1) {
				}
				con = null;
			}

		}).start();
	}
}
