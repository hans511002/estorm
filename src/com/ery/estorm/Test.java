package com.ery.estorm;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.ery.estorm.exceptions.ConfigException;
import com.ery.estorm.handler.TopologySubmit;
import com.ery.estorm.util.ToolUtil;
import com.ery.estorm.zk.ZooKeeperServer;

public class Test {

	public Test() {
		// TODO Auto-generated constructor stub
	}

	public static class ExclaimBolt implements IBasicBolt {
		@Override
		public void prepare(Map conf, TopologyContext context) {
		}

		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String input = tuple.getString(1);
			collector.emit(new Values(tuple.getValue(0), input + "!"));
		}

		public void cleanup() {
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "result"));
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			// TODO Auto-generated method stub
			return null;
		}

	}

	// public static void main(String[] args) throws IOException {
	// // 编译程序
	// JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
	// int result = javaCompiler.run(null, null, null, "-d","./temp/","./temp/com/Hello.java");
	// System.out.println( result == 0 ? "恭喜编译成功" : "对不起编译失败");
	//
	// // 运行程序
	// Runtime run = Runtime.getRuntime();
	// Process process = run.exec("java -cp ./temp temp/com/Hello");
	// InputStream in = process.getInputStream();
	// BufferedReader reader = new BufferedReader(new InputStreamReader(in));
	// String info = "";
	// while ((info = reader.readLine()) != null) {
	// System.out.println(info);
	//
	// }
	// }

	// public static void main(String[] args) throws IOException{
	// // 1.创建需要动态编译的代码字符串
	// String nr = "\r\n"; //回车
	// String source = "package temp.com; " + nr +
	// " public class  Hello{" + nr +
	// " public static void main (String[] args){" + nr +
	// " System.out.println(\"HelloWorld! 1\");" + nr +
	// " }" + nr +
	// " }";
	// // 2.将欲动态编译的代码写入文件中 1.创建临时目录 2.写入临时文件目录
	// File dir = new File(System.getProperty("user.dir") + "/temp"); //临时目录
	// // 如果 \temp 不存在 就创建
	// if (!dir.exists()) {
	// dir.mkdir();
	// }
	// FileWriter writer = new FileWriter(new File(dir,"Hello.java"));
	// writer.write(source);
	// writer.flush();
	// writer.close();
	//
	// // 3.取得当前系统的编译器
	// JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
	// // 4.获取一个文件管理器
	// StandardJavaFileManager javaFileManager = javaCompiler.getStandardFileManager(null, null, null);
	// // 5.文件管理器根与文件连接起来
	// Iterable it = javaFileManager.getJavaFileObjects(new File(dir,"Hello.java"));
	// // 6.创建编译任务
	// CompilationTask task = javaCompiler.getTask(null, javaFileManager, null, Arrays.asList("-d", "./temp"), null, it);
	// // 7.执行编译
	// task.call();
	// javaFileManager.close();
	//
	// // 8.运行程序
	// Runtime run = Runtime.getRuntime();
	// Process process = run.exec("java -cp ./temp temp/com/Hello");
	// InputStream in = process.getInputStream();
	// BufferedReader reader = new BufferedReader(new InputStreamReader(in));
	// String info = "";
	// while ((info = reader.readLine()) != null) {
	// System.out.println(info);
	//
	// }
	// }
	public static class af {
		int b = 2;

		public int getB() {
			return b;
		}

		public void setB(int b) {
			this.b = b;
		}

	}

	public static class tt implements Serializable {
		int[] aa = new int[3];

		public String toString() {
			String a = "";
			for (int i = 0; i < aa.length; i++) {
				a += "aa[" + i + "]=" + aa[i] + "\n";
			}
			return a;
		}
	}

	static Pattern DbUrlPattern = Pattern.compile("([\\w_]+)/([\\w_]+)@([\\w\\.:]+)");

	static class aaaback implements VoidCallback {
		public void processResult(int rc, String path, Object ctx) {
			System.err.println("aaaback=" + rc + "   " + path + " param:" + ctx);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, NoSuchMethodException, SecurityException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException, InstantiationException, KeeperException,
			InterruptedException {

		ZooKeeperServer zk = new ZooKeeperServer("hadoop01:2181,hadoop02:2181,hadoop03:2181,hadoop04:2181,hadoop05:2181", 10000);
		ZooKeeperServer zks = new ZooKeeperServer("hadoop01:2181,hadoop02:2181,hadoop03:2181,hadoop04:2181,hadoop05:2181", 10000);

		org.apache.zookeeper.data.Stat stat = null;
		stat = zk.exists("/storm0.9/storms/top_1-1-1398336431", true);
		System.err.println(stat);
		stat = zk.exists("/storm0.9/assignments/top_1-1-1398336431", true);
		System.err.println(stat);
		zks.exists("/estorm/etest", true);
		zk.createPresistentNode("/estorm/ptest", "");
		zk.createTempNode("/estorm/etest", "");
		stat = zk.exists("/estorm/ptest", true);
		System.err.println(stat);
		stat = zk.exists("/estorm/etest", true);
		System.err.println(stat);
		zk.setData("/estorm/configdb/configTime", "2014-03-20 16:05:23");
		stat = zk.exists("/estorm/etest", true);
		zk.delete("/estorm/ptest");
		zk.delete("/estorm/etest", -1, new aaaback(), "a");
		if (zk.exists("/estorm/assignment", true) != null) {
			zk.setData("/estorm/assignment", "stop top_1");
			// zk.setData("/estorm/assignment", "stop top_2");
		} else {
			zk.createPresistentNode("/estorm/assignment", "stop top_1");

		}
		zk.close();

		if (zk != null) {
			Utils.sleep(10000);
			return;
		}
		java.text.SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String utsTiimeString = sdf.format(new Date(70, 0, 1));
		System.err.println(utsTiimeString);
		System.err.println(TopologySubmit.class.getCanonicalName());

		System.err.println(System.getProperty("spark.local.dir"));
		System.err.println(ToolUtil.toDouble("2353.64364a"));
		System.err.println(ToolUtil.toLong("2353.64364a"));
		if (utsTiimeString != null)
			return;
		Matcher m = DbUrlPattern.matcher("us_er/_pass@192.179.124.25:1521:ora10");
		if (m.find()) {
			System.err.println(m.group(1));
			System.err.println(m.group(2));
			System.err.println(m.group(3));
		} else {
			throw new ConfigException("数据库连接串格式不正确:" + m.group());
		}

		Map<String, Object> env = new HashMap<String, Object>();
		long l = System.nanoTime();
		env.put("d", 2);
		env.put("g", 6);
		env.put("EXP", "{a}>=1");
		env.put("dg", 6);
		env.put("ORI_CHARGE", 300);
		env.put("email", "zoubfn034yt494tyg@tyd.ic.com");
		System.err.println("==================" + AviatorEvaluator.execute("EXP+','+ORI_CHARGE", env));
		// AviatorEvaluator.setOptimize(value)
		Object val = null;
		for (int i = 0; i < 100000000; i++) {
			env.put("i", i);
			val = AviatorEvaluator.execute("email=~/([\\w0-8]+)@\\w+[\\.\\w+]+/ ? $1:'unknow'", env);
			// Expression exp = AviatorEvaluator.compile("email=~/([\\w0-8]+)@\\w+[\\.\\w+]+/ ? $1:i", true);
			// val = exp.execute(env);
			if (i % 1000000 == 0) {
				System.out.println(System.nanoTime() - l + " " + i + " " + val);
			}
		}
		System.out.println(System.nanoTime() - l + "          ");
		// String result = (String) AviatorEvaluator.execute(
		// " '[foo i='+ foo.i + ' f='+foo.f+' year='+(foo.date.year+1900)+ ' month='+foo.date.month +']' ", env);
		// System.out.println(result);

		env = new HashMap<String, Object>();
		env.put("EXP", "{a}>=1");
		env.put("DURATION", 1);

		env.put("a", new af());
		env.put("ORI_CHARGE", 300);
		System.out.println("sdadgd_{asgfdas}_ass{saf}_s".replaceAll("\\{|\\}", ""));
		AviatorEvaluator.setOptimize(AviatorEvaluator.EVAL);
		// Expression exp = AviatorEvaluator.compile("email=~/([\\w0-8]+)@\\w+[\\.\\w+]+/ ? $1:'unknow'");
		Expression exp = AviatorEvaluator.compile("'a.b'");
		// exp = AviatorEvaluator.compile("long(date_to_string(sysdate(),'yyyyMMdd'))-1+a.b");
		System.err.println(exp.getVariableNames());
		System.out.println("coom");
		System.out.println(exp.execute(env));
		System.out.println(exp.toString());
		long lt = System.currentTimeMillis();
		if (System.currentTimeMillis() > 0)
			return;
		/*
		 * 编译内存中的java代码
		 */
		// 1.将代码写入内存中
		StringWriter writer = new StringWriter(); // 内存字符串输出流
		PrintWriter out = new PrintWriter(writer);
		out.println("package com.dongtai.hello;");
		out.println("public class Hello{");
		out.println("public static void main(String[] args){");
		out.println("System.out.println(\"HelloWorld! 2\");");
		out.println("}");
		out.println("}");
		out.flush();
		out.close();

		// 2.开始编译
		JavaCompiler javaCompiler = ToolProvider.getSystemJavaCompiler();
		JavaFileObject fileObject = new JavaStringObject("Hello", writer.toString());
		CompilationTask task = javaCompiler.getTask(null, null, null,
				Arrays.asList("-d", ClassLoader.getSystemClassLoader().getResource("").getPath()), null, Arrays.asList(fileObject));
		boolean success = task.call();

		if (!success) {
			System.out.println("编译失败");
		} else {
			System.out.println("编译成功");
		}
		Class classl = ClassLoader.getSystemClassLoader().loadClass("com.dongtai.hello.Hello");
		Method method = classl.getDeclaredMethod("main", String[].class);
		String[] argsl = { null };
		method.invoke(classl.newInstance(), argsl);

	}
}
