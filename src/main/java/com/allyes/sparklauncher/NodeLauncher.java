package com.allyes.sparklauncher;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;

import org.apache.hadoop.util.Shell;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.allyes.canape.common.misc.JarUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.TupleEntry;

public class NodeLauncher extends BaseOperation implements Function {
	
	public NodeLauncher() {
	}
	
	public class SparkMasterRunner implements Runnable {
		
		private int port;
		private int uiPort;
		private String scalaDir;
		private String sparkDir;
		
		public SparkMasterRunner(String home, int port, int uiPort, String scalaDir, String sparkDir) {
			this.port = port;
			this.uiPort = uiPort;
			this.scalaDir = scalaDir;
			this.sparkDir = sparkDir;
		}
		
		public void run() {
			final String[] commands = {
					"env",
					String.format("SCALA_HOME=%s", scalaDir),
					String.format("SPARK_HOME=%s", sparkDir),
					String.format("%s/run", sparkDir), 
					"spark.deploy.master.Master",
				  "-p", new Integer(port).toString(),
					"--webui-port", new Integer(uiPort).toString(),
			};
			Shell.ShellCommandExecutor cmd = new Shell.ShellCommandExecutor(commands); 	
			try {
				cmd.execute();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public class SparkWorkerRunner implements Runnable {
		
		private String masterIP;
		private int masterPort;
		private int cores;
		private int mem;
		private String workDir;
		private String scalaDir;
		private String sparkDir;
		
		public SparkWorkerRunner(String home,
				String masterIP, int masterPort,int cores, int mem, String workDir,
				String scalaDir, String sparkDir) {
			this.cores = cores;
			this.mem = mem;
			this.workDir = workDir;
			this.scalaDir = scalaDir;
			this.sparkDir = sparkDir;
		}
		
		public void run() {
			final String[] commands = {
					"env",
					String.format("SCALA_HOME=%s", scalaDir),
					String.format("SPARK_HOME=%s", sparkDir),
					String.format("%s/run", sparkDir),
					"spark.deploy.worker.worker",
					"-c", new Integer(cores).toString(),
					"-m", new Integer(mem).toString(),
					"-d", new Integer(workDir).toString(),
					String.format("spark://%s:%s", masterIP, masterPort)
			};

			Shell.ShellCommandExecutor cmd = new Shell.ShellCommandExecutor(commands); 	
			try {
				cmd.execute();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void runSparkMaster(String home, int port, int uiPort, String scalaDir, String sparkDir) {
		Thread master = new Thread(new SparkMasterRunner(home, port, uiPort, scalaDir, sparkDir));
		master.start();
	}
	
	private void runSparkWorker(String home, String masterIP, 
			int masterPort, int cores, int mem, String workDir,
			String scalaDir, String sparkDir) {
		Thread worker = new Thread(new SparkWorkerRunner(home,
				masterIP, masterPort, cores, mem, workDir, scalaDir, sparkDir));
		worker.start();
	}
	
	private static class ParserContainerFactory implements ContainerFactory {
		public List creatArrayContainer() {
      return new LinkedList();
    }
    public Map createObjectContainer() {
      return new LinkedHashMap();
    }
	}
	
	private String register(String launcherIP, int launcherPort) {
		DefaultHttpClient httpClient = new DefaultHttpClient();
		HttpGet httpGet = new HttpGet(String.format("http://%s:%s/register", launcherIP, launcherPort));
		HttpResponse response;
		String result = null;
		try {
			response = httpClient.execute(httpGet);
			result = response.getEntity().getContent().toString().trim();
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
		return result;
	}
	
	public void operate(FlowProcess process, FunctionCall call) {
		TupleEntry arguments = call.getArguments();
	  String line = arguments.getString(0);
	  
	  JSONParser parser = new JSONParser();
	  Map conf = null;
		try {
			conf = (Map)parser.parse(line, new ParserContainerFactory());
		} catch (ParseException e) {
			e.printStackTrace();
			return;
		}
		
		String jobLocalDir = (String)process.getProperty("job.local.dir");
		
		String result = register((String)conf.get("launcher-ip"), 
															(Integer)conf.get("launcher-port"));
		
		String[] slice = result.split(":");
		if (slice[0] == "master") {
			runSparkMaster(jobLocalDir,
											(Integer)conf.get("master-port"), 
											(Integer)conf.get("master-ui-port"),
											(String)conf.get("scala-dir"),
											(String)conf.get("spark-dir"));
		} else if (slice[0] == "worker") {
			String masterIP = slice[1];
			String workDir = jobLocalDir + "/work";
			runSparkWorker(jobLocalDir,
											masterIP, 
											(Integer)conf.get("master-port"), 
											(Integer)conf.get("worker-cores"), 
											(Integer)conf.get("worker-mem"), 
											workDir,
											(String)conf.get("scala-dir"),
											(String)conf.get("scala-dir"));
		} else {
			// TODO
		}
			
			
	}
}