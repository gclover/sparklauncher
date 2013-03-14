package com.allyes.sparklauncher;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import lombok.Getter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONObject;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.kohsuke.args4j.Option;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.TupleEntry;

import com.allyes.canape.common.cascading.scheme.hadoop.NLineTextFile;
import com.allyes.canape.common.hadoop.tool.FsUtils;
import com.allyes.canape.common.hadoop.tool.TextFolderWriter;
import com.allyes.canape.common.misc.CmdLineBase;
import com.allyes.canape.common.misc.Helper;
import com.allyes.canape.common.misc.CmdLineBase.CmdLineParamsException;
import com.allyes.canape.common.misc.GenericTool;

public class Launcher implements GenericTool {
	
	private Thread localListener;
	
	private static class CmdLineParams extends CmdLineBase {
		
		@Option(name = "-min", usage = "min worker nodes")
		@Getter private int minNodes = 1;
		
		@Option(name = "-max", usage = "max worker nodes")
		@Getter private int maxNodes = 10000;
		
		@Option(name = "-tmp", usage = "hdfs tmp dir", required = true)
		@Getter private String hdfsTmpDir;
		
		@Option(name = "-seed", usage = "map count seed")
		@Getter private int seed = 1000;
		
		@Option(name = "-ip", usage = "launcher ip addess", required = true) 
		@Getter private String listenIP;
		
		@Option(name = "-port", usage = "launcher listener port", required = true) 
		@Getter private int listenPort = 10102;
		
		@Option(name = "-master_port", usage = "master port")
		@Getter private int masterPort = 10103;
		
		@Option(name = "-master_ui_port", usage = "master ui port")
		@Getter private int masterUiPort = 10103;
		
		@Option(name = "-worker_cores", usage = "worker cores")
		@Getter private int workerCores = 1;
		
		@Option(name = "-worker_mem", usage = "worker memory (M)")
		@Getter private int workerMem = 1024;
		
		@Option(name = "-scala_archive", usage = "scala archive", required = true)
		@Getter private String scalaArchive;
		
		@Option(name = "-spark_archive", usage = "spark archive", required = true)
		@Getter private String sparkArchive;
		
		/*
		@Option(name = "-job_archives", usage = "job archives (split by ':')")
		@Getter private String jobArchives;
		
		@Option(name = "-job_archive_dirs", usage = "job archive dirs (split by ':')")
		@Getter private String jobArchiveDirs;
		*/
		
		@Option(name = "-keep", usage = "keep lauch process tmp file")
		@Getter private boolean keepLauchTmpFiles;
		
	}
	
	/*
	public void copyZipEntry(ZipInputStream in, ZipOutputStream out, String entryName) throws IOException {
		out.putNextEntry(new ZipEntry(entryName));
		byte[] buf = new byte[1024];
		int offset = 0;
		int len = 0;
		while ((len = in.read(buf, offset, 1024)) != 0) {
			out.write(buf, 0, len);
			offset += len;
		}
	}
	
	public String deployJobArchives(String[] jobArchives, String launchJobArchiveName, String launchDir) throws IOException {
		FileSystem fs = FsUtils.defaultFileSystem();
		Path dest = new Path(launchDir, launchJobArchiveName);
		ZipOutputStream zipOut = new ZipOutputStream(fs.create(dest));	
		Set<String> hasSubLibArchives = new HashSet<String>();
		Set<String> addedLibArchives = new HashSet<String>();
		for (String archive : jobArchives) {
			if (addedLibArchives.contains(archive))
				continue;
			boolean hasSubLib = false;
			ZipInputStream zipIn = new ZipInputStream(new FileInputStream(archive));
			ByteArrayOutputStream tmpZipOutBuf = new ByteArrayOutputStream();
			ZipOutputStream tmpZipOut = new ZipOutputStream(tmpZipOutBuf);
			ZipEntry entry = null;
			while ((entry = zipIn.getNextEntry()) != null) {
				if (entry.getName().endsWith(".jar")) 
					hasSubLib = true; 
				else 
					copyZipEntry(zipIn, tmpZipOut, entry.getName());
			}
			zipOut.putNextEntry(new ZipEntry(archive));
			byte[] buf = tmpZipOutBuf.toByteArray();
			zipOut.write(buf);
			addedLibArchives.add(archive);
			if (hasSubLib) 
				hasSubLibArchives.add(archive);
		}
		for (String archive : hasSubLibArchives) {
			if (addedLibArchives.contains(archive))
				continue;
			ZipInputStream zipIn = new ZipInputStream(new FileInputStream(archive));
			ZipEntry entry = null;
			while ((entry = zipIn.getNextEntry()) != null) {
				if (!entry.getName().endsWith(".jar"))
					continue;
				else 
					copyZipEntry(zipIn, zipOut, new Path(entry.getName()).getName());				addedLibArchives.add(archive);			}
		}
		
		zipOut.close();
		return dest.toUri().toString();
	}
	
	
	private String[] genAllJobArchives(String archives, String archiveDirs) { 
		String[] splittedArchives = Helper.assign(archives, "").split(":");
		String[] splittedArchiveDirs = Helper.assign(archiveDirs, "").split(":");
		
		List<String> lastArchives = new ArrayList<String>();
		for (String archive : splittedArchives) 
			lastArchives.add(archive);
			
		for (String dir : splittedArchiveDirs) 
			for (File file : new File(dir).listFiles()) 
				lastArchives.add(file.getAbsolutePath());
				
		return lastArchives.toArray(new String[0]);
	}
	*/
	
	private String getFileMajorName(String file) {
		String name = new Path(file).getName();
		return name.substring(0, name.length() - ".zip".length());
	}
	
	private String deploySeedFile(String seedFile, String launchDir,
			String launchScalaArchive, String launchSparkArchive, /*String launchJobArchive, */
			CmdLineParams p) throws IOException {
		Path dest = new Path(launchDir, seedFile);
		TextFolderWriter seedFileOut = new TextFolderWriter(dest);
		for (int i = 0; i < p.getSeed(); i++) {
			JSONObject lineObj = new JSONObject();
			lineObj.put("launcher-ip", p.getListenIP());
			lineObj.put("launcher-port", new Integer(p.getListenPort()));
			lineObj.put("master-port", new Integer(p.getMasterPort())); 
			lineObj.put("master-ui-port", new Integer(p.getMasterUiPort())); 
			lineObj.put("worker-cores", new Integer(p.getWorkerCores())); 
			lineObj.put("worker-mem", new Integer(p.getWorkerMem()));
			lineObj.put("scala-dir", getFileMajorName(launchScalaArchive));
			lineObj.put("spark-dir", getFileMajorName(launchSparkArchive));
			StringWriter out = new StringWriter();
			lineObj.writeJSONString(out);
			out.toString();
			seedFileOut.appendLine(out.toString());
		}
		seedFileOut.close();
		return dest.toUri().toString();
	}
		
	private String deployDepArchive(String archive, String launchDir) throws IOException { 
		FileSystem fs = FsUtils.defaultFileSystem();
		Path src = new Path(archive);
		Path dest = new Path(launchDir, src.getName());
		fs.copyFromLocalFile(src, dest);
		return dest.toUri().toString();
	}
	
	private void startLocalListener(int port, int minNodes, int maxNodes) {
		localListener = new Thread(
				new LocalListener(port, minNodes, maxNodes));
		localListener.start();
	}
	
	private String genLaunchDir(String tmpDir) {
		 return tmpDir + "/sparklaunch-" + UUID.randomUUID() + '/';
	}
	
	private void startLaunchProcess(String launchSeedFile, 
			String launchScalaArchive, String launchSparkArchive/*, String launchJobArchive*/) {
		Properties props = new Properties();
		AppProps.setApplicationJarClass(props, Launcher.class);
		
		Pipe pipe = new Pipe("Spark pipe");
		pipe = new Each(pipe, new NodeLauncher());
		
		Tap source = new Hfs(new NLineTextFile(), launchSeedFile);
		Tap	sink = new Hfs(new NLineTextFile(), launchSeedFile + "-output");
		
		FlowDef def = FlowDef.flowDef();
		def.setName("S");
		def.addSource(pipe, source);
		def.addTailSink(pipe, sink);
		//props.put("mapred.cache.files", String.format("%s,%s,%s", 
		//		launchScalaArchive, launchSparkArchive, launchJobArchive));
		props.put("mapred.cache.files", String.format("%s,%s", 
				launchScalaArchive, launchSparkArchive));		FlowConnector connector = new HadoopFlowConnector(props);
		Flow flow = connector.connect(def);
		flow.complete();
	}
	
	
	public void run(String[] args) {
		
		CmdLineParams p = new CmdLineParams();
		try {
			p.parseArgument(args);
		} catch (CmdLineParamsException e) {
			p.printException("Spark launcher", e);
			return;
		}	
		
		//String launchJobArchiveName = "job_jar.zip";
		
		String launchDir = genLaunchDir(p.getHdfsTmpDir());
		//String[] jobArchives = genAllJobArchives(p.getJobArchives(), p.getJobArchiveDirs());
		
		String launchSeedFile = null;
		String launchScalaArchive = null;
		String launchSparkArchive = null;
		String launchJobArchive = null;
		try {
			launchScalaArchive = deployDepArchive(p.getScalaArchive(), launchDir);
			launchSparkArchive = deployDepArchive(p.getSparkArchive(), launchDir);
			//launchJobArchive = deployJobArchives(jobArchives, launchJobArchiveName, launchDir);
			//launchSeedFile = deploySeedFile("seed", launchDir, 
			//		launchScalaArchive, launchSparkArchive, launchJobArchive, p);
			launchSeedFile = deploySeedFile("seed", launchDir, 
					launchScalaArchive, launchSparkArchive, p);		} catch (IOException e) {
			e.printStackTrace();
		}
		
		startLocalListener(p.getListenPort(), p.getMinNodes(), p.getMaxNodes());
		//startLaunchProcess(launchSeedFile, launchScalaArchive, launchSparkArchive, launchJobArchive);
		startLaunchProcess(launchSeedFile, launchScalaArchive, launchSparkArchive);
		
		try {
			localListener.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		if (!p.isKeepLauchTmpFiles()) {
			try {
				FsUtils.deleteWhenExists(new Path(launchDir), true);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		GenericTool tool = new Launcher();
		tool.run(args);
	}
}


