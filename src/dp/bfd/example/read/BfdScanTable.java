package dp.bfd.example.read;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.JarFinder;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import dp.bfd.example.read.scanTestMapper;

public class BfdScanTable {
	private static final Log LOG = LogFactory.getLog(BfdScanTable.class);
	
	/**
	 * 把所有HBase部分的依赖加入到job中
	 */
	public static void addHBaseDependencyJars(Configuration conf)
			throws IOException {
		addDependencyJars(
				conf,
				org.apache.hadoop.hbase.HConstants.class, // hbase-common
				org.apache.hadoop.hbase.protobuf.generated.ClientProtos.class, // hbase-protocol
				org.apache.hadoop.hbase.client.Put.class, // hbase-client
				org.apache.hadoop.hbase.CompatibilityFactory.class, // hbase-hadoop-compat
				org.apache.hadoop.hbase.mapreduce.TableMapper.class, // hbase-server
				// 其它的hbase独立于hadoop的一些依赖
				org.apache.zookeeper.ZooKeeper.class,
				com.google.protobuf.Message.class,
				com.google.common.collect.Lists.class, org.htrace.Trace.class,
				com.yammer.metrics.core.MetricsRegistry.class
				);
	}

	/**
	 * 列出来所有的hbase客户端开发依赖的所有的包，类似于执行hbase mapredcp命令得到的结果
	 */
	public static String buildDependencyClasspath(Configuration conf) {
		if (conf == null) {
			throw new IllegalArgumentException("Must provide a configuration object.");
		}
		Set<String> paths = new HashSet<String>(conf.getStringCollection("tmpjars"));
		if (paths.size() == 0) {
			throw new IllegalArgumentException("Configuration contains no tmpjars.");
		}
		StringBuilder sb = new StringBuilder();
		for (String s : paths) {
			//类似于'file:/path/to/file.jar'的结构
			int idx = s.indexOf(":");
			if (idx != -1)
				s = s.substring(idx + 1);
			if (sb.length() > 0)
				sb.append(File.pathSeparator);
			sb.append(s);
		}
		return sb.toString();
	}

	  /**
	   *把所有的本程序的依赖都加到job的配置中，然后JobClient就会把该配置的jar包提交到集群，加入到DistributedCache中
	   */
	  public static void addDependencyJars(Job job) throws IOException {
	    addHBaseDependencyJars(job.getConfiguration());
	    try {
	      addDependencyJars(job.getConfiguration(),
	          job.getMapOutputKeyClass(),
	          job.getMapOutputValueClass(),
	          job.getInputFormatClass(),
	          job.getOutputKeyClass(),
	          job.getOutputValueClass(),
	          job.getOutputFormatClass(),
	          job.getPartitionerClass(),
	          job.getCombinerClass());
	    } catch (ClassNotFoundException e) {
	      throw new IOException(e);
	    }
	  }

	  public static void addDependencyJars(Configuration conf, Class<?>... classes) throws IOException {

	    FileSystem localFs = FileSystem.getLocal(conf);
	    Set<String> jars = new HashSet<String>();
	     // 把默认的jar都加载进来
	    jars.addAll(conf.getStringCollection("tmpjars"));

	    //把所有需要加载的jar放到一个map里面，防止不同的地方重复加入
	    Map<String, String> packagedClasses = new HashMap<String, String>();

	    // 添加包含有专属类的jar
	    for (Class<?> clazz : classes) {
	      if (clazz == null) continue;

	      Path path = findOrCreateJar(clazz, localFs, packagedClasses);
	      if (path == null) {
	        LOG.warn("Could not find jar for class " + clazz +  " in order to ship it to the cluster.");
	        continue;
	      }
	      if (!localFs.exists(path)) {
	        LOG.warn("Could not validate jar file " + path + " for class " + clazz);
	        continue;
	      }
	      jars.add(path.toString());
	    }
	    if (jars.isEmpty()) return;

	    conf.set("tmpjars", StringUtils.arrayToString(jars.toArray(new String[jars.size()])));
	  }

	  /**
	   * 查找所有的jar，以及对于class文件，需要单独创建jar。
	   */
	  private static Path findOrCreateJar(Class<?> my_class, FileSystem fs, Map<String, String> packagedClasses)
	  throws IOException {
	    // 确认是否有已经存在的jar包含该class
	    String jar = findContainingJar(my_class, packagedClasses);
	    if (null == jar || jar.isEmpty()) {
	      jar = getJar(my_class);
	      updateMap(jar, packagedClasses);
	    }

	    if (null == jar || jar.isEmpty()) {
	      return null;
	    }

	    LOG.debug(String.format("For class %s, using jar %s", my_class.getName(), jar));
	    return new Path(jar).makeQualified(fs);
	  }

	  private static void updateMap(String jar, Map<String, String> packagedClasses) throws IOException {
	    if (null == jar || jar.isEmpty()) {
	      return;
	    }
	    ZipFile zip = null;
	    try {
	      zip = new ZipFile(jar);
	      for (Enumeration<? extends ZipEntry> iter = zip.entries(); iter.hasMoreElements();) {
	        ZipEntry entry = iter.nextElement();
	        if (entry.getName().endsWith("class")) {
	          packagedClasses.put(entry.getName(), jar);
	        }
	      }
	    } finally {
	      if (null != zip) zip.close();
	    }
	  }

	  /**
	   * 查找包含有一个class名称的jar包
	   */
	  private static String findContainingJar(Class<?> my_class, Map<String, String> packagedClasses)
	      throws IOException {
	    ClassLoader loader = my_class.getClassLoader();

	    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";

	    if (loader != null) {
	      for (Enumeration<URL> itr = loader.getResources(class_file); itr.hasMoreElements();) {
	        URL url = itr.nextElement();
	        if ("jar".equals(url.getProtocol())) {
	          String toReturn = url.getPath();
	          if (toReturn.startsWith("file:")) {
	            toReturn = toReturn.substring("file:".length());
	          }
	          toReturn = toReturn.replaceAll("\\+", "%2B");
	          toReturn = URLDecoder.decode(toReturn, "UTF-8");
	          return toReturn.replaceAll("!.*$", "");
	        }
	      }
	    }

	    return packagedClasses.get(class_file);
	  }

	  private static String getJar(Class<?> my_class) {
	    String ret = null;
	    try {
	      ret = JarFinder.getJar(my_class);
	    } catch (Exception e) {
	      throw new RuntimeException("getJar invocation failed.", e);
	    }
	    return ret;
	  }
	
	public static void main(String [] args){
		Configuration conf = new Configuration();
		try {
			conf.addResource(new FileInputStream("conf/core-site.xml"));
			conf.addResource(new FileInputStream("conf/hdfs-site.xml"));
			conf.addResource(new FileInputStream("conf/yarn-site.xml"));
			conf.addResource(new FileInputStream("conf/mapred-site.xml"));
			conf.addResource(new FileInputStream("conf/hbase-site.xml"));
			conf.set("hbase.mapreduce.inputtable", "lapTest3");
			LOG.info("Test get config:"+ conf.get("hbase.rootdir"));
			
			Job job = new Job(conf,"scanTest");
//			job.setJarByClass(BfdScanTableInputFormat.class);
			job.setInputFormatClass(BfdScanTableInputFormat.class);
			
//			job.setJarByClass(scanTestMapper.class);
			job.setMapperClass(scanTestMapper.class);
			
//			job.setJarByClass(scanTestReduce.class);
			job.setReducerClass(scanTestReduce.class);
			job.setOutputKeyClass(Text.class);
			FileOutputFormat.setOutputPath(job, new Path("hdfs://bfdhadoop26/user/lap/output5"));
			job.setOutputValueClass(IntWritable.class);
			
			addDependencyJars(job);
			
			int jobComplete = job.waitForCompletion(true)?0:1;
			
		} catch (Exception e){
			e.printStackTrace();
		}
		
	}

}
