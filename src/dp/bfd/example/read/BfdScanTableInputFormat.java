package dp.bfd.example.read;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class BfdScanTableInputFormat extends InputFormat<ImmutableBytesWritable, Cell> implements Configurable{
	
	private static final Log LOG = LogFactory.getLog(BfdScanTableInputFormat.class);
	
	public static final String INPUG_TABLE = "hbase.mapreduce.inputtable";
	public static final String MAP_READ_REGION = "bfd.hbase.mapreduce.map.read.region.number";
	
	private Configuration conf = null;
	
	private TableName tableName;

	@Override
	public RecordReader<ImmutableBytesWritable, Cell> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new BfdScanTableRecordReader((FileSplit)split, context.getConfiguration());
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (tableName == null){
			throw new IOException("No table was provided.");
		}
		LOG.info("Test get config:"+ conf.get("hbase.rootdir"));
		
		List<InputSplit> tableRegionInputSplitList = new ArrayList<InputSplit>();
		Path hbaseRootPath = FSUtils.getRootDir(conf);
		Path tablePath = FSUtils.getTableDir(hbaseRootPath, tableName);
		FileSystem fs = tablePath.getFileSystem(conf);
		List<Path> allRegionPaths = FSUtils.getRegionDirs(fs,tablePath);
		List<Path> storeFilePaths = new ArrayList<Path>();
		
		for (Path regionPath: allRegionPaths){
			PathFilter dirFilter = new FSUtils.DirFilter(fs);
			FileStatus[] familyStatus = fs.listStatus(regionPath,dirFilter);
			for (FileStatus fileStatus: familyStatus){
				FileStatus[] storeFiles = fs.listStatus(fileStatus.getPath());
				List<String> rowkeyList = new ArrayList<String>();
				
				if (HConstants.RECOVERED_EDITS_DIR.equals(fileStatus.getPath().getName())){
					continue;
				}
				for (FileStatus storeFile: storeFiles){
					if (!storeFile.isDirectory()){
						LOG.info("get store file path:"+storeFile.getPath().getName());
						long hfileLength = storeFile.getLen();
						String [] hosts = null;
						FileSplit hffs = new FileSplit(storeFile.getPath(), 0, hfileLength, hosts);
						tableRegionInputSplitList.add(hffs);
					}
				}
			}
		}
		
		return tableRegionInputSplitList;
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		this.conf = conf;
		tableName = TableName.valueOf(conf.get(INPUG_TABLE));
		LOG.info("Test get config:"+ conf.get(INPUG_TABLE));
		if (conf.get(MAP_READ_REGION) == null){
			conf.setInt(MAP_READ_REGION, 1);
		}
	}

}
