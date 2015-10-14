package dp.bfd.example.read;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class BfdScanTableRecordReader extends RecordReader<ImmutableBytesWritable, Cell> implements Configurable{
	
	private HFile.Reader hfileReader;
	private HFileScanner hFileScanner;
	private int entryNumber = 0;
	private Configuration conf;
	private static final Log LOG = LogFactory.getLog(BfdScanTableRecordReader.class);

	public BfdScanTableRecordReader(FileSplit split, Configuration conf) throws IOException{
		Path storeFilePath = split.getPath();
		hfileReader = HFile.createReader(FileSystem.get(conf), storeFilePath, new CacheConfig(conf), conf);
		hFileScanner = hfileReader.getScanner(false, false,false);
		hfileReader.loadFileInfo();
		hFileScanner.seekTo();
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		if (hfileReader != null){
			hfileReader.close();
		}
		
	}

	@Override
	public ImmutableBytesWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new ImmutableBytesWritable(hFileScanner.getKeyString().getBytes());
	}

	@Override
	public Cell getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		Cell cell = hFileScanner.getKeyValue();
		return cell;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (hfileReader != null){
			return (entryNumber/hfileReader.length());
		}
		return 1;
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		entryNumber++;
		return hFileScanner.next();
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
		LOG.info("Test get config:"+ conf.get(BfdScanTableInputFormat.INPUG_TABLE));
	}

}
