package dp.bfd.example.read;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class scanTestMapper extends Mapper<ImmutableBytesWritable, Cell, Text, IntWritable>{
	
	private static final Log LOG = LogFactory.getLog(scanTestMapper.class);
	private Text rowKey = new Text();
	private IntWritable one  = new IntWritable(1);
	
	protected void map(ImmutableBytesWritable key, Cell value, Context context) throws IOException,InterruptedException{
		rowKey = new Text(key.copyBytes().toString());
		context.write(rowKey, one);
		String rowKey = new String(CellUtil.cloneRow(value));
		LOG.info("RowName:" + rowKey+ " ");
		LOG.debug("Timetamp:" + value.getTimestamp() + " ");
		LOG.debug("column Family:" + new String(CellUtil.cloneFamily(value)) + " ");
		LOG.debug("row Name:" + new String(CellUtil.cloneQualifier(value)) + " ");
		LOG.debug("value:" + new String(CellUtil.cloneValue(value)) + " ");
	}
}
