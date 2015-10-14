package dp.bfd.example.read;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class scanTestReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
	private static final Log LOG = LogFactory.getLog(scanTestReduce.class);
	private  IntWritable result = new IntWritable();
	protected void reduce(Text key,Iterable<IntWritable> valueList,Context context) throws IOException,InterruptedException{
		int sum = 0;
		for (IntWritable value: valueList){
			sum += value.get();
		}
		result.set(sum);
		context.write(key,result);
	}
}
