package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

	// <a, 1> <a, 1>
	// <solace, 1> <solace, 1> <solace, 1>   ���Ϊ<solace,3>
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {
		// �����ۼӺ�
		int count = 0;
		
		for(IntWritable value:values){
			count += value.get();
		}
		
		// д��
		context.write(key, new IntWritable(count));
		
	}
}
