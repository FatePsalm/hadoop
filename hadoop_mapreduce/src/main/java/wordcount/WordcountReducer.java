package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	//solace 1 solace 1
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context) throws IOException, InterruptedException {
		
		// 1 统计所有单词个数
		int count = 0;
		for(IntWritable value:values){
			count += value.get();
		}
		
		// 2输出所有单词个数
		context.write(key, new IntWritable(count));
	}
}
