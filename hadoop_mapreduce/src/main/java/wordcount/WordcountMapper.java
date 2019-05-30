package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN:�������ݵ�key  �ļ����к�
 * VALUEIN��ÿ�е���������
 * 
 * KEYOUT��������� ��key
 * VALUEOUT:������ݵ�value����
 * @author Administrator
 *
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	//hello world
	//solace solace
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 1��ȡ��һ������
		String line = value.toString();
		
		// 2 ��ȡÿһ������
		String[] words = line.split(" ");
		
		for(String word:words){
			// 3 ���ÿһ������
			context.write(new Text(word), new IntWritable(1));
		}
	}
}



