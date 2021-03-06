package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN:输入数据的key  文件的行号
 * VALUEIN：每行的输入数据
 * KEYOUT：输出数据 的key
 * VALUEOUT:输出数据的value类型
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 将maptask传给我们的文本内容先转换成String
        String line = value.toString();

        // 2 根据空格将这一行切分成单词
        String[] words = line.split(" ");

        // 3 将单词输出为<单词，1>
        for (String word : words) {
            // 将单词作为key，将次数1作为value,以便于后续的数据分发，可以根据单词分发，以便于相同单词会到相同的reducetask中
            context.write(new Text(word), new IntWritable(1));

        }
    }
}
