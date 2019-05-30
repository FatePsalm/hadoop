package order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
	OrderBean bean = new OrderBean();
	
	@Override
	protected void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {
		// 1 ��ȡ����
		String line = value.toString();
		
		// 2 �и�����
		String[] fields = line.split("\t");
		
		// Order_0000002	Pdt_03	522.8
		// 3 ��װbean����
		bean.setOrderId(fields[0]);
		bean.setPrice(Double.parseDouble(fields[2]));
		
		// 4 д��
		context.write(bean, NullWritable.get());
		
	}

}