package order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPatitioner extends Partitioner<OrderBean, NullWritable> {

	@Override
	public int getPartition(OrderBean key, NullWritable value, int numPartitions) {
		// ����key��orderid��hashCodeֵ����
		 return (key.getOrderId().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}