package order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

	@Override
	protected void reduce(OrderBean bean, Iterable<NullWritable> values,
			Context context)	throws IOException, InterruptedException {
		
		// Ð´³ö
		context.write(bean, NullWritable.get());
	}
}
