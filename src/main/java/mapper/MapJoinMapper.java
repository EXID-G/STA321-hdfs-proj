package mapper;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapJoinMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        //Let key be the time stamp, value be the whole line.
        context.write(new LongWritable(Long.parseLong(fields[0])), value);
    }
}
