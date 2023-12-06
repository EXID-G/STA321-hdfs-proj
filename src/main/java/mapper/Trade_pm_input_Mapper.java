package mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Trade_pm_input_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] split = input.split("\\|");

        //For each value, we need to select data from correct bank and time
        //Then select the data by the feature 'ExecType'

        context.write(new Text(split[0]), new IntWritable(Integer.parseInt(split[2])));
    }
}
