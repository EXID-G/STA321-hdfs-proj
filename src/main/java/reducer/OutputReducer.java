package reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class OutputReducer extends Reducer<LongWritable, Text, NullWritable, Text> {

//    private MultipleOutputs<Text, Text> multipleOutputs;
//
//    @Override
//    protected void setup(Context context) {
//        multipleOutputs = new MultipleOutputs<>(context);
//    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,
            InterruptedException {
        // sorted by the key(TIMESTAMP), we just need output the value
        for (Text value : values) {

            context.write(NullWritable.get(), value);
        }
    }

//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//        multipleOutputs.close();
//    }
}
