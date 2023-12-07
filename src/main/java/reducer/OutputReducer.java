package reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class OutputReducer extends Reducer<LongWritable, Text, Text, Text> {

    private MultipleOutputs<Text, Text> multipleOutputs;

    @Override
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,
            InterruptedException {
        // sorted by the key(TIMESTAMP), we just need output the value
        for (Text value : values) {
            multipleOutputs.write("Output",new Text(""), value);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
