package reducer;

//import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class OutputReducer extends Reducer<LongWritable, Text, NullWritable, Text> {

    //    private MultipleOutputs<Text, Text> multipleOutputs;
//
//    @Override
//    protected void setup(Context context) {
//        multipleOutputs = new MultipleOutputs<>(context);
//    }
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Add header to the output file
        context.write(NullWritable.get(), new Text("TIMESTAMP,PRICE,SIZE,BUY_SELL_FLAG,ORDER_TYPE,ORDER_ID," +
                "MARKET_ORDER_TYPE,CANCEL_TYPE"));
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException,
            InterruptedException {
        // sorted by the key(TIMESTAMP), we just need output the value
        for (Text value : values) {
            String input = value.toString();
            String time = input.split(",")[0];

            // From 20190102091500990
            // To   2019-01-02 09:15:00.990000
            Text timeText =
                    new Text(time.substring(0, 4) + "-" +   //2019-
                            time.substring(4, 6) + "-" +          //01-
                            time.substring(6, 8) + " " +          //02
                            time.substring(8, 10) + ":" +          //09:
                            time.substring(10, 12) + ":" +          //15:
                            time.substring(12, 14) + "." +         //00.
                            time.substring(14, 17) + "000"          //990000
                    );
            context.write(NullWritable.get(), new Text(timeText  + input.substring(17)));
        }
    }

//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//        multipleOutputs.close();
//    }
}
