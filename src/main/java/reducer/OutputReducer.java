package reducer;

//import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;
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
        // sorted by the key(TIMESTAMP), we need change the format of TIMESTAMP and let the trade data behind the order data

        // 初始化一个TreeMap来保存键值对数据
        TreeMap<LongWritable, Text> dataMap = new TreeMap<>();

        boolean flag = false; // if there is a trade data, then flag = true, which will be used to judge whether to output the data in the treeMap or not

        for (Text value : values) {
            String[] input = value.toString().split(",");
            String time = input[0];

            // From 20190102091500990
            // To   2019-01-02 09:15:00.990000
            Text timeText =
                    new Text(time.substring(0, 4) + "-" +         //2019-
                            time.substring(4, 6) + "-" +          //01-
                            time.substring(6, 8) + " " +          //02
                            time.substring(8, 10) + ":" +          //09:
                            time.substring(10, 12) + ":" +          //15:
                            time.substring(12, 14) + "." +         //00.
                            time.substring(14, 17) + "000"          //990000
                    );

            //if the length of input.split(",") is 9,then it will be output at last
            if (input.length == 9) {
                flag = true;
                dataMap.put(new LongWritable(Long.parseLong(input[8])),
                        new Text(timeText + "," + input[1] + "," + input[2] + "," + input[3] + "," + input[4] + "," + input[5] + "," + input[6] + "," + input[7]));
            } else {
                //if the length of input.split(",") is 8,then it will be output directly
                context.write(NullWritable.get(), new Text(timeText + "," + input[1] + "," + input[2] + "," + input[3] +
                        "," + input[4] + "," + input[5] + "," + input[6] + "," + input[7]));
            }
        }

        if (flag) {
            for (Map.Entry<LongWritable, Text> entry : dataMap.entrySet()) {
                context.write(NullWritable.get(), entry.getValue());
            }
        }
    }

//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//        multipleOutputs.close();
//    }
}
