package reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashSet;

public class FindKReducer extends Reducer<Text, Text, Text, Text> {

    private MultipleOutputs<Text, Text> multipleOutputs;

    @Override
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // For each key (ORDED_ID, that is,ApplSeqNum), we have the value:
        // (TIMESTAMP, PRICE, SIZE, BUY_SELL_FLAG, ORDER_TYPE, CANCEL_TYPE, AUX)
        // AUX=1, from the table MarketOrder; AUX=0, from the table Traded.

        // For each key, we need to find the unique PRICE with AUX=0, and let K = (num of unique PRICE).
        String order_id = key.toString();
        String timestamp = "";
        String size = "";
        String buy_sell_flag = "";
        String order_type = "";
        String cancel_type = "";

        HashSet<Double> uniqueValues = new HashSet<>();

        for (Text value : values) {
            // for each value, split it by ',' and get the fields
            String[] fields = value.toString().split(",");

            if (fields[6].equals("1")) {
                // if AUX = 1, then it is from the table MarketOrder
                timestamp = fields[0];
                size = fields[2];
                buy_sell_flag = fields[3];
                order_type = fields[4];
                cancel_type = fields[5];
            } else {
                // if AUX = 0, then it is from the table Traded, we only need the unique PRICE
                uniqueValues.add(Double.parseDouble(fields[1]));
            }
        }

        // the output is (ORDER_ID, TIMESTAMP, SIZE, PRICE(=0), BUY_SELL_FLAG, ORDER_TYPE, K, CANCEL_TYPE)
        multipleOutputs.write("MarketOrder", new Text(""), new Text(order_id + "," + timestamp + "," + size + "," + 0 + "," + buy_sell_flag + "," + order_type + "," + uniqueValues.size() + "," + cancel_type));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
