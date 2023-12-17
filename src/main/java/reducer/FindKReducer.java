package reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class FindKReducer extends Reducer<Text, Text, NullWritable, Text> {

//    private MultipleOutputs<Text, Text> multipleOutputs;
//
//    @Override
//    protected void setup(Context context) {
//        multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
//    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        /*
        For each key (ORDED_ID, that is,ApplSeqNum), we have the value with different lengths:

        7 -> (TIMESTAMP, PRICE, SIZE, BUY_SELL_FLAG, ORDER_TYPE, CANCEL_TYPE, AUX)
        AUX=1, from the table MarketOrder; AUX=0, from the table Traded.

        8 -> (TIMESTAMP, PRICE, SIZE, BUY_SELL_FLAG, ORDER_TYPE, ORDED_ID, MARKET_ORDER_TYPE, CANCEL_TYPE)
        */

        //prepare for finding K
        // For each key, we need to find the unique PRICE with AUX=0, and let K = (num of unique PRICE).
        String order_id = key.toString();
        String timestamp = " ";
        String size = "0";
        String buy_sell_flag = " ";
        String order_type = " ";
        String cancel_type = " ";

        HashSet<Double> uniqueValues = new HashSet<>();

        boolean flag = true;  // for different output

        // iterate through the values
        for (Text value : values) {
            // for each value, split it by ',' and get the fields
            String[] fields = value.toString().split(",");

            // check the length of fields,
            // 7 -> find K;
            // 8 -> just output
            if (fields.length == 7) {
                // there are 7 fields

                //considering orders which is before 9:30, they are filtered in mapper, but it can be traded after 9:30.
                timestamp = fields[0];
                buy_sell_flag = fields[3];
                cancel_type = fields[5];

                if (fields[6].equals("1")) {
                    // if AUX = 1, then it is from the table MarketOrder
                    order_type = fields[4];
                    size = fields[2];
                } else if (fields[6].equals("2")) {
                    // if AUX = 0, then it is from the table Traded, we only need the unique PRICE
                    uniqueValues.add(Double.parseDouble(fields[1]));
                }
            } else {
                flag = false;
                //there are 8 fields, just output
                context.write(NullWritable.get(), value);
            }
        }

//        multipleOutputs.write("MarketOrder", new Text(""),
//        new Text(order_id + "," + timestamp + "," + size + "," + 0 + "," + buy_sell_flag + "," +
//        order_type + "," + uniqueValues.size() + "," + cancel_type));
        // the output is (TIMESTAMP, PRICE(=0), SIZE, BUY_SELL_FLAG, ORDER_TYPE, ORDED_ID, K, CANCEL_TYPE)
        if (flag) {
            context.write(NullWritable.get(), new Text(timestamp + "," + 0 + "," + size + "," + buy_sell_flag +
                    "," + order_type + "," + order_id + "," + uniqueValues.size() + "," + cancel_type));

        }
    }

//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//        multipleOutputs.close();
//    }
}
