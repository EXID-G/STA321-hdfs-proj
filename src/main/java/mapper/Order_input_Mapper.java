package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import java.io.IOException;

public class Order_input_Mapper extends Mapper<LongWritable, Text, Text, Text> {
//    private MultipleOutputs<Text, Text> multipleOutputs;
    private final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HHmm");
    private final LocalTime START_TIME_AM = LocalTime.parse("0930", TIME_FORMATTER);
    private final LocalTime END_TIME_AM = LocalTime.parse("1130", TIME_FORMATTER);
    private final LocalTime START_TIME_PM = LocalTime.parse("1300", TIME_FORMATTER);
    private final LocalTime END_TIME_PM = LocalTime.parse("1457", TIME_FORMATTER);

    //记得注销的时候把incontphase也改一下！！！！！！
//    private final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HHmmss");
//    private final LocalTime START_TIME_AM = LocalTime.parse("093000", TIME_FORMATTER);
//    private final LocalTime END_TIME_AM = LocalTime.parse("093030", TIME_FORMATTER);


//    @Override
//    protected void setup(Context context) {
//        multipleOutputs = new MultipleOutputs<>(context);
//    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] record = input.split("\\s+");

        String orderTimeString = record[12].substring(8,12);
//        String orderTimeString = record[12].substring(8,14);   //记得改一下！！！
        LocalTime orderTime = LocalTime.parse(orderTimeString, TIME_FORMATTER);
        boolean inContPhase = ((!orderTime.isBefore(START_TIME_AM))&
                (orderTime.isBefore(END_TIME_AM))) | ((!orderTime.isBefore(START_TIME_PM))&
                (orderTime.isBefore(END_TIME_PM)));

        if ( record[8].equals("000001") & inContPhase) {
            String orderType = record[14];
            switch (orderType){
                case "2" : {
                    Text val = new Text(record[12] + "," +   //TIMESTAMP
                            record[10] + "," +                     //PRICE
                            record[11] + "," +                     //SIZE
                            record[13] + "," +                     //BUY_SELL_FLAG
                            orderType + "," +                      //ORDER_TYPE
                            record[7] + "," +                      //ORDER_ID
                            "0" + "," +                             //MARKET_ORDER_TYPE
                            "2");                                  //CANCEL_TYPE
//                    multipleOutputs.write("LimitedOrder", new Text(""), val);
                    context.write(new Text(record[7]), val);
                    break;
                }
                case "U" : {
                    Text val = new Text(record[12] + "," +   //TIMESTAMP
                            " " + "," +                             //PRICE
                            record[11] + "," +                     //SIZE
                            record[13] + "," +                     //BUY_SELL_FLAG
                            orderType + "," +                      //ORDER_TYPE
                            record[7] + "," +                      //ORDER_ID
                            "0" + "," +                             //MARKET_ORDER_TYPE
                            "2");                                  //CANCEL_TYPE
//                    multipleOutputs.write("SpecOrder", new Text(""), val);
                    context.write(new Text(record[7]), val);
                    break;
                }
                case "1" : {
                    Text val = new Text(record[12] + "," +   //TIMESTAMP
                            record[10] + "," +                     //PRICE
                            record[11] + "," +                     //SIZE
                            record[13] + "," +                     //BUY_SELL_FLAG
                            orderType + "," +                      //ORDER_TYPE
                            "2" + "," +                            //CANCEL_TYPE
                            "1");                                  //AUX
                    context.write(new Text(record[7]), val);
                    break;
                }
                default:
                    break;
            }
        }
        //For each value, we need to select data from "000001" and "9:30~11:30"
        //Then select the data by the feature 'order type'
    }


//    @Override
//    protected void cleanup(Context context) throws IOException, InterruptedException {
//        multipleOutputs.close();
//    }
}
