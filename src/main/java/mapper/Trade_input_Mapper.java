package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class Trade_input_Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private MultipleOutputs<Text, Text> multipleOutputs;
    private final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HHmm");
    private final LocalTime START_TIME_AM = LocalTime.parse("0930", TIME_FORMATTER);
    private final LocalTime END_TIME_AM = LocalTime.parse("1130", TIME_FORMATTER);
    private final LocalTime START_TIME_PM = LocalTime.parse("1300", TIME_FORMATTER);
    private final LocalTime END_TIME_PM = LocalTime.parse("1457", TIME_FORMATTER);
    protected void setup(Context context) {
        multipleOutputs = new MultipleOutputs<>(context);
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] record = input.split("\\s+");

        String tradedTimeString = record[15].substring(8,12);
        LocalTime tradedTime = LocalTime.parse(tradedTimeString, TIME_FORMATTER);
        boolean inContPhase = ((!tradedTime.isBefore(START_TIME_AM))&
                (!tradedTime.isAfter(END_TIME_AM))) | ((!tradedTime.isBefore(START_TIME_PM))&
                (!tradedTime.isAfter(END_TIME_PM)));

        if ( record[8].equals("000001") & inContPhase) {
            String execType = record[14];
            switch (execType){
                case "4" : {
                    Text val = new Text(record[12] + "," +   //TIMESTAMP
                            record[10] + "," +                     //PRICE
                            record[11] + "," +                     //SIZE
                            record[13] + "," +                     //BUY_SELL_FLAG
                            orderType + "," +                      //ORDER_TYPE
                            record[7] + "," +                      //ORDER_ID
                            "" + "," +                             //MARKET_ORDER_TYPE
                            "2");                                  //CANCEL_TYPE
                    multipleOutputs.write("LimitedOrder", new Text(""), val);
                }
                case "U" : {
                    Text val = new Text(record[12] + "," +   //TIMESTAMP
                            "" + "," +                             //PRICE
                            record[11] + "," +                     //SIZE
                            record[13] + "," +                     //BUY_SELL_FLAG
                            orderType + "," +                      //ORDER_TYPE
                            record[7] + "," +                      //ORDER_ID
                            "" + "," +                             //MARKET_ORDER_TYPE
                            "2");                                  //CANCEL_TYPE
                    multipleOutputs.write("SpecOrder", new Text(""), val);
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
                }
            }
        }
    }
}
