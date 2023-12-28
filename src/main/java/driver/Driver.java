package driver;


import mapper.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import reducer.FindKReducer;
import reducer.OutputReducer;

import java.io.IOException;


public class Driver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // Run job1
        boolean success1 = runJob1();
        if (!success1) {
            System.err.println("Job1 failed.");
            System.exit(1);
        }

        // Run job2
        boolean success2 = join_Sort();
        if (!success2) {
            System.err.println("Job2 failed.");
            System.exit(1);
        }
        System.exit(0);


//      System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }


    public static boolean runJob1() throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf1 = new Configuration();

        // the beginning of the first job
        Job job1 = Job.getInstance(conf1, "job1");
        job1.setJarByClass(Driver.class);

        // 如果要要在docker上跑，路径前要加一个“/”，否则会报错。本地就不用”/“
        // 设置第一个输入路径和对应的Map处理逻辑及输出类型
        MultipleInputs.addInputPath(job1, new Path("data/order/am_hq_order_spot.txt"), TextInputFormat.class,
                OrderInputMapper.class);
        // 设置第二个输入路径和对应的Map处理逻辑及输出类型
        MultipleInputs.addInputPath(job1, new Path("data/order/pm_hq_order_spot.txt"), TextInputFormat.class,
                OrderInputMapper.class);
        // 设置第三个输入路径和对应的Map处理逻辑及输出类型
        MultipleInputs.addInputPath(job1, new Path("data/trade/am_hq_trade_spot.txt"), TextInputFormat.class,
                TradeInputMapper.class);
        // 设置第四个输入路径和对应的Map处理逻辑及输出类型
        MultipleInputs.addInputPath(job1, new Path("data/trade/pm_hq_trade_spot.txt"), TextInputFormat.class,
                TradeInputMapper.class);


//        job.setMapperClass(MultipleInputMapper2.class);

        // 设置Reduce处理逻辑及输出类型
        job1.setReducerClass(FindKReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        // 设置输出路径
        Path outputPath = new Path("output/job1");
        FileOutputFormat.setOutputPath(job1,outputPath);

//        // 设置多输出,即输出四张表
//        MultipleOutputs.addNamedOutput(job1, "MarketOrder", TextOutputFormat.class, Text.class, Text.class);
//        MultipleOutputs.addNamedOutput(job1, "LimitedOrder", TextOutputFormat.class, Text.class, Text.class);
//        MultipleOutputs.addNamedOutput(job1, "SpecOrder", TextOutputFormat.class, Text.class, Text.class);
//        MultipleOutputs.addNamedOutput(job1, "Cancel", TextOutputFormat.class, Text.class, Text.class);

        // 设置权限
        FileSystem fs = FileSystem.get(conf1);

        if (fs.exists(outputPath)){
            fs.delete(outputPath, true);  // 删除已存在的输出目录
        }

        // 提交job1
        return job1.waitForCompletion(true);
    }


    public static boolean join_Sort() throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf2 = new Configuration();

        // The beginning of job2
        Job job2 = Job.getInstance(conf2, "job2_join");
        job2.setJarByClass(Driver.class);

//        // 设置第一个输入路径和对应的Map处理逻辑及输出类型
//        MultipleInputs.addInputPath(job2, new Path("output/job1/MarketOrder-r-00000"), TextInputFormat.class,
//                MapJoinMapper.class);
//        // 设置第二个输入路径和对应的Map处理逻辑及输出类型
//        MultipleInputs.addInputPath(job2, new Path("output/job1/LimitedOrder-r-00000"), TextInputFormat.class,
//                MapJoinMapper.class);
//        // 设置第三个输入路径和对应的Map处理逻辑及输出类型
//        MultipleInputs.addInputPath(job2, new Path("output/job1/SpecOrder-r-00000"), TextInputFormat.class,
//                MapJoinMapper.class);
//        // 设置第四个输入路径和对应的Map处理逻辑及输出类型
//        MultipleInputs.addInputPath(job2, new Path("output/job1/Cancel-r-00000"), TextInputFormat.class,
//                MapJoinMapper.class);


        job2.setMapperClass(MapJoinMapper.class);
        FileInputFormat.addInputPath(job2, new Path("output/job1/part-r-00000"));


        // 设置Reduce处理逻辑及输出类型
        job2.setReducerClass(OutputReducer.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);

        // 设置输出路径
        Path outputPath = new Path("output/job2");
        FileOutputFormat.setOutputPath(job2, outputPath);

        FileSystem fs = FileSystem.get(conf2);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);  // 删除已存在的输出目录
        }

        // 提交job2
        return job2.waitForCompletion(true);

    }

}
