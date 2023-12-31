package driver;


import mapper.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
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
        boolean success2 = runJob2();
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

        // 设置第一个输入路径和对应的Map处理逻辑及输出类型
        MultipleInputs.addInputPath(job1, new Path("data/order/am_hq_order_spot.txt"), TextInputFormat.class,
                Order_input_Mapper.class);
        // 设置第二个输入路径和对应的Map处理逻辑及输出类型
        MultipleInputs.addInputPath(job1, new Path("data/order/pm_hq_order_spot.txt"), TextInputFormat.class,
                Order_input_Mapper.class);
        // 设置第三个输入路径和对应的Map处理逻辑及输出类型
        MultipleInputs.addInputPath(job1, new Path("data/trade/am_hq_trade_spot.txt"), TextInputFormat.class,
                Trade_input_Mapper.class);
        // 设置第四个输入路径和对应的Map处理逻辑及输出类型
        MultipleInputs.addInputPath(job1, new Path("data/trade/pm_hq_trade_spot.txt"), TextInputFormat.class,
                Trade_input_Mapper.class);


//        job.setMapperClass(MultipleInputMapper2.class);

        // 设置Reduce处理逻辑及输出类型
        job1.setReducerClass(FindKReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        // 设置输出路径
        Path outputPath = new Path("output/job1");
        FileOutputFormat.setOutputPath(job1,outputPath);

        // 设置多输出,即输出四张表
        MultipleOutputs.addNamedOutput(job1, "MarketOrder", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job1, "LimitedOrder", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job1, "SpecOrder", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job1, "Cancel", TextOutputFormat.class, Text.class, Text.class);

        // 设置权限
        FileSystem fs = FileSystem.get(conf1);
        // 设置目录权限
//        fs.setPermission(new Path("output"), FsPermission.createImmutable((short) 0755));
        // 设置文件权限
//        fs.setPermission(new Path("/user/myuser/output/part-r-00000"), FsPermission.valueOf("644"));
//        fs.close();

        if (fs.exists(outputPath)){
            fs.delete(outputPath, true);  // 删除已存在的输出目录
        }

        // 提交job1
        return job1.waitForCompletion(true);
    }


    public static boolean runJob2() throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf2 = new Configuration();

        // The beginning of job2
        Job job2 = Job.getInstance(conf2, "job2_join");
        job2.setJarByClass(Driver.class);

        // 设置第一个输入路径和对应的Map处理逻辑及输出类型
        MultipleInputs.addInputPath(job2, new Path("output/job1/MarketOrder-r-00000"), TextInputFormat.class,
                MapJoinMapper.class);
        // 设置第二个输入路径和对应的Map处理逻辑及输出类型
        MultipleInputs.addInputPath(job2, new Path("output/job1/LimitedOrder-r-00000"), TextInputFormat.class,
                MapJoinMapper.class);
        // 设置第三个输入路径和对应的Map处理逻辑及输出类型
        MultipleInputs.addInputPath(job2, new Path("output/job1/SpecOrder-r-00000"), TextInputFormat.class,
                MapJoinMapper.class);
        // 设置第四个输入路径和对应的Map处理逻辑及输出类型
        MultipleInputs.addInputPath(job2, new Path("output/job1/Cancel-r-00000"), TextInputFormat.class,
                MapJoinMapper.class);

        // 设置Reduce处理逻辑及输出类型
        job2.setReducerClass(OutputReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // 设置输出路径
        Path outputPath = new Path("output/job2");
        FileOutputFormat.setOutputPath(job2, outputPath);

//        FileSystem fs = FileSystem.get(conf2);
//        if (fs.exists(outputPath)) {
//            fs.delete(outputPath, true);  // 删除已存在的输出目录
//        }

        // 提交job2
        return job2.waitForCompletion(true);

    }

}
