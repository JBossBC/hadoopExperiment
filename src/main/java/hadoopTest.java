import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Time;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class hadoopTest {
     static Configuration config = new Configuration();

    public static void main(String[] args) throws  Exception {
        if (args.length<=0){
            System.out.println("the input params cant less than zero");
            System.exit(-1);
        }
//        uploadFileByJavaAPI(args);
//        downLoadFileByJavaAPI(args);
//        System.exit(-1);
//        config.set("mapreduce.input.fileinputformat.inputdir","/input");
//        config.set("mapreduce.output.fileoutputformat.outputdir","/output");
//        System.out.println(new JobConf(config).getWorkingDirectory());
//        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:9000"),config,"root");
//        fileSystem.copyFromLocalFile(new Path("./实验3第一题数据.txt"),new Path("/input/实验3第一题数据.txt"));
//        fileSystem.mkdirs(new Path("/output"));
//        fileSystem.close();
//        System.out.println("Starting the one job");
//        mapreduceTest(args);
//        System.out.println("Ending the one job");
//        System.out.println("Starting the two job");
//        sortResultFromOne(args);
//        System.out.println("Ending the Two job");
//        System.out.println("Starting the three job");
//        timePartition(args);
//        System.out.println("Ending the three job");
//        System.out.println("Starting the compute average value job");
//        averageValue(args);
//        System.out.println("Ending the compute average value job");
        System.out.println("Starting the statisticPublicNumber job");
        statisticPublicNumber(args);
        System.out.println("Ending the statisticPublicNumber job");


    }
//    public static void searchFiles() throws URISyntaxException, IOException, InterruptedException {
//        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf, "root");
//        fs.listFiles("true)
//    }

    /**
     * need to convey the InputFile path and OutputFile path,
     * @param args
     * @throws IOException
     */
    public static void statisticPublicNumber(String[]args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.fileinputformat.inputdir",args[0]);
        conf.set("mapreduce.output.fileoutputformat.outputdir",args[1]);
        Job instanceJob = Job.getInstance(conf);
        instanceJob.setJarByClass(hadoopTest.class);
//        instanceJob.setNumReduceTasks(3);
        instanceJob.setMapperClass(five.statisticsPublicNumberMapper.class);
        instanceJob.setMapOutputKeyClass(Text.class);
        instanceJob.setMapOutputValueClass(Text.class);
        // 5 设置最终输出的kV类型
        instanceJob.setReducerClass(five.statisticsPublicNumberReduce.class);
        instanceJob.setOutputKeyClass(Text.class);
        instanceJob.setOutputValueClass(Text.class);
//        // 6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(new JobConf(conf),new Path(args[0]));
        FileOutputFormat.setOutputPath(new JobConf(conf),new Path(args[1]));
        System.out.println(conf.get("mapreduce.output.fileoutputformat.outputdir"));
        boolean b = instanceJob.waitForCompletion(true);
        if(!b){
            System.out.println("The statisticPublicNumber program failed,please connected the author");
            System.exit(-1);
        }
    }
    /**
     * need to convey the InputFile path and OutputFile path,
     * @param args
     * @throws IOException
     */
    public static void averageValue(String[]args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
//        JobConf conf = new JobConf();
//        conf.set("mapreduce.framework.name","local");
//        conf.addResource(new URL("hdfs://master:9000"));
        conf.set("mapreduce.input.fileinputformat.inputdir",args[0]);
        conf.set("mapreduce.output.fileoutputformat.outputdir",args[1]);
        Job instance = Job.getInstance(conf);
        instance.setJarByClass(hadoopTest.class);
        instance.setMapperClass(four.averageGradeMapper.class);
        instance.setReducerClass(four.averageGradeReduce.class);
        instance.setMapOutputKeyClass(Text.class);
        instance.setMapOutputValueClass(IntWritable.class);
        instance.setOutputKeyClass(Text.class);
        instance.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(new JobConf(conf),new Path(args[0]));
        FileOutputFormat.setOutputPath(new JobConf(conf),new Path(args[1]));
        System.out.println(conf.get("mapreduce.output.fileoutputformat.outputdir"));
        boolean b = instance.waitForCompletion(true);
        if(!b){
            System.out.println("the averageValue program error");
            System.exit(-1);
        }

    }
    public static void downLoadFileByJavaAPI(String[]args) throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf, "root");
        if(!fs.exists(new Path(String.format("/Xiyang/%s",args[0])))){
            System.out.println(String.format("/Xiyang/%s not exist ,program exit\n",args[0]));
            System.exit(-1);
        }
        System.out.println(String.format("download the  /Xiyang/%s to current repository %s file\n",args[0],args[0]));
        fs.copyToLocalFile(new Path(String.format("/Xiyang/%s",args[0])),new Path(String.format("./%s",args[0])));
        System.out.println("Completing the download the file\n");
        fs.close();
    }
    public static void uploadFileByJavaAPI(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://master:9000"), conf, "root");
        if(!fs.exists(new Path("/Xiyang"))){
            System.out.println("Cant exist the Xiyang repository,Creating Now.....");
            fs.mkdirs(new Path("/Xiyang"));
        }
        if(fs.exists(new Path(String.format("/Xiyang/%s",args[0])))){
            System.out.println(String.format("delete the origin %s file\n",args[0]));
            fs.delete(new Path(String.format("/Xiyang/%s",args[0])),true);
        }
        System.out.printf("Starting copy the %s to /Xiyang/%s\n",args[0],args[0]);
        fs.copyFromLocalFile(new Path(String.format("./%s",args[0])),new Path(String.format("/Xiyang/%s",args[0])));
        System.out.println("Completing the upload the file\n");

        fs.close();
    }
    public static void mapreduceTest(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.addResource(new URL("hdfs://master:9000"));
        conf.set("mapreduce.input.fileinputformat.inputdir",args[0]);
        conf.set("mapreduce.output.fileoutputformat.outputdir",args[1]);

        Job job = Job.getInstance(conf);

        // 2 设置jar包路径
        job.setJarByClass(hadoopTest.class);

        // 3 关联mapper和reducer
        job.setMapperClass(One.TokenizerMapper.class);
        job.setReducerClass(One.IntSumReducer.class);

        // 4 设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置最终输出的kV类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);


        // 6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(new JobConf(conf),new Path(args[0]));
        FileOutputFormat.setOutputPath(new JobConf(conf),new Path(args[1]));

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        if(!result){
            System.out.println("the mapreduceTest program error");
            System.exit(-1);
        }
    }

    public static void sortResultFromOne(String[]args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.addResource(new URL("hdfs://master:9000"));
        conf.set("mapreduce.input.fileinputformat.inputdir",args[1]);
        conf.set("mapreduce.output.fileoutputformat.outputdir",args[2]);
        Job job = Job.getInstance(conf);
        // 2 设置jar包路径
        job.setJarByClass(hadoopTest.class);

        // 3 关联mapper和reducer
        job.setMapperClass(two.sortTimeByTimes.class);
        // 5 设置最终输出的kV类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 5 设置最终输出的kV类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // 6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(new JobConf(conf),new Path(args[1]));
        FileOutputFormat.setOutputPath(new JobConf(conf),new Path(args[2]));

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        if(!result){
            System.out.println("the sortResultFromOne program error");
            System.exit(-1);
        }
    }

    public static void timePartition(String[]args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.addResource(new URL("hdfs://master:9000"));
        conf.set("mapreduce.input.fileinputformat.inputdir",args[1]);
        conf.set("mapreduce.output.fileoutputformat.outputdir",args[3]);
        Job job = Job.getInstance(conf);
        // 2 设置jar包路径
        job.setJarByClass(hadoopTest.class);

        // 3 关联mapper和reducer
        job.setMapperClass(three.sortTimeByTimes.class);
        job.setPartitionerClass(three.timePartitioner.class);
        job.setNumReduceTasks(5);
        // 5 设置最终输出的kV类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 5 设置最终输出的kV类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // 6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(new JobConf(conf),new Path(args[1]));
        FileOutputFormat.setOutputPath(new JobConf(conf),new Path(args[3]));

        // 7 提交job
        boolean result = job.waitForCompletion(true);
        if(!result){
            System.out.println("the timePartition program error");
            System.exit(-1);
        }

    }

}
