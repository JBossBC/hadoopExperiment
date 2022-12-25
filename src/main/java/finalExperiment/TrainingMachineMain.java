package finalExperiment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.Buffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class TrainingMachineMain{
   static String defaultHDFSURI="hdfs://master:9000";
  static  Path trainingMachineFile;
  //fixed resultFile,current cant extend the params
  static  Path resultFile=new Path("/testResult");
    static  Path testDataFile;
  static Configuration conf;
  static FileSystem hadoopFS;
  //训练数据的暂存文件
  static Path tempTrainingResultFile=new Path("/tempMachineLearning");
  static Map<String,Integer> originMachineResult;
  //limit the result element size should less than 1000
  static HashSet<String> rootResult=new HashSet<String>();
    /**
     *  args[0]=trainingMachineFile;args[1]=testFile
     *  temporary don't appoint the eventual outputFile  path(default in the /testResult directory)
     * @param args
     * @throws URISyntaxException
     * @throws IOException
     */
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException, ClassNotFoundException {
        if(args.length!=2){
            System.out.println("you must input the params(trainingData location and testData location in hdfs filesystem )");
            System.exit(1);
        }
        // init hadoop client
           initHadoopFS();
          trainingMachineFile=new Path(args[0]);
          testDataFile=new Path(args[1]);
          //model training
        theMachineLeaningJob();
        //pre-handler the machine learning data
        handlerTheMachineData();
        //Starting the test data analysis
        testDataAnalysisJob();
        //analysis the percent of accuracy
        System.out.println("测试数据的准确度为"+((double)AnalysisDataJob.AnalysisDataMapper.matchNumber/AnalysisDataJob.AnalysisDataMapper.sumNumber));
        System.out.println("Starting the finalizer resource");
        finalizerTempData();
        System.out.println("finalize resource successfully");
    }

    private static void finalizerTempData() throws IOException {
        System.out.println("Deleting the tempTrainingResultFile");
        hadoopFS.delete(tempTrainingResultFile,true);
        System.out.println("Delete the tempTrainingResultFile successfully");
        hadoopFS.close();
    }

    /**
     * 开始测试数据分析
     */
    private static void testDataAnalysisJob() throws IOException, InterruptedException, ClassNotFoundException {
        testDataAnalysisJobBefore();
        //analysis data
        conf.set("mapreduce.input.fileinputformat.inputdir",testDataFile.toString());
        conf.set("mapreduce.output.fileoutputformat.outputdir",resultFile.toString());
        Job instance = Job.getInstance(conf);
        instance.setJarByClass(TrainingMachineMain.class);
        instance.setMapperClass(AnalysisDataJob.AnalysisDataMapper.class);
        instance.setMapOutputValueClass(Text.class);
        instance.setMapOutputKeyClass(IntWritable.class);
        instance.setOutputKeyClass(IntWritable.class);
        instance.setOutputValueClass(Text.class);
        boolean result = instance.waitForCompletion(true);
        if(!result){
            System.out.println("testData analysis error");
            System.exit(1);
        }
        System.out.println("testData execute successfully");
    }

    /**
     * the hooks before test DataAnalysisJob
     * @throws IOException
     */
    public static void testDataAnalysisJobBefore() throws IOException {

     System.out.println("Deleting the resultFile if it exist");
      if(hadoopFS.exists(resultFile)){
          hadoopFS.delete(resultFile,true);
      }
    }

    /**
     * only allow the one partition
     */
    public static void handlerTheMachineData() throws IOException {
        FSDataInputStream open = hadoopFS.open(new Path(tempTrainingResultFile.toString() + "/part-r-00000"));
        BufferedReader bf = new BufferedReader(new InputStreamReader(open));
        //init the MachineData
        System.out.println("Starting  pre-handler the data from machine learning");
        originMachineResult=new HashMap<>();
        String line;
        while(( line=bf.readLine())!=null){
            String[] keyAndValue = line.split("\t");
            //对key进行处理
            String key=keyAndValue[0];
            if(key.contains("_")){
                String[] root_feature = key.split("_");
                rootResult.add(root_feature[0]);
            }
            //保存结果
            originMachineResult.put(keyAndValue[0],Integer.parseInt(keyAndValue[1]));
        }
        //init analysisDataJob static variable
        AnalysisDataJob.AnalysisDataMapper.rootResult= rootResult.toArray(new String[0]);
        //limit the result element size should less than 1000
        AnalysisDataJob.AnalysisDataMapper.originMachineResult= originMachineResult;
        System.out.println("pre-handler the data from machine learning successfully");

    }

    /**
     * 删除存在的临时文件
     */
    public static void HooksBeforeMachineLeaning() throws IOException {
        System.out.println("Deleting the tempTrainingResultFile if it exist");
       if(hadoopFS.exists(tempTrainingResultFile)){
            hadoopFS.delete(tempTrainingResultFile,true);
       }
    }
    private static void theMachineLeaningJob() throws IOException, InterruptedException, ClassNotFoundException {
        //the machine learning data location
        conf.set("mapreduce.input.fileinputformat.inputdir",trainingMachineFile.toString());
        //the machine learning output file location??
        conf.set("mapreduce.output.fileoutputformat.outputdir",tempTrainingResultFile.toString());
        HooksBeforeMachineLeaning();
        Job instanceJob = Job.getInstance(conf);
        instanceJob.setJarByClass(TrainingMachineMain.class);
        instanceJob.setMapperClass(TrainingMachineJob.TrainingStatisticMapper.class);
        instanceJob.setMapOutputKeyClass(Text.class);
        instanceJob.setMapOutputValueClass(IntWritable.class);
        instanceJob.setReducerClass(TrainingMachineJob.TrainingStatisticReduce.class);
        instanceJob.setOutputKeyClass(Text.class);
        instanceJob.setOutputValueClass(IntWritable.class);
        System.out.println("Starting  the Model training,please wait");
        boolean result = instanceJob.waitForCompletion(true);

        if(!result){
            System.out.println("MachineLearning error");
            System.exit(1);
        }
        if(!hadoopFS.exists(tempTrainingResultFile)){
            System.out.println("tempTrainingResultFile cant exists,please connect the owner");
            System.exit(1);
        }
        System.out.println("Model training successfully");
    }
    public static void initHadoopFS() throws URISyntaxException, IOException {
        conf=new Configuration();
        hadoopFS=FileSystem.get(new URI(defaultHDFSURI),conf);
    }
}
