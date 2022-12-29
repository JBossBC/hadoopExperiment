package com.xiyang.ml.DomainLayer.hadoopClient;

import jakarta.annotation.Resource;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 统计各个特征在不同result中的次数
 */
@Resource
public class TrainingMachineJob {
    public static  class TrainingStatisticMapper extends Mapper<Object, Text,Text, IntWritable>{

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(":");
            if(split.length==1){
                split=split[0].split("：");
            }
            if(split.length!=2){
                System.out.println(value+"split error,skipping the line");
                return;
            }
            String result=split[0];
            String features=split[1];
            String []  featuresArr=features.split("\\u0020");
            //排除非中文的逻辑????
            Pattern compile = Pattern.compile("[\u4e00-\u9fa5]");
            int vaildValue=0;
            for (int i=0;i<featuresArr.length;i++){
                //排除非中文的逻辑????
                Matcher matcher = compile.matcher(featuresArr[i]);
                if(!matcher.find()){
                    continue;
                }
                vaildValue++;
                context.write(new Text(featuresArr[i]),new IntWritable(1));
                context.write(new Text(result+"_"+featuresArr[i]),new IntWritable(1));
            }
            context.write(new Text(result),new IntWritable(vaildValue));
            context.write(new Text("totalNumber"),new IntWritable(vaildValue));
        }
    }

    public static class TrainingStatisticReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
           //values length must less than the max size of int64
            int result=0;
            for(IntWritable temp:values){
                result+=temp.get();
            }
           context.write(key,new IntWritable(result));
        }
    }
}
