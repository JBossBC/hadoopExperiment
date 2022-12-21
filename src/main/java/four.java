import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class four {

    public static class averageGradeMapper extends Mapper<Text,IntWritable, Text,IntWritable>{
        @Override
        protected void map(Text key, IntWritable value, Mapper<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            context.write(key,value);
        }
    }
    public static class averageGradeReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum=0;
            int times=0;
            for (IntWritable val:values){
                sum+=val.get();
                times++;
            }
            context.write(key,new IntWritable(sum/times));
        }
    }
}
