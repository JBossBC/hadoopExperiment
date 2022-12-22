import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class four {

    public static class averageGradeMapper extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String [] strArr=line.split("\\u0020\\u0020\\u0020\\u0020");
            System.out.println(strArr[0]);
            context.write(new Text(strArr[0]),new IntWritable(Integer.parseInt(strArr[1])));
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
