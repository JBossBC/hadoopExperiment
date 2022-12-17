import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class three {
    public static class timePartitioner extends Partitioner<IntWritable, Text> {


        @Override
        public int getPartition(IntWritable intWritable, Text text, int i) {
            System.out.println("text"+text.toString());
            System.out.println("intWritable,"+intWritable);
            String[] split = text.toString().split("-");
            if(split[1].compareTo("02")==0){
                return 2;
            }
            if(split[1].compareTo("01")==0){
                return 1;
            }
            return 0;
        }
    }
    public static class sortTimeByTimes
            extends Mapper<Object, Text, IntWritable, Text> {
        private Text word = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String str=value.toString();
            String[]split= str.split("\t",2);
            word.set(split[1]);
            context.write( new IntWritable(Integer.parseInt(split[0])), new Text(split[1]));
        }
    }
    public static class customCompactor extends WritableComparator {
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            int i = Integer.parseInt(a.toString());
            int j=Integer.parseInt(b.toString());
            return i-j;
        }
    }
}
