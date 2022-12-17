import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInputStream;
import java.io.IOException;

public class two {
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
