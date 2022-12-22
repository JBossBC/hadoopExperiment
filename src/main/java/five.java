import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class five {
    //TODO need to test,maybe result file cant conform the real result
    public static class statisticsPublicNumberMapper extends Mapper<Object ,Text ,Text ,Text >{
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\\u0020");
            context.write(new Text(split[1]),new Text(split[0]));
        }
    }
    public static class statisticsPublicNumberReduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            StringBuilder sb=new StringBuilder();
            for(Text value:values){
                sb.append(value);
                sb.append(",");
            }
            context.write(key,new Text(sb.toString()));
        }
    }



}
