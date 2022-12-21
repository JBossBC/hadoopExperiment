import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class five {
    //TODO need to test,maybe result file cant conform the real result
    public static class statisticsPublicNumberMapper extends Mapper<Text ,Text ,Text ,Text >{
        @Override
        protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(value,key);
        }
    }



}
