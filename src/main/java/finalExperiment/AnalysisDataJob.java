package finalExperiment;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AnalysisDataJob {
    public static class AnalysisDataMapper extends Mapper<Object, Text,Text, Text>{
        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
//            Map<String, Integer> machineResult = TrainingMachineMain.origiMachineResult;
//            machineResult.forEach((key,values)->{
//                try {
//                    context.write(new Text(key),new Text(Integer.toString(values)));
//                } catch (IOException e) {
//                    e.printStackTrace();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            });
        }
         public static String [] rootResult;
        static int currentNumber=1;
        static Map<String,Integer> originMachineResult;
        static int sumNumber;
        static int matchNumber;
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            sumNumber++;
            String line = value.toString();
            String[] split = line.split(":");
            if(split.length==1) {
                split = split[0].split("：");
            }
                if(split.length!=2){
                    System.out.println(value+"split error,skipping the line");
                    return;
                }

            String currentResult=split[0];
            String [] inputFeatures=split[1].split("\\u0020");
            String maxResult="default";
            double maxValue=Double.MIN_VALUE;
            //排除非中文的逻辑????
            Pattern compile = Pattern.compile("[\u4e00-\u9fa5]");
            for(int i=0;i<rootResult.length;i++){
                String currentMaybe=rootResult[i];
                double tempResult=((double)originMachineResult.get(currentMaybe)/originMachineResult.get("totalNumber"));
                for (int j=0;j<inputFeatures.length;j++){
                     String tempFeature=inputFeatures[j];
                    Matcher matcher = compile.matcher(tempFeature);
                    if(!matcher.find()){
                        continue;
                    }
                    if((!originMachineResult.containsKey((currentMaybe+"_"+tempFeature))||(!originMachineResult.containsKey(tempFeature)))){
                        tempResult=0;
                        break;
                    }
                    tempResult*=((double)originMachineResult.get(currentMaybe+"_"+tempFeature)/originMachineResult.get(tempFeature));
                }
                if (maxValue<tempResult){
                    maxValue=tempResult;
                    maxResult=currentMaybe;
                }
            }
            context.write(new Text(Integer.toString(currentNumber)),new Text(maxResult));
            if(currentResult.compareTo(maxResult)==0){
                matchNumber++;
            }
            currentNumber++;
        }
    }
   public static class sortResultMapper extends Mapper<Object,Text,Text,Text>{
       @Override
       protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
           String[] split = value.toString().split("\t");
           context.write(new Text(split[0]),new Text(split[1]));
       }
   }
}
