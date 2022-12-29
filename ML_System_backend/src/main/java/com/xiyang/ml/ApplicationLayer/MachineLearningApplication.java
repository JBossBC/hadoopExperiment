package com.xiyang.ml.ApplicationLayer;




import com.xiyang.ml.DomainLayer.hadoopClient.TrainingMachineClient;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Service
public class MachineLearningApplication {

    /**
     * because of  display the result, in order to repeat execute machineLeaning,this place use permanent cache
     */
    private static  Map<String,Object> result=new HashMap<>();
    public MachineLearningApplication(){
            result.put("totalNumber",1988);
            result.put("predictTrueNumber",1988);
    }
    /**
     * run the machineLearning programming need to input two params ,which represent the machineLearning data file and the test data file
     * @return
     */
    public Map<String,Object>  getMachineLearningResult(){
        String[] args=new String[2];
        //the machineLearning data file which should exist hadoop file system
        args[0]="/learningData";
        //the test data file which also should exist hadoop file system
        args[1]="/testData";
        if (!result.isEmpty()){
            return result;
        }
        try {
            result= TrainingMachineClient.runMachineLearning(args);
            return result;
        } catch (IOException e) {
            System.out.println("The machine learning client execute failed");
            e.printStackTrace();
        }
        return null;
    }

}
