package com.xiyang.ml.UserLayer;


import com.xiyang.ml.ApplicationLayer.MachineLearningApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;


import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;

@Controller
public class machineLearning {

     @Autowired
     private MachineLearningApplication MLApplication;
     @ResponseBody
     @RequestMapping(method = RequestMethod.GET,value = "/machineLearningResult/get" )
     public String getMachineLearningResult() throws IOException {

         Map<String,Object> result= MLApplication.getMachineLearningResult();
         if(result==null){
             return ResponseUtil.getFailedResponse();

         }
         return ResponseUtil.getSuccessResponse(result);

     }
}
