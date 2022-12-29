package com.xiyang.ml.UserLayer;

import com.alibaba.fastjson2.JSON;
import jakarta.annotation.Resource;

import java.util.Map;
@Resource
public class ResponseUtil {
    public String result;
    public String message;
    public Map<String,Object> data;
    public ResponseUtil(String result,String message,Map<String,Object> data){
        this.data=data;
        this.result=result;
        this.message=message;
    }
    public ResponseUtil(){
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public static String  getSuccessResponse(Map<String,Object> result){
        ResponseUtil basicResponse = new ResponseUtil("true","返回成功",result);
        return JSON.toJSONString(basicResponse);
    }
    public static String getFailedResponse(){
        ResponseUtil basicResponse = new ResponseUtil("fail","返回失败",null);
        return JSON.toJSONString(basicResponse);
    }
}
