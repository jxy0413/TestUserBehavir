package com.atguigu.gmall0105logger.controller;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
/**
 * Created by jxy on 2020/12/5 0005 21:47
 */
@RestController //@RestController = @Controller + @ResonseBody
@Slf4j
public class LoggerController {
      //@ResponseBody 绝对方法的返回值是 网页 还是文本
      @Autowired
      private KafkaTemplate kafkaTemplate;

      @RequestMapping("/applog")
      public String applog(@RequestBody JSONObject jsonObject){
          String logJson = jsonObject.toJSONString();
          log.info(logJson);
          if(jsonObject.getString("start")!=null){
              kafkaTemplate.send("GMALL_START",logJson);
          }else{
              kafkaTemplate.send("GMALL_EVENT",logJson);
          }
          return "success";
      }
}
