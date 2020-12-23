package com.atguigu.gmall0105.gmall0105publisher.controller;
import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0105.gmall0105publisher.service.EsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jxy on 2020/12/22 0022 20:45
 */
@RestController
public class PublicsherController {
    @Autowired
    private EsService esService;

    @GetMapping("/realtime-total")
    public String realtimeTotal(@RequestParam("date")String dt){
        Long dauTotal = esService.getDauTotal(dt);
        List<Map<String,Object>> rsList = new ArrayList();
        Map<String,Object> duaMap = new HashMap<>();
        duaMap.put("id","dau");
        duaMap.put("name","新增日活");
        duaMap.put("value",dauTotal);
        rsList.add(duaMap);

        Map<String,Object> newMidMap = new HashMap<>();
        newMidMap.put("id","new_mid");
        newMidMap.put("name","新增设备");
        newMidMap.put("value","233");
        rsList.add(newMidMap);
        return JSON.toJSONString(rsList);
    }

    @GetMapping("/realtime-hour")
    public String realtimeHour(@RequestParam("id")String id,@RequestParam("date")String date){
        Map dauHour = esService.getDauHour(date);
        String yd = getYd(date);
        Map mayHourPr = esService.getDauHour(yd);
        Map<String,Map<String,Long>> mapResult = new HashMap<>();
        mapResult.put("yesterday",mayHourPr);
        mapResult.put("today",dauHour);
        return JSON.toJSONString(mapResult);
    }

    private static String getYd(String today){
        int i = Integer.parseInt(today);
        int qian = i-1;
        return qian+"";
    }
}
