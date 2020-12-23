package com.atguigu.gmall0105.gmall0105publisher.service;

import java.util.Map;

/**
 * Created by jxy on 2020/12/22 0022 20:46
 */
public interface EsService {
    public Long getDauTotal(String date);

    public Map getDauHour(String date);
}
