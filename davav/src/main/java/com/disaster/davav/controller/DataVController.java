package com.disaster.davav.controller;

import com.disaster.davav.service.MySQLService;
import org.apache.ibatis.annotations.Param;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class DataVController {

    @Autowired
    MySQLService service;

    @RequestMapping("getTradeMarkAgg")
    public List<Map> getTradeMarkAgg(@Param("startTime") String startTime, @Param("endTime") String endTime, @Param("topN") int topN) {
        return service.getTradeMarkAgg(startTime, endTime, topN);
    }
}
