package com.disaster.davav.service.impl;

import com.disaster.davav.mapper.TradeMarkMapper;
import com.disaster.davav.service.MySQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class MySQLServiceImpl implements MySQLService {

    @Autowired
    TradeMarkMapper mapper;

    @Override
    public List<Map> getTradeMarkAgg(String startTime, String endTime, int topN) {
        return mapper.selectAmountTopN(startTime, endTime, topN);
    }
}
