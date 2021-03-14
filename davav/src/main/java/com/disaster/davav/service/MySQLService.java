package com.disaster.davav.service;

import java.util.List;
import java.util.Map;

public interface MySQLService {
    List<Map> getTradeMarkAgg(String startTime, String endTime, int topN);
}
