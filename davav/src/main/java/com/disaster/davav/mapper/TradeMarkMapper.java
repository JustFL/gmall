package com.disaster.davav.mapper;

import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

public interface TradeMarkMapper {
    List<Map> selectAmountTopN(@Param("start_time") String startTime, @Param("end_time") String endTime,@Param("topN") int TopN);
}
