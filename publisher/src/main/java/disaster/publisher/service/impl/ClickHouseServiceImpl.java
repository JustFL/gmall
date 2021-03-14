package disaster.publisher.service.impl;

import disaster.publisher.mapper.OrderWideMapper;
import disaster.publisher.service.ClickHouseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Service
public class ClickHouseServiceImpl implements ClickHouseService {

    @Autowired
    OrderWideMapper mapper;

    @Override
    public BigDecimal getOrderTotalAmount(String date) {
        return mapper.selectOrderTotalAmount(date);
    }

    @Override
    public List<Map> getOrderHourAmount(String date) {
        return mapper.selectOrderHourAmount(date);
    }
}
