package disaster.publisher.service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface ClickHouseService {
    public BigDecimal getOrderTotalAmount(String date);
    public List<Map> getOrderHourAmount(String date);
}
