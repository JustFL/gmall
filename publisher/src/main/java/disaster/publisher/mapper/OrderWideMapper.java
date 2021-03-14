package disaster.publisher.mapper;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface OrderWideMapper {
    public BigDecimal selectOrderTotalAmount(String date);
    public List<Map> selectOrderHourAmount(String date);
}
