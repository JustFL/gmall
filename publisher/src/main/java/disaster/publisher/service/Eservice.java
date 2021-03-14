package disaster.publisher.service;

import java.io.IOException;
import java.util.Map;

public interface Eservice {
    //日活的总数查询
    Long getDauTotal(String date) throws IOException;
    //日活的分时查询
    Map getDauHour(String date);
}
