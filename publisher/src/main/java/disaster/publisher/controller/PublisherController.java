package disaster.publisher.controller;

import com.alibaba.fastjson.JSON;
import disaster.publisher.service.ClickHouseService;
import disaster.publisher.service.Eservice;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


@RestController
public class PublisherController {

    @Autowired
    Eservice service;

    @Autowired
    ClickHouseService cService;

    @RequestMapping("getOrderAmount")
    public BigDecimal getOrderTotalAmount(@RequestParam("date") String date) {
        return cService.getOrderTotalAmount(date);
    }

    @RequestMapping("getOrderHour")
    public List<Map> getOrderHourAmount(@RequestParam("date") String date) {
        return cService.getOrderHourAmount(date);
    }

    @RequestMapping(value = "realtime-total",method = RequestMethod.GET)
    public String getTotal(@RequestParam("date") String dt) {
        Long dauTotal = null;
        try {
            dauTotal = service.getDauTotal(dt);
        } catch (IOException e) {
            System.out.println("TMD 为啥还要捕获一次");
            e.printStackTrace();
        }

        ArrayList<Map<String, String>> maps = new ArrayList<>();
        HashMap<String, String> dauMap = new HashMap<>();
        dauMap.put("id","dau");
        dauMap.put("name","新增日活");
        dauMap.put("value",String.valueOf(dauTotal));

        HashMap<String, String> midMap = new HashMap<>();
        midMap.put("id","newMid");
        midMap.put("name","新增设备");
        midMap.put("value","233");

        maps.add(dauMap);
        maps.add(midMap);

        String jsonStr = JSON.toJSONString(maps);
        return jsonStr;
    }

    @RequestMapping("realtime-hour")
    public String getDauHour(@RequestParam("date") String dt,@RequestParam("id") String dauId){
        HashMap<String, Map<String, Long>> result = new HashMap<>();
        Map todayData = service.getDauHour(dt);
        Map yesterdayData = service.getDauHour(getYesterday(dt));
        result.put("yesterday",yesterdayData);
        result.put("today",todayData);
        String s = JSON.toJSONString(result);
        return s;
    }

    private String getYesterday(String today){
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date now = null;
        try {
            now = df.parse(today);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Date yesterday = DateUtils.addDays(now, -1);
        String res = df.format(yesterday);
        return res;
    }
}
