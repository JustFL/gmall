package gmall.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * RequestMapping("/applog") 表示访问/applog的一个请求
 * ResponseBody 表示return的是一个网页
 * @RestController = @Controller + @ResponseBody
 *
 * 接受参数可以接受来自网页的参数 还可以接受来自程序的请求体@ResponseBody
 * 网页参数接收处理方法
 * @RequestParam("parameter") String word 接受请求的参数
 * 网页端请求http://localhost:8080/applog?parameter=fuck
 *
 *
 * @RequestMapping("/pageRequest")
 * @ResponseBody
 * public String pageRequest(@RequestParam("parameter") String word){
 *     System.out.println(word);
 *     return word;
 * }
 */

@RestController
@Slf4j
public class LoggerController {

    @Autowired
    KafkaTemplate template;

    @RequestMapping("/applog")
    public String log(@RequestBody String body){
        System.out.println(body);
        //写入本地日志 路径 格式等配置都在logback.xml文件中
        //log.info(body);

        JSONObject jsonObject = JSON.parseObject(body);
        if (jsonObject.getString("start") != null && jsonObject.getString("start").length() != 0){
            template.send("GMALL_LOG_START", body);
        }else {
            template.send("GMALL_LOG_COMMON", body);
        }

        return body;
    }
}
