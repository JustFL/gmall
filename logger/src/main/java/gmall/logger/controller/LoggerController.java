package gmall.logger.controller;

import lombok.extern.log4j.Log4j;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

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
    @RequestMapping("/applog")
    public String log(@RequestBody String body){
        System.out.println(body);
        log.info(body);
        return body;
    }
}
