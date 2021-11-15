package org.welyss.mysqlsync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.welyss.mysqlsync.services.TaskService;

@RestController
@SpringBootApplication
public class Application {

	private static final Logger log = LoggerFactory.getLogger(Application.class);

//	@Autowired
//	private AutowireCapableBeanFactory beanFactory;

	@Autowired
	private TaskService taskService;

//	@Bean
//	public ExitCodeGenerator exitCodeGenerator() {
//		System.out.println("*****************************exitCodeGenerator*****************************");
//		LOGGER.info("*****************************exitCodeGenerator-log*****************************");
//		return () -> 42;
//	}

//	@PreDestroy
//	public void destroy() {
//		System.out.println("*****************************destroy*****************************");
//		LOGGER.info("*****************************destroy-log*****************************");
//	}

	@RequestMapping("/metaConvert.do")
	String tableMetaConvert(@RequestParam String source, @RequestParam String target, @RequestParam String table, @RequestParam(required = false) String cluster) {
		String err = taskService.createTable(source, table, target, cluster);
		return err;
	}

	@RequestMapping("/metaCreateInfo.do")
	String generateTable(@RequestParam String source, @RequestParam String target, @RequestParam String table, @RequestParam(required = false) String cluster) {
		String result;
		try {
			result = taskService.generateTable(source, table, target, cluster);
		} catch (Exception e) {
			result = e.getMessage();
		}
		return result;
	}

	public static void main(String[] args) throws Exception {
		SpringApplication.run(Application.class, args);
		log.info("ClickHouse Transport Started.");
	}
}
