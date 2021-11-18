package org.welyss.mysqlsync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RestController;

@RestController
@SpringBootApplication
public class Application {

	private static final Logger log = LoggerFactory.getLogger(Application.class);

//	@Autowired
//	private AutowireCapableBeanFactory beanFactory;

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

	public static void main(String[] args) throws Exception {
		SpringApplication.run(Application.class, args);
		log.info("ClickHouse Transport Started.");
	}
}
