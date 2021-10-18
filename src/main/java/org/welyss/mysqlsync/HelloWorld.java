package org.welyss.mysqlsync;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.welyss.mysqlsync.db.CHDataSourceConfig;

@RestController
@SpringBootApplication
public class HelloWorld {

	private static final Logger LOGGER = LoggerFactory.getLogger(HelloWorld.class);
	@Value("${wys}")
	private String propertytest;

	@Autowired
	private AutowireCapableBeanFactory beanFactory;

	@Autowired
	private CHDataSourceConfig yamlProperties;

	@Bean
	public ExitCodeGenerator exitCodeGenerator() {
		System.out.println("*****************************exitCodeGenerator*****************************");
		LOGGER.info("*****************************exitCodeGenerator-log*****************************");
		return () -> 42;
	}

	@RequestMapping("/")
	String home() {
		yamlProperties.getDatasource().entrySet().forEach((e) -> {
			System.out.println(e.getKey() + ":");
			e.getValue().entrySet().forEach((ev)->{
				System.out.println(ev.getKey() + ": " + ev.getValue());
			});
		});
//		Collection<Object[]> list = testRepository.getList();
//		System.out.println(list.size());
//		System.out.println(tm.aa);
		System.out.println("property wys is:" + propertytest);
		return "Hello World!";
	}

	@PreDestroy
	public void destroy() {
		System.out.println("*****************************destroy*****************************");
		LOGGER.info("*****************************destroy-log*****************************");
	}

	public static void main(String[] args) throws Exception {
		SpringApplication.run(HelloWorld.class, args);
	}
}
