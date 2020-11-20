package org.welyss.mysqlsync;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@SpringBootApplication
public class HelloWorld {

	private static final Logger LOGGER = LoggerFactory.getLogger(HelloWorld.class);

	@Autowired
	DatabaseAccountService das;

	@Value("${wys}")
	private String propertytest;

	@Bean
	public ExitCodeGenerator exitCodeGenerator() {
		System.out.println("*****************************exitCodeGenerator*****************************");
		LOGGER.info("*****************************exitCodeGenerator-log*****************************");
		return () -> 42;
	}

	@RequestMapping("/")
	String home() {
		das.display();
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
//		Thread t = new Thread(new Runnable() {
//			@Override
//			public void run() {
//				// TODO Auto-generated method stub
//				System.out.println("run:" + Thread.currentThread().getState());
//			}
//		});
//		System.out.println("before start:" + t.getState().name());
//		t.start();
//		System.out.println("after start:" + t.getState().name());
//		Thread.sleep(3000);
//		System.out.println("after start 3s:" + t.getState().name());
//		if(t.getState().compareTo(State.TERMINATED) == 0) {
//		}
	}
}
