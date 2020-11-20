package org.welyss.mysqlsync;

import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class TaskManager implements CommandLineRunner {
	
	@Autowired
	private Properties configs;
	
	@Autowired
	private Environment env;
	
	@Override
	public void run(String... args) throws Exception {
		System.out.println("-------------------------------------------->" + configs.getProperty("wys"));
		System.out.println("-------------------------------------------->" + env.getProperty("wys"));
		System.out.println("go-------------------------------------------->");
	}
}
