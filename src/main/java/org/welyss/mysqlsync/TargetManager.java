package org.welyss.mysqlsync;

import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class TargetManager implements CommandLineRunner {

//	@Autowired
//	private Environment config;

	public String aa = "wystest";
	public Map<String, Target> targetPool;

	@Value("${sync.working.clickhouses}")
	private String clickhouses;

	/**
	 * 主入口
	 */
	@Override
	public void run(String... args) throws Exception {
		Optional<String> chso = Optional.ofNullable(clickhouses);
//		chso.ifPresent((chs -> {
//			targetPool = new HashMap<String, Target>((int)(chs.length() / 0.6));
//			for(String ch : chs.split(",")) {
//				String host = config.getProperty("clickhouse." + ch + ".host");
//				if (host != null) {
//					int port = Integer.parseInt(config.getProperty("clickhouse." + ch + ".port", "9004"));
//					String schema = config.getProperty("clickhouse." + ch + ".schema", "default");
//					String user = config.getProperty("clickhouse." + ch + ".user", "test");
//					String password = config.getProperty("clickhouse." + ch + ".password", "test123");
//					Target target = new Target(ch, host, port, schema, user, password);
//					targetPool.put(ch, target);
//				} else {
//					throw new RuntimeException("host is required");
//				}
//			}
//		}));
	}
}
