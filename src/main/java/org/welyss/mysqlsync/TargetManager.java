package org.welyss.mysqlsync;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class TargetManager implements CommandLineRunner {

//	@Autowired
//	private Environment config;

	public Map<String, Target> targetPool;

	@Value("${sync.working.clickhouses}")
	private String clickhouses;

	/**
	 * 主入口
	 */
	@Override
	public void run(String... args) throws Exception {
		Optional<String> chso = Optional.ofNullable(clickhouses);
		chso.ifPresent((chs -> {
			targetPool = new HashMap<String, Target>((int)(chs.length() / 0.6));
			for(String ch : chs.split(",")) {
				Target target = new Target(ch);
				targetPool.put(ch, target);
				target.start();
			}
		}));
	}
}
