package org.welyss.mysqlsync.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.welyss.mysqlsync.services.TaskService;

@RestController
public class taskController {

	public taskController() {}

	@Autowired
	private TaskService taskService;

	@RequestMapping("/metaConvert.do")
	String tableMetaConvert(@RequestParam String source, @RequestParam String target, @RequestParam String table,
			@RequestParam(required = false) String cluster) {
		String err = taskService.createTable(source, table, target, cluster);
		return err;
	}

	@RequestMapping("/metaCreateInfo.do")
	String generateTable(@RequestParam String source, @RequestParam String target, @RequestParam String table,
			@RequestParam(required = false) String cluster) {
		String result;
		try {
			result = taskService.generateTable(source, table, target, cluster);
		} catch (Exception e) {
			result = e.getMessage();
		}
		return result;
	}
}
