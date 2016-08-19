package com.naskar.fluentquery.jpa.dao;

import java.util.Map;

public interface RowHandler {
	
	boolean execute(Map<String, Object> row);

}
