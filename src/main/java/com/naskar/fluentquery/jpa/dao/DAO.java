package com.naskar.fluentquery.jpa.dao;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import com.naskar.fluentquery.Query;

public interface DAO {
	
	<T> T insert(T o);

	<T> T update(T o);

	<T> T delete(T o);

	<T> List<T> list(Query<T> query);
	
	<T> List<T> list(Query<T> query, Long first, Long max);

	<T> Query<T> query(Class<T> clazz);
	
	<T> T single(Query<T> query);

	<T, R> List<R> list(Query<T> query, Class<R> clazzR);
	
	void nativeSQL(String sql, List<Object> params, RowHandler handler);

	void nativeExecute(String sql, List<Object> params);
	
	void insert(String table, 
			Map<String, Object> params, 
			BiConsumer<String, List<Object>> call);

	void update(String table, 
			Map<String, Object> params, 
			Map<String, Object> where,
			BiConsumer<String, List<Object>> call);

	void delete(String table, 
			Map<String, Object> where, 
			BiConsumer<String, List<Object>> call);

	List<String> getPrimaryKeyFromTable(String tableName);

	List<String> getColumnsFromTable(String tableName);
}
