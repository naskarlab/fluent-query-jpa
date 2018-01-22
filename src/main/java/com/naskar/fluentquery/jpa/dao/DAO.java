package com.naskar.fluentquery.jpa.dao;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import com.naskar.fluentquery.Delete;
import com.naskar.fluentquery.Into;
import com.naskar.fluentquery.Query;
import com.naskar.fluentquery.Update;
import com.naskar.fluentquery.binder.BinderSQL;

public interface DAO {
	
	<T> T insert(T o);

	<T> T update(T o);

	<T> T delete(T o);

	<T> List<T> list(Query<T> query);
	
	<T> List<T> list(Query<T> query, Long first, Long max);
	
	List<Map<String, Object>> list(String sql, List<Object> params, Long first, Long max);

	<T> Query<T> query(Class<T> clazz);
	
	<T> T single(Query<T> query);

	<T, R> List<R> list(Query<T> query, Class<R> clazzR);
	
	void nativeSQL(String sql, List<Object> params, RowHandler handler);
	
	void nativeExecute(String sql, List<Object> params, RowHandler handlerKeys);
	
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
	
	<T> Into<T> insert(Class<T> clazz);
	
	<T> Update<T> update(Class<T> clazz);
	
	<T> Delete<T> delete(Class<T> clazz);
	
	<R> BinderSQL<R> binder(Class<R> clazz);
	
	<R, T> void configure(BinderSQL<R> binder, Into<T> into);
	
	<T> void execute(Into<T> into);
	
	<T> void execute(Update<T> update);
	
	<T> void execute(Delete<T> delete);
	
	<R> void execute(BinderSQL<R> binder, R r);

	List<String> getPrimaryKeyFromTable(String tableName);

	List<String> getColumnsFromTable(String tableName);
}
