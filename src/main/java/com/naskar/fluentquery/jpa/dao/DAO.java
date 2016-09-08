package com.naskar.fluentquery.jpa.dao;

import java.util.List;

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
	
}
