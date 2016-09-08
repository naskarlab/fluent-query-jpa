package com.naskar.fluentquery.jpa.dao.impl;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;

import com.naskar.fluentquery.Query;
import com.naskar.fluentquery.QueryBuilder;
import com.naskar.fluentquery.converters.NativeSQL;
import com.naskar.fluentquery.converters.NativeSQLResult;
import com.naskar.fluentquery.jpa.dao.DAO;
import com.naskar.fluentquery.jpa.dao.RowHandler;

public class DAOImpl implements DAO {

	private EntityManager em;
	
	private NativeSQL nativeSQL;
	
	public DAOImpl() {
		this.nativeSQL = new NativeSQL();
	}
	
	public void setEm(EntityManager em) {
		this.em = em;
	}
	
	@Override
	public <T> T insert(T o) {
		em.persist(o);
		em.flush();
		return o;
	}
	
	@Override
	public <T> T update(T o) {
		T no = em.merge(o);
		em.flush();
		return no;
	}
	
	@Override
	public <T> T delete(T o) {
		em.remove(em.merge(o));
		em.flush();
		return o;
	}
	
	@SuppressWarnings("unchecked")
	private <T> List<T> list(Class<T> clazz, String sql, List<Object> params, 
			Long first, Long max) {
		javax.persistence.Query q = em.createNativeQuery(sql, clazz);
		addParams(q, params);
		
		Long count = -1L;
		
		if(first != null) {
			q.setFirstResult(first.intValue());
			count = getCount(sql, params);
		}
		
		if(max != null) {
			q.setMaxResults(max.intValue());
		}
		
		List<T> result = q.getResultList(); 
		
		if(first != null) {
			result = new SubListImpl<T>(result, first, max, count);
		}
		
		return result;
	}
	
	private void addParams(PreparedStatement st, List<Object> params) throws SQLException {
		if(params != null) {
			for(int i = 0; i < params.size(); i++) {
				st.setObject(i + 1, params.get(i));
			}
		}
	}

	private void addParams(javax.persistence.Query q, List<Object> params) {
		if(params != null) {
			for(int i = 0; i < params.size(); i++) {
				q.setParameter(i + 1, params.get(i));
			}
		}
	}
	
	private Long getCount(String sql, List<Object> params) {
		String sqlCount = sql;
		
		int pos = sqlCount.indexOf("order by");
		if(pos > -1) {
			sqlCount = sqlCount.substring(0, pos);
		}
		
		javax.persistence.Query q = em.createNativeQuery("SELECT COUNT(*) FROM (" + sqlCount + ")");
		addParams(q, params);
		
		return ((Number)q.getSingleResult()).longValue();
	}
	
	@Override
	public <T> List<T> list(Query<T> query) {
		return list(query, null, null);
	}
	
	@Override
	public <T> List<T> list(Query<T> query, Long first, Long max) {
		NativeSQLResult result = query.to(nativeSQL);
		return list(query.getClazz(), result.sqlValues(), result.values(), first, max);
	}
	
	private Map<String, Field> getFields(Class<?> clazz) {
		Map<String, Field> m = new HashMap<String, Field>();
		
		for(Field f : clazz.getDeclaredFields()) {
			m.put(f.getName().toUpperCase(), f);
		}
		
		return m;
	}
	
	@Override
	public <T, R> List<R> list(Query<T> query, Class<R> clazzR) {
		Map<String, Field> m = getFields(clazzR);
		List<R> l = new ArrayList<R>();
		
		NativeSQLResult result = query.to(nativeSQL);
		nativeSQL(result.sqlValues(), result.values(), (row) -> {
			
			try {
				R r = clazzR.newInstance();
			
				for(Map.Entry<String, Object> e : row.entrySet()) {
					
					Field f = m.get(e.getKey().toUpperCase());
					
					if(f != null) {
						f.setAccessible(true);
						f.set(r, e.getValue());
						f.setAccessible(false);
					}
					
				}
				
				l.add(r);
			
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			
			return true;
		});
		
		return l;
	}
		
	@Override
	public <T> Query<T> query(Class<T> clazz) {
		return new QueryBuilder().from(clazz);
	}
	
	@SuppressWarnings("unchecked")
	private <T> T single(Class<T> clazz, String sql, List<Object> params) {
		try {
			javax.persistence.Query q = em.createNativeQuery(sql, clazz);
			q.setMaxResults(1);
			addParams(q, params);
			return (T) q.getSingleResult();
		} catch(NoResultException nre) {
			return null;
		}
	}
	
	private void nativeSQL(String sql, List<Object> params, RowHandler handler) { 
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			st = em.unwrap(Connection.class).prepareStatement(sql);
			
			addParams(st, params);
			
			rs = st.executeQuery();
			
			ResultSetMetaData md = rs.getMetaData();
			while(rs.next()) {
				Map<String, Object> row = new HashMap<String, Object>();
				
				for(int j = 1; j <= md.getColumnCount(); j++) {
					row.put(md.getColumnName(j), rs.getObject(j));
				}
				
				if(!handler.execute(row)) {
					break;
				}
			}
			
		} catch(Exception e) {
			throw new RuntimeException(e);
			
		} finally {
			
			if(rs != null) {
				try {
					rs.close();
				} catch(Exception e) {
					// TODO: logger
					e.printStackTrace();
				}
			}
			if(st != null) {
				try {
					st.close();
				} catch(Exception e) {
					// TODO: logger
					e.printStackTrace();
				}
			}
		}
	}
	
	@Override
	public <T> T single(Query<T> query) {
		NativeSQLResult result = query.to(nativeSQL);
		return single(query.getClazz(), result.sqlValues(), result.values());
	}

}
