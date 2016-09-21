package com.naskar.fluentquery.jpa.dao.impl;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;

import org.eclipse.persistence.config.QueryHints;
import org.eclipse.persistence.config.ResultType;

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
	@Override
	public List<Map<String, Object>> list(String sql, List<Object> params, Long first, Long max) {
		javax.persistence.Query q = em.createNativeQuery(sql);
		q.setHint(QueryHints.RESULT_TYPE, ResultType.Map);
		
		addParams(q, params);
		
		Long count = -1L;
		
		if(first != null) {
			q.setFirstResult(first.intValue());
			count = getCount(sql, params);
		}
		
		if(max != null) {
			q.setMaxResults(max.intValue());
		}
		
		
		List<Map<String, Object>> dbResult = q.getResultList();

		
		final List<Map<String, Object>> result = new ArrayList<Map<String, Object>>(dbResult.size());
		dbResult.forEach(i -> {
			Map<String, Object> m = new HashMap<String, Object>();
			i.forEach((Object k, Object v) -> {
				m.put(k.toString(), v);
			});
			result.add(m);
		});
		
		if(first != null) {
			return new SubListImpl<Map<String, Object>>(result, first, max, count);
		} else {
			return result;
		}
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
				Object o = params.get(i);
				if(o instanceof Date) {
					st.setDate(i + 1, new java.sql.Date(((java.util.Date)o).getTime()));
				} else {
					st.setObject(i + 1, o);
				}
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
		
		javax.persistence.Query q = em.createNativeQuery("SELECT COUNT(*) FROM (" + sqlCount + ") _v");
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
	
	@Override
	public void nativeSQL(String sql, List<Object> params, RowHandler handler) { 
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
	
	@Override
	public void insert(String table, 
			Map<String, Object> params, 
			BiConsumer<String, List<Object>> call) {
		
		List<String> columns = new ArrayList<String>();
		List<Object> values = new ArrayList<Object>();
		List<String> p = new ArrayList<String>();
		
		params.forEach((k, v) -> {
			columns.add(k);
			values.add(v);
			p.add("?");
		});
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("insert into ");
		sb.append(table);
		sb.append(columns.stream()
			     .map(i -> i.toString())
			     .collect(Collectors.joining(", ", " (", ")")));
		sb.append(p.stream()
			     .map(i -> i.toString())
			     .collect(Collectors.joining(", ", " values (", ")")));
		
		call.accept(sb.toString(), values);
	}
	
	@Override
	public void update(String table,
			Map<String, Object> params, 
			Map<String, Object> where,
			BiConsumer<String, List<Object>> call) {
		
		List<String> columns = new ArrayList<String>();
		List<String> columnsWhere = new ArrayList<String>();
		List<Object> values = new ArrayList<Object>();
		
		params.forEach((k, v) -> {
			columns.add(k + " = ? ");
			values.add(v);
		});
		
		where.forEach((k, v) -> {
			columnsWhere.add(k + " = ? ");
			values.add(v);
		});
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("update ");
		sb.append(table);
		sb.append(columns.stream()
			     .map(i -> i.toString())
			     .collect(Collectors.joining(", ", " set ", " ")));
		sb.append(columnsWhere.stream()
			     .map(i -> i.toString())
			     .collect(Collectors.joining(" and ", "where ", "")));
		
		call.accept(sb.toString(), values);
	}
	
	@Override
	public void delete(String table, 
			Map<String, Object> where,
			BiConsumer<String, List<Object>> call) {
		
		List<String> columnsWhere = new ArrayList<String>();
		List<Object> values = new ArrayList<Object>();
		
		where.forEach((k, v) -> {
			columnsWhere.add(k + " = ? ");
			values.add(v);
		});
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("delete from ");
		sb.append(table);
		sb.append(columnsWhere.stream()
			     .map(i -> i.toString())
			     .collect(Collectors.joining(" and ", " where ", " ")));
		
		call.accept(sb.toString(), values);
	}
	
	@Override
	public void nativeExecute(String sql, List<Object> params) { 
		PreparedStatement st = null;
		try {
			st = em.unwrap(Connection.class).prepareStatement(sql);
			
			addParams(st, params);
			
			st.executeUpdate();
			
		} catch(Exception e) {
			throw new RuntimeException(e);
			
		} finally {
			
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
	public List<String> getColumnsFromTable(String tableName) {
		List<String> names = new ArrayList<String>();
		
		Connection conn = null;
		ResultSet rs = null;
		try {
			conn = em.unwrap(Connection.class);
			
			DatabaseMetaData meta = conn.getMetaData();
			rs = meta.getColumns(null, null, tableName.toLowerCase(), null);
			
			while (rs.next()) {
				names.add(rs.getString("COLUMN_NAME").toLowerCase());
			}
			
		} catch(Exception e) {
			throw new RuntimeException(e);
			
		} finally {
			
			try {
				if(rs != null) {
					rs.close();
				}
			} catch(Exception e) {
				// TODO: logger
				e.printStackTrace();
			}
		}
			
		return names;
	}
	
	@Override
	public List<String> getPrimaryKeyFromTable(String tableName) {
		List<String> names = new ArrayList<String>();
		
		Connection conn = null;
		ResultSet rs = null;
		try {
			conn = em.unwrap(Connection.class);
			
			DatabaseMetaData meta = conn.getMetaData();
			rs = meta.getPrimaryKeys(null, null, tableName.toLowerCase());
			
			while (rs.next()) {
				names.add(rs.getString("COLUMN_NAME").toLowerCase());
			}
			
		} catch(Exception e) {
			throw new RuntimeException(e);
			
		} finally {
			
			try {
				if(rs != null) {
					rs.close();
				}
			} catch(Exception e) {
				// TODO: logger
				e.printStackTrace();
			}
		}
			
		return names;
	}

}
