package com.naskar.fluentquery.jpa.dao.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.NoResultException;

import org.eclipse.persistence.config.QueryHints;
import org.eclipse.persistence.config.ResultType;

import com.naskar.fluentquery.Delete;
import com.naskar.fluentquery.DeleteBuilder;
import com.naskar.fluentquery.InsertBuilder;
import com.naskar.fluentquery.Into;
import com.naskar.fluentquery.Query;
import com.naskar.fluentquery.QueryBuilder;
import com.naskar.fluentquery.Update;
import com.naskar.fluentquery.UpdateBuilder;
import com.naskar.fluentquery.binder.BinderSQL;
import com.naskar.fluentquery.binder.BinderSQLBuilder;
import com.naskar.fluentquery.converters.NativeSQL;
import com.naskar.fluentquery.converters.NativeSQLDelete;
import com.naskar.fluentquery.converters.NativeSQLInsertInto;
import com.naskar.fluentquery.converters.NativeSQLResult;
import com.naskar.fluentquery.converters.NativeSQLUpdate;
import com.naskar.fluentquery.impl.Convention;
import com.naskar.fluentquery.jpa.dao.DAO;
import com.naskar.fluentquery.jpa.dao.RowHandler;

public class DAOImpl implements DAO {

	private EntityManager em;
	
	private NativeSQL nativeSQL;
	private QueryBuilder queryBuilder;
	
	private NativeSQLInsertInto insertSQL;
	private InsertBuilder insertBuilder;
	
	private NativeSQLUpdate updateSQL;
	private UpdateBuilder updateBuilder;
	
	private NativeSQLDelete deleteSQL;
	private DeleteBuilder deleteBuilder;
	
	private BinderSQLBuilder binderBuilder;
	
	private static final List<Integer> BINARY_TYPES = Arrays.asList(
		Types.BINARY, Types.LONGVARBINARY, Types.VARBINARY
	);
	
	public DAOImpl() {
		this.nativeSQL = new NativeSQL();
		this.queryBuilder = new QueryBuilder();
		
		this.insertSQL = new NativeSQLInsertInto();
		this.insertBuilder = new InsertBuilder();
		
		this.updateSQL = new NativeSQLUpdate();
		this.updateBuilder = new UpdateBuilder();
		
		this.deleteSQL = new NativeSQLDelete();
		this.deleteBuilder = new DeleteBuilder();
		
		this.binderBuilder = new BinderSQLBuilder();
	}
	
	public void setEm(EntityManager em) {
		this.em = em;
	}
	
	protected EntityManager getEm() {
		return em;
	}
	
	public void setConvention(Convention convention) {
		this.nativeSQL.setConvention(convention);
		this.insertSQL.setConvention(convention);
		this.updateSQL.setConvention(convention);
		this.deleteSQL.setConvention(convention);
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
	
	@Override
	public <T> Into<T> insert(Class<T> clazz) {
		return insertBuilder.into(clazz);
	}
	
	@Override
	public <T> Update<T> update(Class<T> clazz) {
		return updateBuilder.entity(clazz);
	}
	
	@Override
	public <T> Delete<T> delete(Class<T> clazz) {
		return deleteBuilder.entity(clazz);
	}
	
	@Override
	public <T> void execute(Into<T> into) {
		NativeSQLResult result = into.to(insertSQL);
		nativeExecute(result.sqlValues(), result.values(), null);
	}
	
	@Override
	public <T> void execute(Update<T> update) {
		NativeSQLResult result = update.to(updateSQL);
		nativeExecute(result.sqlValues(), result.values(), null);
	}
	
	@Override
	public <T> void execute(Delete<T> delete) {
		NativeSQLResult result = delete.to(deleteSQL);
		nativeExecute(result.sqlValues(), result.values(), null);
	}
	
	@Override
	public <R> void execute(BinderSQL<R> binder, R r) {
		NativeSQLResult result = binder.bind(r);
		nativeExecute(result.sqlValues(), result.values(), null);
	}
	
	@Override
	public <R> BinderSQL<R> binder(Class<R> clazz) {	
		return binderBuilder.from(clazz);
	}
	
	@Override
	public <R, T> void configure(BinderSQL<R> binder, Into<T> into) {
		binder.configure(into.to(insertSQL));
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
				m.put(k.toString().toLowerCase(), v);
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
					st.setTimestamp(i + 1, new java.sql.Timestamp(((java.util.Date)o).getTime()));
				} else if(o instanceof File) {
					try {
						st.setBinaryStream(i + 1, new FileInputStream((File)o));
					} catch(Exception e) {
						throw new RuntimeException(e);
					}
				} else if(o instanceof InputStream) {
					try {
						st.setBinaryStream(i + 1, (InputStream)o);
					} catch(Exception e) {
						throw new RuntimeException(e);
					}				
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
		
		/*
		TODO: verificar se ainda é necessario
		int pos = sqlCount.indexOf("order by");
		if(pos > -1) {
			sqlCount = sqlCount.substring(0, pos);
		}
		*/
		
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
			m.put(f.getName().toLowerCase(), f);
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
					
					Field f = m.get(e.getKey().toLowerCase());
					
					if(f != null) {
						f.setAccessible(true);
						f.set(r, e.getValue());
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
		return queryBuilder.from(clazz);
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
			
			forEachHandler(rs, handler);
			
		} catch(Exception e) {
			// TODO: logger
			System.out.println("SQL:" + sql + "\nParams:" + params);
			e.printStackTrace();
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

	private void forEachHandler(ResultSet rs, RowHandler handler) throws SQLException {
		ResultSetMetaData md = rs.getMetaData();
		while(rs.next()) {
			Map<String, Object> row = new HashMap<String, Object>();
			
			for(int j = 1; j <= md.getColumnCount(); j++) {
				if(BINARY_TYPES.contains(md.getColumnType(j))) {
					row.put(md.getColumnName(j), rs.getBinaryStream(j));
				} else {
					row.put(md.getColumnName(j), rs.getObject(j));
				}
			}
			
			if(!handler.execute(row)) {
				break;
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
	public void nativeExecute(String sql, List<Object> params, RowHandler handlerKeys) { 
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			st = em.unwrap(Connection.class)
					.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			
			addParams(st, params);
			
			// TODO: logger
			// System.out.println("SQL:" + sql + " params: " + params);
			
			st.executeUpdate();

			if(handlerKeys != null) {
				rs = st.getGeneratedKeys();
				if(rs != null) {
					forEachHandler(rs, handlerKeys);
				}
			}
			
		} catch(Exception e) {
			
			// TODO: logger
			System.out.println("SQL:" + sql + " params: " + params);
			
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
	public List<String> getColumnsFromTable(String tableName) {
		List<String> names = new ArrayList<String>();
		
		Connection conn = null;
		ResultSet rs = null;
		try {
			conn = em.unwrap(Connection.class);
			
			DatabaseMetaData meta = conn.getMetaData();
			String[] tableAttrs = getTableAttrs(tableName.toLowerCase());
			rs = meta.getColumns(null, tableAttrs[0], tableAttrs[1], null);
			
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
			String[] tableAttrs = getTableAttrs(tableName.toLowerCase());
			rs = meta.getPrimaryKeys(null, tableAttrs[0], tableAttrs[1]);
			
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

	private String[] getTableAttrs(String tableName) {
		String catalog = null;
		String table = tableName;
		
		if(tableName.contains(".")) {
			String[] parts = tableName.split(Pattern.quote("."));
			if(parts.length == 2) {
				catalog = parts[0];
				table = parts[1];
			}
		}
		
		return new String[] { catalog, table };
	}

}
