package com.naskar.fluentquery.jpa;

import java.util.List;
import java.util.Map;

import javax.persistence.EntityGraph;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.Query;
import javax.persistence.StoredProcedureQuery;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaDelete;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.CriteriaUpdate;
import javax.persistence.metamodel.Metamodel;

public class SharedEntityManager implements EntityManager {
	
	private ThreadLocal<EntityManager> scope;
	
	public SharedEntityManager() {
		this.scope = new ThreadLocal<EntityManager>();
	}
	
	public void set(EntityManager em) {
		this.scope.set(em);
	}
	
	public EntityManager getEntityManager() {
		return this.scope.get();
	}
	
	public void removeEntityManager() {
		this.scope.remove();
	}
	
	// ----
	
	public void persist(Object entity) {
		getEntityManager().persist(entity);
	}

	public <T> T merge(T entity) {
		return getEntityManager().merge(entity);
	}

	public void remove(Object entity) {
		getEntityManager().remove(entity);
	}

	public <T> T find(Class<T> entityClass, Object primaryKey) {
		return getEntityManager().find(entityClass, primaryKey);
	}

	public <T> T find(Class<T> entityClass, Object primaryKey, Map<String, Object> properties) {
		return getEntityManager().find(entityClass, primaryKey, properties);
	}

	public <T> T find(Class<T> entityClass, Object primaryKey, LockModeType lockMode) {
		return getEntityManager().find(entityClass, primaryKey, lockMode);
	}

	public <T> T find(Class<T> entityClass, Object primaryKey, LockModeType lockMode, Map<String, Object> properties) {
		return getEntityManager().find(entityClass, primaryKey, lockMode, properties);
	}

	public <T> T getReference(Class<T> entityClass, Object primaryKey) {
		return getEntityManager().getReference(entityClass, primaryKey);
	}

	public void flush() {
		getEntityManager().flush();
	}

	public void setFlushMode(FlushModeType flushMode) {
		getEntityManager().setFlushMode(flushMode);
	}

	public FlushModeType getFlushMode() {
		return getEntityManager().getFlushMode();
	}

	public void lock(Object entity, LockModeType lockMode) {
		getEntityManager().lock(entity, lockMode);
	}

	public void lock(Object entity, LockModeType lockMode, Map<String, Object> properties) {
		getEntityManager().lock(entity, lockMode, properties);
	}

	public void refresh(Object entity) {
		getEntityManager().refresh(entity);
	}

	public void refresh(Object entity, Map<String, Object> properties) {
		getEntityManager().refresh(entity, properties);
	}

	public void refresh(Object entity, LockModeType lockMode) {
		getEntityManager().refresh(entity, lockMode);
	}

	public void refresh(Object entity, LockModeType lockMode, Map<String, Object> properties) {
		getEntityManager().refresh(entity, lockMode, properties);
	}

	public void clear() {
		getEntityManager().clear();
	}

	public void detach(Object entity) {
		getEntityManager().detach(entity);
	}

	public boolean contains(Object entity) {
		return getEntityManager().contains(entity);
	}

	public LockModeType getLockMode(Object entity) {
		return getEntityManager().getLockMode(entity);
	}

	public void setProperty(String propertyName, Object value) {
		getEntityManager().setProperty(propertyName, value);
	}

	public Map<String, Object> getProperties() {
		return getEntityManager().getProperties();
	}

	public Query createQuery(String qlString) {
		return getEntityManager().createQuery(qlString);
	}

	public <T> TypedQuery<T> createQuery(CriteriaQuery<T> criteriaQuery) {
		return getEntityManager().createQuery(criteriaQuery);
	}

	public Query createQuery(CriteriaUpdate updateQuery) {
		return getEntityManager().createQuery(updateQuery);
	}

	public Query createQuery(CriteriaDelete deleteQuery) {
		return getEntityManager().createQuery(deleteQuery);
	}

	public <T> TypedQuery<T> createQuery(String qlString, Class<T> resultClass) {
		return getEntityManager().createQuery(qlString, resultClass);
	}

	public Query createNamedQuery(String name) {
		return getEntityManager().createNamedQuery(name);
	}

	public <T> TypedQuery<T> createNamedQuery(String name, Class<T> resultClass) {
		return getEntityManager().createNamedQuery(name, resultClass);
	}

	public Query createNativeQuery(String sqlString) {
		return getEntityManager().createNativeQuery(sqlString);
	}

	public Query createNativeQuery(String sqlString, Class resultClass) {
		return getEntityManager().createNativeQuery(sqlString, resultClass);
	}

	public Query createNativeQuery(String sqlString, String resultSetMapping) {
		return getEntityManager().createNativeQuery(sqlString, resultSetMapping);
	}

	public StoredProcedureQuery createNamedStoredProcedureQuery(String name) {
		return getEntityManager().createNamedStoredProcedureQuery(name);
	}

	public StoredProcedureQuery createStoredProcedureQuery(String procedureName) {
		return getEntityManager().createStoredProcedureQuery(procedureName);
	}

	public StoredProcedureQuery createStoredProcedureQuery(String procedureName, Class... resultClasses) {
		return getEntityManager().createStoredProcedureQuery(procedureName, resultClasses);
	}

	public StoredProcedureQuery createStoredProcedureQuery(String procedureName, String... resultSetMappings) {
		return getEntityManager().createStoredProcedureQuery(procedureName, resultSetMappings);
	}

	public void joinTransaction() {
		getEntityManager().joinTransaction();
	}

	public boolean isJoinedToTransaction() {
		return getEntityManager().isJoinedToTransaction();
	}

	public <T> T unwrap(Class<T> cls) {
		return getEntityManager().unwrap(cls);
	}

	public Object getDelegate() {
		return getEntityManager().getDelegate();
	}

	public void close() {
		getEntityManager().close();
	}

	public boolean isOpen() {
		return getEntityManager().isOpen();
	}

	public EntityTransaction getTransaction() {
		return getEntityManager().getTransaction();
	}

	public EntityManagerFactory getEntityManagerFactory() {
		return getEntityManager().getEntityManagerFactory();
	}

	public CriteriaBuilder getCriteriaBuilder() {
		return getEntityManager().getCriteriaBuilder();
	}

	public Metamodel getMetamodel() {
		return getEntityManager().getMetamodel();
	}

	public <T> EntityGraph<T> createEntityGraph(Class<T> rootType) {
		return getEntityManager().createEntityGraph(rootType);
	}

	public EntityGraph<?> createEntityGraph(String graphName) {
		return getEntityManager().createEntityGraph(graphName);
	}

	public EntityGraph<?> getEntityGraph(String graphName) {
		return getEntityManager().getEntityGraph(graphName);
	}

	public <T> List<EntityGraph<? super T>> getEntityGraphs(Class<T> entityClass) {
		return getEntityManager().getEntityGraphs(entityClass);
	}	

}
