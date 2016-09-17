package com.naskar.fluentquery.jpa;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.Supplier;

import javax.persistence.EntityManager;

public class EntityManagerInvocationHandler implements InvocationHandler {
	
	private Object target;
	private SharedEntityManager sem;
	private Supplier<EntityManager> entityManagerSupplier;
	
	public EntityManagerInvocationHandler(
		Object target,
		SharedEntityManager sem,
		Supplier<EntityManager> entityManagerSupplier) {
		this.target = target;
		this.sem = sem;
		this.entityManagerSupplier = entityManagerSupplier;
	}
	
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		Object result = null;
		
		EntityManager em = null;
		boolean created = false;
		try {
			em = sem.getEntityManager();
			if(em == null) {
				em = entityManagerSupplier.get();
				em.getTransaction().begin();
				sem.set(em);
				created = true;
			}
			
			result = method.invoke(target, args);
			
			if(created) {
				em.getTransaction().commit();
			}
			
		} catch(Exception e) {
			if(created) {
				if(em != null) {
					try {
						em.getTransaction().rollback();
					} catch(Exception et) {
						// TODO: logger;
						et.printStackTrace();
					}
				}
			}
			
			if(e instanceof InvocationTargetException) {
				throw ((InvocationTargetException)e).getTargetException();
			} else {
				throw e;
			}
			
		} finally {
			if(created) {
				sem.removeEntityManager();
				if(em != null) {
					em.close();
				}
			}
		}
		
		return result;
	}
	
	public Object getTarget() {
		return this.target;
	}

}
