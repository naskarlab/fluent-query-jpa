package com.naskar.fluentquery.jpa.dao;

import java.util.List;

public interface SubList<T> extends List<T> {
	
	List<T> getDelegate();

	Long getFirst();

	Long getMax();

	Long getCount();

}
