package com.naskar.fluentquery.jpa.dao.impl;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import com.naskar.fluentquery.jpa.dao.SubList;

public class SubListImpl<T> implements List<T>, SubList<T> {
	
	private List<T> delegate;
	private Long first;
	private Long max;
	private Long count;
	
	public SubListImpl(List<T> delegate, Long first, Long max, Long count) {
		this.delegate = delegate;
		this.first = first;
		this.max = max;
		this.count = count;
	}

	public List<T> getDelegate() {
		return delegate;
	}
	
	public Long getFirst() {
		return first;
	}

	public Long getMax() {
		return max;
	}

	public Long getCount() {
		return count;
	}

	// ----- delegate methods -----------
	public void forEach(Consumer<? super T> action) {
		delegate.forEach(action);
	}

	public int size() {
		return delegate.size();
	}

	public boolean isEmpty() {
		return delegate.isEmpty();
	}

	public boolean contains(Object o) {
		return delegate.contains(o);
	}

	public Iterator<T> iterator() {
		return delegate.iterator();
	}

	public Object[] toArray() {
		return delegate.toArray();
	}

	public <E> E[] toArray(E[] a) {
		return delegate.toArray(a);
	}

	public boolean add(T e) {
		return delegate.add(e);
	}

	public boolean remove(Object o) {
		return delegate.remove(o);
	}

	public boolean containsAll(Collection<?> c) {
		return delegate.containsAll(c);
	}

	public boolean addAll(Collection<? extends T> c) {
		return delegate.addAll(c);
	}

	public boolean addAll(int index, Collection<? extends T> c) {
		return delegate.addAll(index, c);
	}

	public boolean removeAll(Collection<?> c) {
		return delegate.removeAll(c);
	}

	public boolean retainAll(Collection<?> c) {
		return delegate.retainAll(c);
	}

	public void replaceAll(UnaryOperator<T> operator) {
		delegate.replaceAll(operator);
	}

	public boolean removeIf(Predicate<? super T> filter) {
		return delegate.removeIf(filter);
	}

	public void sort(Comparator<? super T> c) {
		delegate.sort(c);
	}

	public void clear() {
		delegate.clear();
	}

	public boolean equals(Object o) {
		return delegate.equals(o);
	}

	public int hashCode() {
		return delegate.hashCode();
	}

	public T get(int index) {
		return delegate.get(index);
	}

	public T set(int index, T element) {
		return delegate.set(index, element);
	}

	public void add(int index, T element) {
		delegate.add(index, element);
	}

	public Stream<T> stream() {
		return delegate.stream();
	}

	public T remove(int index) {
		return delegate.remove(index);
	}

	public Stream<T> parallelStream() {
		return delegate.parallelStream();
	}

	public int indexOf(Object o) {
		return delegate.indexOf(o);
	}

	public int lastIndexOf(Object o) {
		return delegate.lastIndexOf(o);
	}

	public ListIterator<T> listIterator() {
		return delegate.listIterator();
	}

	public ListIterator<T> listIterator(int index) {
		return delegate.listIterator(index);
	}

	public List<T> subList(int fromIndex, int toIndex) {
		return delegate.subList(fromIndex, toIndex);
	}

	public Spliterator<T> spliterator() {
		return delegate.spliterator();
	}

}
