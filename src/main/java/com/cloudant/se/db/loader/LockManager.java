package com.cloudant.se.db.loader;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

public class LockManager {
	private static ConcurrentMap<String, Semaphore>		locksAvailable	= new ConcurrentHashMap<>();
	private static ThreadLocal<Map<String, Semaphore>>	locksHeld		= new ThreadLocal<>();

	public static void acquire(String key) throws InterruptedException {
		setupLocksHeld();

		locksAvailable.putIfAbsent(key, new Semaphore(1));
		Semaphore s = locksAvailable.get(key);

		s.acquire();
		locksHeld.get().put(key, s);
	}

	public static void release(String key) {
		setupLocksHeld();

		if (locksHeld.get().containsKey(key)) {
			Semaphore s = locksHeld.get().remove(key);
			s.release();
		} else {
			throw new RuntimeException("Asked to release a lock on \"" + key + "\" but lock was not held");
		}
	}

	private static void setupLocksHeld() {
		if (locksHeld.get() == null) {
			locksHeld.set(new HashMap<String, Semaphore>());
		}
	}
}
