package com.ery.estorm.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ery.estorm.daemon.Stoppable;

public abstract class Chore extends HasThread {
	private final Log LOG = LogFactory.getLog(this.getClass());
	private final Sleeper sleeper;
	protected final Stoppable stopper;

	/**
	 * @param p
	 *            Period at which we should run. Will be adjusted appropriately should we find work and it takes time to complete.
	 * @param stopper
	 *            When {@link Stoppable#isStopped()} is true, this thread will cleanup and exit cleanly.
	 */
	public Chore(String name, final int p, final Stoppable stopper) {
		super(name);
		if (stopper == null) {
			throw new NullPointerException("stopper cannot be null");
		}
		this.sleeper = new Sleeper(p, stopper);
		this.stopper = stopper;
	}

	/**
	 * This constructor is for test only. It allows to create an object and to call chore() on it. There is no sleeper nor stoppable.
	 */
	protected Chore() {
		sleeper = null;
		stopper = null;
	}

	/**
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {
		try {
			boolean initialChoreComplete = false;
			while (!this.stopper.isStopped()) {
				long startTime = System.currentTimeMillis();
				try {
					if (!initialChoreComplete) {
						initialChoreComplete = initialChore();
					} else {
						chore();
					}
				} catch (Exception e) {
					LOG.error("Caught exception", e);
					if (this.stopper.isStopped()) {
						continue;
					}
				}
				this.sleeper.sleep(startTime);
			}
		} catch (Throwable t) {
			LOG.fatal(getName() + "error", t);
		} finally {
			LOG.info(getName() + " exiting");
			cleanup();
		}
	}

	/**
	 * If the thread is currently sleeping, trigger the core to happen immediately. If it's in the middle of its operation, will begin
	 * another operation immediately after finishing this one.
	 */
	public void triggerNow() {
		this.sleeper.skipSleepCycle();
	}

	/*
	 * Exposed for TESTING! calls directly the chore method, from the current thread.
	 */
	public void choreForTesting() {
		chore();
	}

	/**
	 * Override to run a task before we start looping.
	 * 
	 * @return true if initial chore was successful
	 */
	protected boolean initialChore() {
		// Default does nothing.
		return true;
	}

	/**
	 * Look for chores. If any found, do them else just return.
	 */
	protected abstract void chore();

	/**
	 * Sleep for period.
	 */
	protected void sleep() {
		this.sleeper.sleep();
	}

	/**
	 * Called when the chore has completed, allowing subclasses to cleanup any extra overhead
	 */
	protected void cleanup() {
	}
}
