/*
 * Copyright (c) 2012, someone All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1.Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer. 2.Redistributions in binary
 * form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided
 * with the distribution. 3.Neither the name of the Happyelements Ltd. nor the
 * names of its contributors may be used to endorse or promote products derived
 * from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.server.jobtracker.TaskTracker;

/**
 * assign task in round robin fashion
 * 
 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
 * @author <a href="mailto:zzdtsv@gmail.com">zizon</a>
 */
public class RoundRobinScheduler extends TaskScheduler {

	private static final Log LOGGER = LogFactory
			.getLog(RoundRobinScheduler.class);

	private static final List<Task> EMPTY_TASK_LIST = Collections
			.unmodifiableList(new ArrayList<Task>(0));

	/**
	 * shortcut task selector
	 * 
	 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
	 */
	public static enum TaskSelector {
		Map, Reduce;

		/**
		 * select a task accroding self type
		 * 
		 * @param job
		 *            the job tips
		 * @param status
		 *            the tasktracker status
		 * @param cluster_size
		 *            the cluster size
		 * @param uniq_hosts
		 *            uniq hosts
		 * @return the task selected ,null if none
		 * @throws IOException
		 *             throw when obtain task fail
		 */
		Task select(JobInProgress job, TaskTrackerStatus status,
				int cluster_size, int uniq_hosts) throws IOException {
			switch (this) {
			case Map:
				return job.obtainNewMapTask(status, cluster_size, uniq_hosts);
			case Reduce:
				return job.getStatus().mapProgress() > job.getJobConf()
						.getFloat("mapred.reduce.slowstart.completed.maps",
								0.7f)
						|| job.desiredMaps() <= 0 // some job may not have
													// map tasks
				? job.obtainNewReduceTask(status, cluster_size, uniq_hosts)
						: null;
			}
			return null;
		};
	}

	private NavigableSet<JobInProgress> jobs = new ConcurrentSkipListSet<JobInProgress>(
			new Comparator<JobInProgress>() {
				private int translatePriority(JobPriority priority) {
					switch (priority) {
					case VERY_HIGH:
						return 0;
					case HIGH:
						return 1;
					case NORMAL:
						return 2;
					case LOW:
						return 3;
					case VERY_LOW:
						return 4;
					default:
						return 5;
					}
				}

				@Override
				public int compare(JobInProgress o1, JobInProgress o2) {
					// first fall back
					if (o1.getJobID().equals(o2.getJobID())) {
						return 0;
					}

					// then compare priority
					int diff = this.translatePriority(o1.getPriority())
							- this.translatePriority(o2.getPriority());
					if (diff == 0) {
						return o1.getJobID().compareTo(o2.getJobID());
					} else {
						return diff;
					}
				}
			});

	private Queue<JobInProgress> initialize_queue;
	private Thread async_initialize_thread;

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.hadoop.mapred.TaskScheduler#start()
	 */
	@Override
	public void start() throws IOException {
		super.start();

		// initialize queue
		this.initialize_queue = new ConcurrentLinkedQueue<JobInProgress>();

		// start or create if needed
		if (this.async_initialize_thread == null
				|| !this.async_initialize_thread.isAlive()) {
			this.async_initialize_thread = new Thread("async-initialize-job") {

				@Override
				public void run() {
					for (;;) {
						try {
							Queue<JobInProgress> queue = RoundRobinScheduler.this.initialize_queue;
							if (queue == null) {
								RoundRobinScheduler.LOGGER
										.info("no queue found,quit job async initialize thread");
								break;
							}

							Iterator<JobInProgress> iterator = queue.iterator();
							while (iterator.hasNext()) {
								JobInProgress job = iterator.next();
								if (job != null) {
									try {
										RoundRobinScheduler.this.taskTrackerManager
												.initJob(job);

										if (!RoundRobinScheduler.this.jobs
												.add(job)) {
											RoundRobinScheduler.LOGGER
													.error("fail to register job:"
															+ job.getJobID());
										}
									} catch (Exception e) {
										RoundRobinScheduler.LOGGER.error(
												"fail to initialize job:"
														+ job.getJobID(), e);
										continue;
									}
								} else {
									RoundRobinScheduler.LOGGER
											.warn("get null job in progress in job async initialize thread");
								}

								iterator.remove();
							}

							// pause for a while
							LockSupport.parkNanos(1000000000L);
						} catch (Throwable e) {
							RoundRobinScheduler.LOGGER
									.warn("unexpcted error for job async initialzie thread,resume",
											e);
						}
					}
				}

				@Override
				public void start() {
					this.setDaemon(true);
					super.start();
				}
			};
			this.async_initialize_thread.start();
		}

		RoundRobinScheduler.LOGGER.info("start round robin scheduler");
		this.taskTrackerManager
				.addJobInProgressListener(new JobInProgressListener() {
					@Override
					public void jobUpdated(JobChangeEvent event) {
						RoundRobinScheduler.LOGGER
								.info("get an event:" + event);
						if (event instanceof JobStatusChangeEvent) {
							// job status change,should check here if need to
							// remove job from job set in progress
							JobStatusChangeEvent status = (JobStatusChangeEvent) event;
							if (status.getEventType() == JobStatusChangeEvent.EventType.RUN_STATE_CHANGED) {
								// job is done remove it
								switch (status.getNewStatus().getRunState()) {
								case JobStatus.FAILED:
								case JobStatus.KILLED:
								case JobStatus.SUCCEEDED:
									this.jobRemoved(status.getJobInProgress());
									break;
								}
							}
						}
					}

					@Override
					public void jobRemoved(JobInProgress job) {
						RoundRobinScheduler.LOGGER.info("remove job	" + job);
						RoundRobinScheduler.this.jobs.remove(job);
					}

					@Override
					public void jobAdded(final JobInProgress job)
							throws IOException {
						RoundRobinScheduler.LOGGER.info("add job " + job);
						if (job != null) {
							if (!RoundRobinScheduler.this.initialize_queue
									.offer(job)) {
								// queue fail,switch back to sync initialize
								RoundRobinScheduler.this.taskTrackerManager
										.initJob(job);

								if (!RoundRobinScheduler.this.jobs.add(job)) {
									RoundRobinScheduler.LOGGER
											.error("fail to register job:"
													+ job.getJobID());
								}
							}
						}
					}
				});
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.hadoop.mapred.TaskScheduler#terminate()
	 */
	@Override
	public void terminate() throws IOException {
		// terminate async init thread
		this.initialize_queue = null;
		super.terminate();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<Task> assignTasks(TaskTracker tasktracker) throws IOException {
		// easy case,no jobs
		if (this.jobs.isEmpty()) {
			RoundRobinScheduler.LOGGER
					.info("assign null,as the job tracking counter indicate no jobs");
			return RoundRobinScheduler.EMPTY_TASK_LIST;
		}

		// fetch tracker status
		TaskTrackerStatus status = tasktracker.getStatus();
		final int cluster_size = this.taskTrackerManager.getClusterStatus()
				.getTaskTrackers();
		final int uniq_hosts = this.taskTrackerManager.getNumberOfUniqueHosts();

		// bookkeeping
		int assigned_reduce = 0;
		int map_capacity = status.getAvailableMapSlots();
		int reduce_capacity = status.getAvailableReduceSlots();
		JobInProgress job = null;
		Task task = null;
		JobID avoid_infinite_loop_mark = null;
		TaskSelector selector = null;
		TaskSelector temporary_reduce_swtich = null;
		int capacity = 0;

		// kick start capacity
		if (map_capacity > 0) {
			selector = TaskSelector.Map;
			capacity = map_capacity;
		} else if (reduce_capacity > 0) {
			selector = TaskSelector.Reduce;
			capacity = reduce_capacity;
		} else {
			// optimize case,no map/reduce available
			RoundRobinScheduler.LOGGER.info("out of usage.map capacity:"
					+ map_capacity + " reduce capacity:" + reduce_capacity);
			return RoundRobinScheduler.EMPTY_TASK_LIST;
		}

		// make iterator
		Iterator<JobInProgress> iterator = this.newJobIterator();
		if (iterator == null) {
			RoundRobinScheduler.LOGGER.error("instant job interator fail");
			return RoundRobinScheduler.EMPTY_TASK_LIST;
		}

		// lazy initial tasks
		int total = map_capacity + reduce_capacity;
		List<Task> assigned = new ArrayList<Task>(total <= 0 ? 0 : total);

		// delay logging until here
		RoundRobinScheduler.LOGGER.info("assign tasks for "
				+ status.getTrackerName());

		// assign
		while (capacity > 0) {
			if (!iterator.hasNext() || (job = iterator.next()) == null) {
				// temporary no jobs
				break;
			}

			// optimize case:
			// when map already finished or there is no map task,switch to
			// reduce case temporary
			if ((job.getStatus().mapProgress() >= 1.0f //
					|| job.desiredMaps() <= 0)//
					&& selector != TaskSelector.Reduce//
					&& reduce_capacity > 0) {
				temporary_reduce_swtich = selector;
				selector = TaskSelector.Reduce;
				capacity = reduce_capacity;
			}

			if ((task = selector.select(job, status, cluster_size, uniq_hosts)) != null) {
				assigned.add(task);
				capacity--;

				// backtrack bookkeeping
				if (selector == TaskSelector.Reduce) {
					++assigned_reduce;
					--reduce_capacity;
				} else if (--map_capacity <= 0) {
					// no map remains,switch to reduce mode
					selector = TaskSelector.Reduce;
					capacity = reduce_capacity;
				}

				// clear mark
				avoid_infinite_loop_mark = null;
			} else if (avoid_infinite_loop_mark == null) {
				// not seen yet,mark it
				avoid_infinite_loop_mark = job.getJobID();
			} else if (avoid_infinite_loop_mark.equals(job.getJobID())) {
				// if a reduce selector switch occurs,
				// switch back to map selector
				if (temporary_reduce_swtich != null) {
					selector = temporary_reduce_swtich;
					temporary_reduce_swtich = null;
					capacity = map_capacity;
				}

				// loop back,switch to next level,and retry
				switch (selector) {
				case Map:
					selector = TaskSelector.Reduce;
					// switch capacity to reduce capacity
					capacity = reduce_capacity;
					break;
				case Reduce:
				default:
					// tricky way to stop the loop
					capacity = 0;
					break;
				}

				// clear mark,as the selector may already changed mode
				avoid_infinite_loop_mark = null;
			}

			// if a reduce selector switch occurs,
			// switch back to map selector
			if (temporary_reduce_swtich != null) {
				selector = temporary_reduce_swtich;
				temporary_reduce_swtich = null;
				capacity = map_capacity;
			}
		}

		// log informed
		RoundRobinScheduler.LOGGER.info("assigned task:" + assigned.size() //
				+ " assign map:" + (assigned.size() - assigned_reduce) //
				+ " assign redcue:" + assigned_reduce //
				+ " map_capacity:" + status.getAvailableMapSlots() //
				+ " reduce_capacity:" + status.getAvailableReduceSlots());

		return assigned;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Collection<JobInProgress> getJobs(String identity) {
		return new CopyOnWriteArraySet<JobInProgress>(this.jobs);
	}

	/**
	 * return the job cycled-iterator
	 * 
	 * @return cycled-iterator,will return null JobInprogress only when there is
	 *         no job at all
	 */
	protected Iterator<JobInProgress> newJobIterator() {
		return new Iterator<JobInProgress>() {
			private Iterator<JobInProgress> delegate = RoundRobinScheduler.this.jobs
					.iterator();

			@Override
			public boolean hasNext() {
				if (!this.delegate.hasNext()) {
					this.delegate = RoundRobinScheduler.this.jobs.iterator();
				}
				return this.delegate.hasNext();
			}

			@Override
			public JobInProgress next() {
				if (!this.delegate.hasNext()) {
					this.delegate = RoundRobinScheduler.this.jobs.iterator();
				}

				try {
					return this.delegate.next();
				} catch (NoSuchElementException e) {
					return null;
				}
			}

			@Override
			public void remove() {
				this.delegate.remove();
			}
		};
	}
}
