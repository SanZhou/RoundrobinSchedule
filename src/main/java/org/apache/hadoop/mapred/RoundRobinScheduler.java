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
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArraySet;

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

	/**
	 * shortcut task selector
	 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
	 */
	public static enum TaskSelector {
		LocalMap, RackMap, Map, Reduce;

		/**
		 * select a task accroding self type
		 * @param job
		 * 		the job tips
		 * @param status
		 * 		the tasktracker status
		 * @param cluster_size
		 * 		the cluster size
		 * @param uniq_hosts
		 * 		uniq hosts
		 * @return
		 * 		the task selected ,null if none
		 * @throws IOException
		 * 		throw when obtain task fail
		 */
		Task select(JobInProgress job, TaskTrackerStatus status,
				int cluster_size, int uniq_hosts) throws IOException {
			switch (this) {
			case LocalMap:
				return job.obtainNewNodeLocalMapTask(status, cluster_size,
						uniq_hosts);
			case RackMap:
				return job.obtainNewNodeOrRackLocalMapTask(status,
						cluster_size, uniq_hosts);
			case Map:
				return job.obtainNewNonLocalMapTask(status, cluster_size,
						uniq_hosts);
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

	private Set<JobInProgress> jobs = new ConcurrentSkipListSet<JobInProgress>(
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

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.hadoop.mapred.TaskScheduler#start()
	 */
	@Override
	public void start() throws IOException {
		super.start();

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
							RoundRobinScheduler.this.taskTrackerManager
									.initJob(job);
							RoundRobinScheduler.this.jobs.add(job);
						}
					}
				});
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<Task> assignTasks(TaskTracker tasktracker) throws IOException {
		TaskTrackerStatus status = tasktracker.getStatus();
		RoundRobinScheduler.LOGGER.info("assign tasks for "
				+ status.getTrackerName());

		// prepare task
		List<Task> assigned = new LinkedList<Task>();
		final int cluster_size = this.taskTrackerManager.getClusterStatus()
				.getTaskTrackers();
		final int uniq_hosts = this.taskTrackerManager.getNumberOfUniqueHosts();

		// assign map task
		int map_capacity = status.getAvailableMapSlots() > 0 ? status
				.getMaxMapSlots() : 0;
		int reduce_capacity = status.getAvailableReduceSlots() > 0 ? status
				.getMaxReduceSlots() : 0;

		Iterator<JobInProgress> iterator = this.newJobIterator();
		if (iterator == null) {
			RoundRobinScheduler.LOGGER.error("instant job interator fail");
			return assigned;
		}

		JobInProgress job = null;
		Task task = null;
		JobID avoid_infinite_loop_mark = null;
		TaskSelector selector = null;
		int capacity = 0;

		// kickstart
		if (map_capacity > 0) {
			selector = TaskSelector.LocalMap;
			capacity = map_capacity;
		} else {
			selector = TaskSelector.Reduce;
			capacity = reduce_capacity;
		}

		// assign
		while (capacity > 0) {
			if (!iterator.hasNext() || (job = iterator.next()) == null) {
				// temporary no jobs
				break;
			}

			if ((task = selector.select(job, status, cluster_size, uniq_hosts)) != null) {
				assigned.add(task);
				capacity--;

				// backtrack bookkeeping
				if (selector == TaskSelector.Reduce) {
					reduce_capacity--;
				} else if (map_capacity-- <= 0) {
					// no map remains,switch to reduce mode
					selector = TaskSelector.Reduce;
					capacity = reduce_capacity;

					// clear mark
					avoid_infinite_loop_mark = null;
				}
			} else // no task ,see if this job has been seem before
			if (avoid_infinite_loop_mark == null) {
				// not see yet,mark it
				avoid_infinite_loop_mark = job.getJobID();
			} else if (avoid_infinite_loop_mark.equals(job.getJobID())) {
				// loop back,switch to next level,and retry
				switch (selector) {
				case LocalMap:
					selector = TaskSelector.RackMap;
					break;
				case RackMap:
					selector = TaskSelector.Map;
					break;
				case Map:
					selector = TaskSelector.Reduce;
					// switch capacity to reduce capacity
					capacity = reduce_capacity;
					break;
				case Reduce:
				default:
					capacity = 0;
					break;
				}

				// clear mark
				avoid_infinite_loop_mark = null;
			}
		}

		// log informed
		RoundRobinScheduler.LOGGER.info("assigned task:" + assigned.size()
				+ " map_capacity:" + status.getAvailableMapSlots()
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
	 * @return
	 * 		cycled-iterator,will return null JobInprogress only when there is no job at all
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
				}
				return null;
			}

			@Override
			public void remove() {
				this.delegate.remove();
			}
		};
	}
}
