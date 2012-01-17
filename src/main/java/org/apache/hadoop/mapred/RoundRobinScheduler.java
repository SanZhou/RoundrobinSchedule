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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

	private static ExecutorService SERVICE = new ThreadPoolExecutor(0,
			Short.MAX_VALUE, 10, TimeUnit.SECONDS,
			new LinkedBlockingQueue<Runnable>());

	private static final List<Task> EMPTY_ASSIGNED = Collections
			.unmodifiableList(new LinkedList<Task>());

	private Map<JobID, JobInProgress> jobs = new ConcurrentHashMap<JobID, JobInProgress>();

	/**
	 * attatch weight to job in progress
	 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
	 */
	protected static class WeightedJobInProgress {
		private final JobInProgress job;
		private final double weigth;

		/**
		 * constructor , attatch job and weigth 
		 */
		public WeightedJobInProgress(JobInProgress job, double weigth) {
			this.job = job;
			this.weigth = weigth;
		}

		/**
		 * get the job
		 * @return
		 * 	the job in progress
		 */
		public JobInProgress getJob() {
			return this.job;
		}

		/**
		 * get the weigh
		 * @return
		 */
		public double getWeigth() {
			return this.weigth;
		}
	}

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
				return job
						.obtainNewReduceTask(status, cluster_size, uniq_hosts);
			}
			return null;
		};
	}

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
						RoundRobinScheduler.this.jobs.remove(job.getJobID());
					}

					@Override
					public void jobAdded(final JobInProgress job)
							throws IOException {
						RoundRobinScheduler.LOGGER.info("add job " + job);
						if (job != null) {
							// wait for async submit done.
							// as when it return,the client may close the
							// connection?
							RoundRobinScheduler.SERVICE.execute(new Runnable() {
								@Override
								public void run() {
									RoundRobinScheduler.this.taskTrackerManager
											.initJob(job);
									RoundRobinScheduler.this.jobs.put(
											job.getJobID(), job);
								}
							});
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
		Iterator<Entry<JobID, JobInProgress>> round_robin = RoundRobinScheduler.this.jobs
				.entrySet().iterator();

		// get jobs
		List<WeightedJobInProgress> in_progress = null;
		final long now = System.currentTimeMillis();

		// find running jobs
		while (round_robin.hasNext()) {
			JobInProgress job = round_robin.next().getValue();
			if (job != null) {
				switch (job.getStatus().getRunState()) {
				case JobStatus.RUNNING:
					if (in_progress == null) {
						// lazy initialize
						in_progress = new ArrayList<WeightedJobInProgress>();
					}

					// normalize to
					// (start_time*mpas*mpas*reduces*reduces)/(finished_maps
					// *
					// finished_maps * finished_reduces
					// *finished_reduces)*(now -
					// start_time)
					in_progress
							.add(new WeightedJobInProgress(
									job,
									((double) (job.startTime * job.numMapTasks
											* job.numMapTasks
											* job.numReduceTasks * job.numReduceTasks))
											/ (job.finishedMapTasks
													* job.finishedMapTasks
													* job.finishedReduceTasks * job.finishedReduceTasks)
											* (now - job.startTime)));
					break;
				case JobStatus.FAILED:
				case JobStatus.KILLED:
				case JobStatus.SUCCEEDED:
					round_robin.remove();
					break;
				}
			}
		}

		// get jobs
		if (in_progress == null) {
			return RoundRobinScheduler.EMPTY_ASSIGNED;
		}

		// try weight the jobs
		if (in_progress.size() > 1) {
			Collections.sort(in_progress, new Comparator<WeightedJobInProgress>() {
				@Override
				public int compare(WeightedJobInProgress o1,
						WeightedJobInProgress o2) {
					double diff = o1.weigth - o2.weigth;
					if (diff > 0) {
						return 1;
					} else if (diff == 0) {
						return 0;
					} else {
						return -1;
					}
				}
			});
		}
		RoundRobinScheduler.LOGGER.info("assign tasks for "
				+ status.getTrackerName());

		// prepare task
		List<Task> assigned = new ArrayList<Task>();
		final int task_tracker = this.taskTrackerManager.getClusterStatus()
				.getTaskTrackers();
		final int uniq_hosts = this.taskTrackerManager.getNumberOfUniqueHosts();

		// assign map task
		TaskSelector selector = TaskSelector.LocalMap;
		int capacity = status.getAvailableMapSlots();
		do {
			// assign map
			capacity = this.internalAssignTasks(selector, capacity,
					in_progress, status, task_tracker, uniq_hosts, assigned);

			// have remains,try next selector
			if (capacity > 0) {
				switch (selector) {
				case LocalMap:
					selector = TaskSelector.RackMap;
					break;
				case RackMap:
					selector = TaskSelector.Map;
					break;
				case Map:
					selector = TaskSelector.Reduce;
					capacity = status.getAvailableReduceSlots();
					break;
				case Reduce:
				default:
					selector = null;
					break;
				}
			} else {
				// no remains,try switch mode
				switch (selector) {
				case LocalMap:
				case RackMap:
				case Map:
					selector = TaskSelector.Reduce;
					capacity = status.getAvailableReduceSlots();
					break;
				case Reduce:
				default:
					selector = null;
					break;
				}
			}
		} while (selector != null);

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
		return new CopyOnWriteArraySet<JobInProgress>(this.jobs.values());
	}

	/**
	 * internal assign task according to selector typ
	 * @param selector
	 * 		the selector
	 * @param capacity
	 * 		the estimate capacity
	 * @param in_progress
	 * 		the job
	 * @param status
	 * 		the tasktracker status
	 * @param task_tracker
	 * 		the number of task tracker
	 * @param uniq_hosts
	 * 		the uniq_hosts
	 * @param assigned
	 * 		the assigned task
	 * @return
	 * 		the number of remained capacity
	 * @throws IOException
	 * 		throw when assign fail
	 */
	protected int internalAssignTasks(TaskSelector selector, int capacity,
			List<WeightedJobInProgress> in_progress, TaskTrackerStatus status,
			int task_tracker, int uniq_hosts, List<Task> assigned)
			throws IOException {
		int local_tracker = 0;
		int stop = in_progress.size();
		while (capacity > 0 && stop > 0) {
			// iterate it
			Task task = selector.select(
					in_progress.get(local_tracker).getJob(), status,
					task_tracker, uniq_hosts);
			local_tracker = ++local_tracker % in_progress.size();
			if (task != null) {
				assigned.add(task);
				capacity--;
			} else {
				stop--;
			}
		}

		return capacity;
	}
}
