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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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

	private volatile long version;

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.hadoop.mapred.TaskScheduler#start()
	 */
	@Override
	public void start() throws IOException {
		super.start();
		this.version = System.currentTimeMillis();

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
						RoundRobinScheduler.this.jobs.put(job.getJobID(), job);
						RoundRobinScheduler.this.version = System
								.currentTimeMillis();
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

		// take a snapshot
		long snapshot = this.version;
		final Iterator<Entry<JobID, JobInProgress>> round_robin = this.jobs
				.entrySet().iterator();

		// get jobs
		List<JobInProgress> in_progress = null;
		while (round_robin.hasNext()) {
			JobInProgress job = round_robin.next().getValue();
			if (job != null) {
				switch (job.getStatus().getRunState()) {
				case JobStatus.RUNNING:
					if (in_progress == null) {
						// lazy initialize
						in_progress = new ArrayList<JobInProgress>();
					}
					in_progress.add(job);
					break;
				case JobStatus.FAILED:
				case JobStatus.KILLED:
				case JobStatus.SUCCEEDED:
					round_robin.remove();
					break;
				}
			}
		}

		// no runnning job
		if (in_progress == null) {
			return RoundRobinScheduler.EMPTY_ASSIGNED;
		}

		final long now = System.currentTimeMillis();
		// try weight the jobs
		Collections.sort(in_progress, new Comparator<JobInProgress>() {
			private double calculate(JobInProgress job) {
				// normalize to
				// (start_time*mpas*mpas*reduces*reduces)/(finished_maps *
				// finished_maps * finished_reduces *finished_reduces)*(now -
				// start_time)
				return ((double) (job.startTime * job.numMapTasks
						* job.numMapTasks * job.numReduceTasks * job.numReduceTasks))
						/ (job.finishedMapTasks * job.finishedMapTasks
								* job.finishedReduceTasks * job.finishedReduceTasks)
						* (now - job.startTime);
			}

			@Override
			public int compare(JobInProgress o1, JobInProgress o2) {
				double diff = calculate(o1) - calculate(o2);
				if (diff > 0) {
					return 1;
				} else if (diff == 0) {
					return 0;
				} else {
					return -1;
				}
			}
		});

		RoundRobinScheduler.LOGGER.info("assign tasks for "
				+ status.getTrackerName());
		List<Task> assigned = null;
		int task_tracker = this.taskTrackerManager.getClusterStatus()
				.getTaskTrackers();
		int uniq_hosts = this.taskTrackerManager.getNumberOfUniqueHosts();

		// assign map task
		int map_capacity = status.getAvailableMapSlots();

		// leave a chance to live
		int stop = in_progress.size();

		// snapshot,to make the assign max throughput
		int local_tracker = 0;

		// ensure not empty
		while (map_capacity > 0 && stop > 0) {
			// iterate it
			local_tracker = ++local_tracker % in_progress.size();
			Task task = in_progress.get(local_tracker).obtainNewMapTask(status,
					task_tracker, uniq_hosts);
			if (task != null) {
				if (assigned == null) {
					// lazy initialize
					assigned = new ArrayList<Task>();
				}
				assigned.add(task);
				map_capacity--;
			} else {
				stop--;
			}
		}

		// reset flag
		stop = in_progress.size();

		// assign reduce task
		int reduce_capacity = status.getAvailableReduceSlots();
		while (reduce_capacity > 0 && stop > 0) {
			// iterate it
			local_tracker = ++local_tracker % in_progress.size();
			Task task = in_progress.get(local_tracker).obtainNewReduceTask(
					status, task_tracker, uniq_hosts);
			if (task != null) {
				if (assigned == null) {
					// lazy initialize
					assigned = new ArrayList<Task>();
				}
				assigned.add(task);
				reduce_capacity--;
			} else {
				stop--;
			}
		}

		// this will not eliminate miss assign,but make it a little less
		if (snapshot != this.version) {
			Set<JobID> keeped_jobs = new HashSet<JobID>();
			do {
				// update snapshot
				snapshot = this.version;
				keeped_jobs.clear();

				// job removed
				// mark current job id
				// filter remove jobs
				Iterator<JobInProgress> job_iterator = in_progress.iterator();
				while (job_iterator.hasNext()) {
					JobInProgress job = job_iterator.next();
					if (this.jobs.containsKey(job.getJobID())) {
						keeped_jobs.add(job.getJobID());
					} else {
						job_iterator.remove();
					}
				}

				// filter task
				Iterator<Task> task_iterator = assigned.iterator();
				while (task_iterator.hasNext()) {
					if (!keeped_jobs.contains(task_iterator.next().getJobID())) {
						task_iterator.remove();
					}
				}
			} while (snapshot != this.version);
		}

		RoundRobinScheduler.LOGGER.info("assigned task:"
				+ (assigned == null ? 0 : assigned.size()) + " map_capacity:"
				+ map_capacity + " reduce_capacity:" + reduce_capacity);

		return assigned;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Collection<JobInProgress> getJobs(String identity) {
		return new CopyOnWriteArraySet<JobInProgress>(this.jobs.values());
	}
}
