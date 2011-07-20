package org.apache.hadoop.mapred;

import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

/**
 * assign task in round robin fashion
 * 
 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
 */
public class RoundRobinScheduler extends TaskScheduler {

	private static final Log LOGGER = LogFactory
			.getLog(RoundRobinScheduler.class);

	private static ExecutorService SERVICE = new ThreadPoolExecutor(0,
			Short.MAX_VALUE, 10, TimeUnit.SECONDS,
			new LinkedBlockingQueue<Runnable>());

	private static final List<Task> EMPTY_ASSIGNED = Collections
			.unmodifiableList(new LinkedList<Task>());

	/**
	 * an attempt to help GC
	 * 
	 * @author <a href="mailto:zhizhong.qiu@happyelements.com">kevin</a>
	 * @version 1.0
	 * @since 1.0 2011-5-3
	 */
	public static class GCNice {

		protected static final ReferenceQueue<Object> QUEUE = new ReferenceQueue<Object>();

		/**
		 * make an obejct that attatch to an reference queue.
		 * 
		 * @param <Type>
		 *            the obejct type
		 * @param object
		 *            the new attatch queue
		 * @return the SoftReference object
		 */
		public static <Type> SoftReference<Type> makeReference(Type object) {
			return new SoftReference<Type>(object, GCNice.QUEUE);
		}

		/**
		 * make an obejct that attatch to an reference queue.
		 * 
		 * @param <Type>
		 *            the obejct type
		 * @param object
		 *            the new attatch queue
		 * @return the object
		 */
		public static <Type> Type make(Type object) {
			return new SoftReference<Type>(object, GCNice.QUEUE).get();
		}
	}

	private Map<JobInProgress, JobInProgress> jobs = new ConcurrentHashMap<JobInProgress, JobInProgress>();

	private int tracker;

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.hadoop.mapred.TaskScheduler#start()
	 */
	@Override
	public void start() throws IOException {
		super.start();
		this.tracker = 0;
		RoundRobinScheduler.LOGGER.info("start round robin scheduler");
		this.taskTrackerManager
				.addJobInProgressListener(new JobInProgressListener() {
					@Override
					public void jobUpdated(JobChangeEvent event) {
						RoundRobinScheduler.LOGGER
								.info("get an event:" + event);
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
							RoundRobinScheduler.SERVICE.execute(new Runnable() {
								public void run() {
									RoundRobinScheduler.this.taskTrackerManager
											.initJob(GCNice.make(job));
									RoundRobinScheduler.this.jobs.put(job, job);
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
	public List<Task> assignTasks(TaskTrackerStatus status) throws IOException {
		// take a snapshot
		final Iterator<Entry<JobInProgress, JobInProgress>> round_robin = this.jobs
				.entrySet().iterator();

		// get jobs
		List<JobInProgress> in_progress = null;
		while (round_robin.hasNext()) {
			JobInProgress job = round_robin.next().getKey();
			if (job != null
					&& job.getStatus().getRunState() == JobStatus.RUNNING) {
				if (in_progress == null) {
					// lazy initialize
					in_progress = GCNice.make(new ArrayList<JobInProgress>());
				}
				in_progress.add(job);
			}
		}

		// no runnning job
		if (in_progress.size() <= 0) {
			return EMPTY_ASSIGNED;
		}

		RoundRobinScheduler.LOGGER.info("assign tasks for "
				+ status.getTrackerName());
		List<Task> assigned = null;
		int task_tracker = this.taskTrackerManager.getClusterStatus()
				.getTaskTrackers();
		int uniq_hosts = this.taskTrackerManager.getNumberOfUniqueHosts();

		// assign map task
		int map_capacity = status.getMaxMapTasks() - status.countMapTasks();

		// leave a chance to live
		int stop = in_progress.size();

		// ensure not empty
		while (map_capacity > 0 && stop > 0) {
			// iterate it
			this.tracker = ++this.tracker % in_progress.size();
			Task task = in_progress.get(this.tracker).obtainNewMapTask(status,
					task_tracker, uniq_hosts);
			if (task != null) {
				if (assigned == null) {
					// lazy initialize
					assigned = GCNice.make(new ArrayList<Task>());
				}
				assigned.add(GCNice.make(task));
				map_capacity--;
			} else {
				stop--;
			}
		}

		// reset flag
		stop = in_progress.size();

		// assign reduce task
		int reduce_capacity = status.getMaxReduceTasks()
				- status.countReduceTasks();
		while (reduce_capacity > 0 && stop > 0) {
			// iterate it
			this.tracker = ++this.tracker % in_progress.size();
			Task task = in_progress.get(this.tracker).obtainNewReduceTask(
					status, task_tracker, uniq_hosts);
			if (task != null) {
				if (assigned == null) {
					// lazy initialize
					assigned = GCNice.make(new ArrayList<Task>());
				}
				assigned.add(GCNice.make(task));
				reduce_capacity--;
			} else {
				stop--;
			}
		}

		RoundRobinScheduler.LOGGER.info("assigned task:" + assigned.size()
				+ " map_capacity:" + map_capacity + " reduce_capacity:"
				+ reduce_capacity);
		return assigned;
	}

	@Override
	public Collection<JobInProgress> getJobs(String identity) {
		return GCNice.make(new CopyOnWriteArraySet<JobInProgress>(this.jobs
				.keySet()));
	}
}
