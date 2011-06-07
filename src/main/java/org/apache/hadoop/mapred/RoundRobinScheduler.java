package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
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

	private static final Log LOGGER = LogFactory.getLog(RoundRobinScheduler.class);
	private static ExecutorService SERVICE = new ThreadPoolExecutor(0, Short.MAX_VALUE, 10, TimeUnit.SECONDS,
			new LinkedBlockingQueue<Runnable>());

	private Map<JobInProgress, JobInProgress> jobs = new ConcurrentHashMap<JobInProgress, JobInProgress>();

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.hadoop.mapred.TaskScheduler#start()
	 */
	@Override
	public void start() throws IOException {
		super.start();
		RoundRobinScheduler.LOGGER.info("start round robin scheduler");
		this.taskTrackerManager.addJobInProgressListener(new JobInProgressListener() {
			@Override
			public void jobUpdated(JobChangeEvent event) {
				RoundRobinScheduler.LOGGER.info("get an event:" + event);
			}

			@Override
			public void jobRemoved(JobInProgress job) {
				RoundRobinScheduler.LOGGER.info("remove job	" + job);
				RoundRobinScheduler.this.jobs.remove(job);
			}

			@Override
			public void jobAdded(final JobInProgress job) throws IOException {
				RoundRobinScheduler.LOGGER.info("add job	" + job);
				if (job != null) {
					SERVICE.execute(new Runnable() {
						public void run() {
							taskTrackerManager.initJob(job);
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
		Iterator<Entry<JobInProgress, JobInProgress>> round_robin = this.jobs.entrySet().iterator();
		// nothing to do
		if (!round_robin.hasNext()) {
			return new ArrayList<Task>(0);
		}

		// get jobs
		List<JobInProgress> in_progress = new ArrayList<JobInProgress>();
		while (round_robin.hasNext()) {
			JobInProgress job = round_robin.next().getKey();
			if (job != null && job.getStatus().getRunState() == JobStatus.RUNNING) {
				in_progress.add(job);
			}
		}

		// no runnning job
		if (in_progress.size() <= 0) {
			return new ArrayList<Task>(0);
		}

		RoundRobinScheduler.LOGGER.info("assign tasks for " + status);
		final List<Task> assigned = new ArrayList<Task>();
		final int task_tracker = this.taskTrackerManager.getClusterStatus().getTaskTrackers();
		final int uniq_hosts = this.taskTrackerManager.getNumberOfUniqueHosts();

		// assign map task
		int map_capacity = status.getMaxMapTasks() - status.countMapTasks();

		// ensure not empty
		int tracker = 0;
		while (map_capacity > 0) {
			LOGGER.info("map capacity:" + map_capacity);

			// iterate it
			tracker = ++tracker % in_progress.size();
			JobInProgress job = in_progress.get(tracker);
			Task task = job.obtainNewMapTask(status, task_tracker, uniq_hosts);
			if (task != null) {
				LOGGER.info("add a map task:" + task);
				assigned.add(task);
				map_capacity--;
			} else {
				LOGGER.info("no map capactiy remaind");
				break;
			}
		}

		// assign reduce task
		int reduce_capacity = status.getMaxMapTasks() - status.countMapTasks();
		tracker = 0;
		while (reduce_capacity > 0) {
			LOGGER.info("reduce capacity:" + map_capacity);

			// iterate it
			tracker = ++tracker % in_progress.size();
			JobInProgress job = in_progress.get(tracker);
			Task task = job.obtainNewReduceTask(status, task_tracker, uniq_hosts);
			if (task != null) {
				LOGGER.info("add a reduce task:" + task);
				assigned.add(task);
				reduce_capacity--;
			} else {
				LOGGER.info("no reduce capactiy remaind");
				break;
			}
		}

		RoundRobinScheduler.LOGGER.info("assign task:" + assigned.size());
		return assigned;
	}

	@Override
	public Collection<JobInProgress> getJobs(String identity) {
		RoundRobinScheduler.LOGGER.info("get jobs");
		return new CopyOnWriteArraySet<JobInProgress>(this.jobs.keySet());
	}
}
