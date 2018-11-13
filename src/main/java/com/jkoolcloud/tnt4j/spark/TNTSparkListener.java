/*
 * Copyright 2014-2018 JKOOL, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jkoolcloud.tnt4j.spark;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.scheduler.*;

import com.jkoolcloud.tnt4j.TrackingLogger;
import com.jkoolcloud.tnt4j.core.OpCompCode;
import com.jkoolcloud.tnt4j.core.OpLevel;
import com.jkoolcloud.tnt4j.tracker.TrackingActivity;
import com.jkoolcloud.tnt4j.tracker.TrackingEvent;

/**
 * This class implements SparkListener interface and tracks behavior of Spark jobs using TNT4J API. All spark run-time
 * events are correlated together using spark application id as a correlator. Developers may extend this class and add
 * other correlators if needed.
 * 
 * @version $Revision: 1 $
 * 
 */
public class TNTSparkListener implements SparkListenerInterface {

	private ConcurrentHashMap<String, TrackingActivity> activityMap = new ConcurrentHashMap<String, TrackingActivity>(
			89);
	TrackingLogger logger;
	TrackingActivity applActivity;

	public TNTSparkListener(String name) throws IOException {
		logger = TrackingLogger.getInstance(name);
		logger.setKeepThreadContext(false);
		logger.open();
	}

	@Override
	public void onApplicationEnd(SparkListenerApplicationEnd arg0) {
		applActivity.stop(arg0.time());
		logger.tnt(applActivity);
	}

	@Override
	public void onApplicationStart(SparkListenerApplicationStart arg0) {
		String appid = arg0.appName() + "/" + arg0.appId().get();
		applActivity = logger.newActivity(OpLevel.INFO, appid);
		applActivity.setUser(arg0.sparkUser());
		applActivity.setCorrelator(arg0.appId().get());
		applActivity.start(arg0.time() * 1000); // convert to microseconds
	}

	@Override
	public void onBlockManagerAdded(SparkListenerBlockManagerAdded arg0) {
		String name = "block-manager/" + arg0.blockManagerId().toString();
		TrackingActivity activity = logger.newActivity(OpLevel.INFO, name);
		activity.start(arg0.time() * 1000); // convert to microseconds
		activity.setCorrelator(applActivity.getCorrelator());
		activityMap.put(name, activity);
	}

	@Override
	public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved arg0) {
		String name = arg0.blockManagerId().toString();
		TrackingActivity activity = activityMap.get(name);
		if (activity != null) {
			activity.stop(arg0.time() * 1000); // convert to microseconds
			activityMap.remove(name);
			logger.tnt(activity);
		}
	}

	@Override
	public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate arg0) {
		TrackingEvent event = logger.newEvent(OpLevel.INFO, "onEnvironmentUpdate", applActivity.getCorrelator(),
				"onEnvironmentUpdate: {0}", arg0);
		logger.tnt(event);
	}

	@Override
	public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate arg0) {
		TrackingEvent event = logger.newEvent(OpLevel.INFO, "onExecutorMetricsUpdate", applActivity.getCorrelator(),
				"onExecutorMetricsUpdate: {0}", arg0);
		logger.tnt(event);
	}

	@Override
	public void onExecutorAdded(SparkListenerExecutorAdded arg0) {
		TrackingEvent event = logger.newEvent(OpLevel.INFO, "onExecutorAdded", applActivity.getCorrelator(),
				"onExecutorAdded: {0}", arg0);
		logger.tnt(event);
	}

	@Override
	public void onExecutorRemoved(SparkListenerExecutorRemoved arg0) {
		TrackingEvent event = logger.newEvent(OpLevel.INFO, "onExecutorRemoved", applActivity.getCorrelator(),
				"onExecutorRemoved: {0}", arg0);
		logger.tnt(event);
	}

	@Override
	public void onBlockUpdated(SparkListenerBlockUpdated arg0) {
		TrackingEvent event = logger.newEvent(OpLevel.INFO, "onBlockUpdated", applActivity.getCorrelator(),
				"onBlockUpdated: {0}", arg0);
		logger.tnt(event);
	}

	@Override
	public void onSpeculativeTaskSubmitted(SparkListenerSpeculativeTaskSubmitted arg0) {
		TrackingEvent event = logger.newEvent(OpLevel.INFO, "onSpeculativeTaskSubmitted", applActivity.getCorrelator(),
				"onSpeculativeTaskSubmitted: {0}", arg0);
		logger.tnt(event);
	}

	@Override
	public void onExecutorBlacklisted(SparkListenerExecutorBlacklisted arg0) {
		TrackingEvent event = logger.newEvent(OpLevel.INFO, "onExecutorBlacklisted", applActivity.getCorrelator(),
				"onExecutorBlacklisted: {0}", arg0);
		logger.tnt(event);
	}

	@Override
	public void onExecutorUnblacklisted(SparkListenerExecutorUnblacklisted arg0) {
		TrackingEvent event = logger.newEvent(OpLevel.INFO, "onExecutorUnblacklisted", applActivity.getCorrelator(),
				"onExecutorUnblacklisted: {0}", arg0);
		logger.tnt(event);
	}

	@Override
	public void onNodeBlacklisted(SparkListenerNodeBlacklisted arg0) {
		TrackingEvent event = logger.newEvent(OpLevel.INFO, "onNodeBlacklisted", applActivity.getCorrelator(),
				"onNodeBlacklisted: {0}", arg0);
		logger.tnt(event);
	}

	@Override
	public void onNodeUnblacklisted(SparkListenerNodeUnblacklisted arg0) {
		TrackingEvent event = logger.newEvent(OpLevel.INFO, "onNodeUnblacklisted", applActivity.getCorrelator(),
				"onNodeUnblacklisted: {0}", arg0);
		logger.tnt(event);
	}

	@Override
	public void onOtherEvent(SparkListenerEvent arg0) {
		TrackingEvent event = logger.newEvent(OpLevel.INFO, "onOtherEvent", applActivity.getCorrelator(),
				"onOtherEvent: {0}", arg0);
		logger.tnt(event);
	}

	@Override
	public void onJobEnd(SparkListenerJobEnd arg0) {
		String name = "job/" + arg0.jobId();
		TrackingActivity activity = activityMap.get(name);
		if (activity != null) {
			activity.stop();
			activityMap.remove(name);
			logger.tnt(activity);
		}
	}

	@Override
	public void onJobStart(SparkListenerJobStart arg0) {
		String name = "job/" + arg0.jobId();
		TrackingActivity activity = logger.newActivity(OpLevel.INFO, name);
		activity.start();
		activity.setCorrelator(applActivity.getCorrelator());
		activityMap.put(name, activity);
	}

	@Override
	public void onStageCompleted(SparkListenerStageCompleted arg0) {
		String name = "stage/" + arg0.stageInfo().name() + "/" + arg0.stageInfo().stageId();
		TrackingActivity activity = activityMap.get(name);
		if (activity != null) {
			activity.stop();
			activity.setException(arg0.stageInfo().failureReason().get());
			activityMap.remove(name);
			logger.tnt(activity);
		}
	}

	@Override
	public void onStageSubmitted(SparkListenerStageSubmitted arg0) {
		String name = "stage/" + arg0.stageInfo().name() + "/" + arg0.stageInfo().stageId();
		TrackingActivity activity = logger.newActivity(OpLevel.INFO, name);
		activity.start();
		activity.setCorrelator(applActivity.getCorrelator());
		activityMap.put(name, activity);
	}

	@Override
	public void onTaskEnd(SparkListenerTaskEnd arg0) {
		String name = "task/" + arg0.stageId() + "/" + arg0.taskInfo().id();
		TrackingActivity activity = activityMap.get(name);
		if (activity != null) {
			activity.stop();
			activity.setLocation(arg0.taskInfo().host());
			activity.setException(arg0.reason().toString());
			if (arg0.taskInfo().failed()) {
				activity.setCompCode(OpCompCode.ERROR);
			}
			activityMap.remove(name);
			logger.tnt(activity);
		}
	}

	@Override
	public void onTaskGettingResult(SparkListenerTaskGettingResult arg0) {
		TrackingEvent event = logger.newEvent(OpLevel.INFO, "onTaskGettingResult", applActivity.getCorrelator(),
				"onTaskGettingResult: {0}", arg0);
		logger.tnt(event);
	}

	@Override
	public void onTaskStart(SparkListenerTaskStart arg0) {
		String name = "task/" + arg0.stageId() + "/" + arg0.taskInfo().id();
		TrackingActivity activity = logger.newActivity(OpLevel.INFO, name);
		activity.start();
		activity.setCorrelator(applActivity.getCorrelator());
		activityMap.put(name, activity);
	}

	@Override
	public void onUnpersistRDD(SparkListenerUnpersistRDD arg0) {
		TrackingEvent event = logger.newEvent(OpLevel.INFO, "onUnpersistRDD", applActivity.getCorrelator(),
				"onUnpersistRDD: {0}", arg0);
		logger.tnt(event);
	}
}
