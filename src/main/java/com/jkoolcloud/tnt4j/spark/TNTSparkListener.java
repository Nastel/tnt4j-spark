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

	private ConcurrentHashMap<String, TrackingActivity> activityMap = new ConcurrentHashMap<>(89);
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

	public void onCommonEvent(SparkListenerEvent sparkEvent, String eventName) {
		TrackingEvent event = logger.newEvent(OpLevel.INFO, eventName, applActivity.getCorrelator(),
				eventName + ": {0}", sparkEvent);
		logger.tnt(event);
	}

	@Override
	public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate arg0) {
		onCommonEvent(arg0, "onEnvironmentUpdate");
	}

	@Override
	public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate arg0) {
		onCommonEvent(arg0, "onExecutorMetricsUpdate");
	}

	@Override
	public void onStageExecutorMetrics(SparkListenerStageExecutorMetrics arg0) {
		onCommonEvent(arg0, "onStageExecutorMetrics");
	}

	@Override
	public void onExecutorAdded(SparkListenerExecutorAdded arg0) {
		onCommonEvent(arg0, "onExecutorAdded");
	}

	@Override
	public void onExecutorRemoved(SparkListenerExecutorRemoved arg0) {
		onCommonEvent(arg0, "onExecutorRemoved");
	}

	@Override
	public void onBlockUpdated(SparkListenerBlockUpdated arg0) {
		onCommonEvent(arg0, "onBlockUpdated");
	}

	@Override
	public void onSpeculativeTaskSubmitted(SparkListenerSpeculativeTaskSubmitted arg0) {
		onCommonEvent(arg0, "onSpeculativeTaskSubmitted");
	}

	@Override
	public void onExecutorBlacklisted(SparkListenerExecutorBlacklisted arg0) {
		onCommonEvent(arg0, "onExecutorBlacklisted");
	}

	@Override
	public void onExecutorExcluded(SparkListenerExecutorExcluded arg0) {
		onCommonEvent(arg0, "onExecutorExcluded");
	}

	@Override
	public void onExecutorBlacklistedForStage(SparkListenerExecutorBlacklistedForStage arg0) {
		onCommonEvent(arg0, "onExecutorBlacklistedForStage");
	}

	@Override
	public void onExecutorExcludedForStage(SparkListenerExecutorExcludedForStage arg0) {
		onCommonEvent(arg0, "onExecutorExcludedForStage");
	}

	@Override
	public void onNodeBlacklistedForStage(SparkListenerNodeBlacklistedForStage arg0) {
		onCommonEvent(arg0, "onNodeBlacklistedForStage");
	}

	@Override
	public void onNodeExcludedForStage(SparkListenerNodeExcludedForStage arg0) {
		onCommonEvent(arg0, "onNodeExcludedForStage");
	}

	@Override
	public void onExecutorUnblacklisted(SparkListenerExecutorUnblacklisted arg0) {
		onCommonEvent(arg0, "onExecutorUnblacklisted");
	}

	@Override
	public void onExecutorUnexcluded(SparkListenerExecutorUnexcluded arg0) {
		onCommonEvent(arg0, "onExecutorUnexcluded");
	}

	@Override
	public void onNodeBlacklisted(SparkListenerNodeBlacklisted arg0) {
		onCommonEvent(arg0, "onNodeBlacklisted");
	}

	@Override
	public void onNodeExcluded(SparkListenerNodeExcluded arg0) {
		onCommonEvent(arg0, "onNodeExcluded");
	}

	@Override
	public void onNodeUnblacklisted(SparkListenerNodeUnblacklisted arg0) {
		onCommonEvent(arg0, "onNodeUnblacklisted");
	}

	@Override
	public void onNodeUnexcluded(SparkListenerNodeUnexcluded arg0) {
		onCommonEvent(arg0, "onNodeUnexcluded");
	}

	@Override
	public void onUnschedulableTaskSetAdded(SparkListenerUnschedulableTaskSetAdded arg0) {
		onCommonEvent(arg0, "onUnschedulableTaskSetAdded");
	}

	@Override
	public void onUnschedulableTaskSetRemoved(SparkListenerUnschedulableTaskSetRemoved arg0) {
		onCommonEvent(arg0, "onUnschedulableTaskSetRemoved");
	}

	@Override
	public void onOtherEvent(SparkListenerEvent arg0) {
		onCommonEvent(arg0, "onOtherEvent");
	}

	@Override
	public void onResourceProfileAdded(SparkListenerResourceProfileAdded arg0) {
		onCommonEvent(arg0, "onResourceProfileAdded");
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
		onCommonEvent(arg0, "onTaskGettingResult");
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
		onCommonEvent(arg0, "onUnpersistRDD");
	}
}
