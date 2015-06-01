/*
 * Copyright 2014-2015 JKOOL, LLC.
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
package org.jkool.tnt4j.spark;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerBlockManagerAdded;
import org.apache.spark.scheduler.SparkListenerBlockManagerRemoved;
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate;
import org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.SparkListenerTaskGettingResult;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.apache.spark.scheduler.SparkListenerUnpersistRDD;

import com.nastel.jkool.tnt4j.TrackingLogger;
import com.nastel.jkool.tnt4j.core.OpCompCode;
import com.nastel.jkool.tnt4j.core.OpLevel;
import com.nastel.jkool.tnt4j.tracker.TrackingActivity;
import com.nastel.jkool.tnt4j.tracker.TrackingEvent;

/**
 * This class implements SparkListener interface and tracks behavior of Spark jobs using
 * TNT4J API. All spark run-time events are correlated together using spark application id as
 * a correlator. Developers may extend this class and add other correlators if needed.
 * 
 * @version $Revision: 1 $
 * 
 */
public class TNTSparkListener implements SparkListener {

	private ConcurrentHashMap<String, TrackingActivity> activityMap = new ConcurrentHashMap<String, TrackingActivity>(89);
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
		applActivity = logger.newActivity(OpLevel.SUCCESS, appid);
		applActivity.setUser(arg0.sparkUser());
		applActivity.setCorrelator(arg0.appId().get());
		applActivity.start(arg0.time()*1000); // convert to microseconds
	}

	@Override
    public void onBlockManagerAdded(SparkListenerBlockManagerAdded arg0) {
		String name = "block-manager/" + arg0.blockManagerId().toString();
		TrackingActivity activity = logger.newActivity(OpLevel.SUCCESS, name);
		activity.start(arg0.time()*1000); // convert to microseconds
		activity.setCorrelator(applActivity.getCorrelator());
		activityMap.put(name, activity);
    }

	@Override
    public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved arg0) {
		String name = arg0.blockManagerId().toString();
		TrackingActivity activity = activityMap.get(name);
		if (activity != null) {
			activity.stop(arg0.time()*1000); // convert to microseconds
			activityMap.remove(name);
			logger.tnt(activity);
		}
    }

	@Override
    public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate arg0) {
		TrackingEvent event = logger.newEvent(OpLevel.INFO, "onEnvironmentUpdate", applActivity.getCorrelator(), "onEnvironmentUpdate: {0}", arg0);
		logger.tnt(event);
	}

	@Override
    public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate arg0) {
		TrackingEvent event = logger.newEvent(OpLevel.INFO, "onExecutorMetricsUpdate", applActivity.getCorrelator(), "onExecutorMetricsUpdate: {0}", arg0);
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
		TrackingActivity activity = logger.newActivity(OpLevel.SUCCESS, name);
		activity.start(); // convert to microseconds
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
		TrackingActivity activity = logger.newActivity(OpLevel.SUCCESS, name);
		activity.start(); // convert to microseconds
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
		TrackingEvent event = logger.newEvent(OpLevel.INFO, "onTaskGettingResult", applActivity.getCorrelator(), "onTaskGettingResult: {0}", arg0);
		logger.tnt(event);
    }

	@Override
    public void onTaskStart(SparkListenerTaskStart arg0) {
		String name = "task/" + arg0.stageId() + "/" + arg0.taskInfo().id(); 
		TrackingActivity activity = logger.newActivity(OpLevel.SUCCESS, name);
		activity.start(); // convert to microseconds
		activity.setCorrelator(applActivity.getCorrelator());
		activityMap.put(name, activity);
    }

	@Override
    public void onUnpersistRDD(SparkListenerUnpersistRDD arg0) {
		TrackingEvent event = logger.newEvent(OpLevel.INFO, "onUnpersistRDD", applActivity.getCorrelator(), "onUnpersistRDD: {0}", arg0);
		logger.tnt(event);
    }
}
