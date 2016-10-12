/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.util.Utils

private[scheduler] object BlacklistTracker extends Logging {

  private val DEFAULT_TIMEOUT = "1h"

  /**
   * Returns true if the blacklist is enabled, based on checking the configuration in the following
   * order:
   * 1. Is it specifically enabled or disabled?
   * 2. Is it enabled via the legacy timeout conf?
   * 3. Default is off
   */
  def isBlacklistEnabled(conf: SparkConf): Boolean = {
    conf.getOption(BlacklistConfs.BLACKLIST_ENABLED) match {
      case Some(enabled) =>
        enabled.toBoolean
      case None =>
        // if they've got a non-zero setting for the legacy conf, always enable the blacklist,
        // otherwise, use the default.
        val legacyKey = BlacklistConfs.BLACKLIST_LEGACY_TIMEOUT_CONF
        conf.getOption(legacyKey).exists { legacyTimeout =>
          if (legacyTimeout == 0) {
            logWarning(s"Turning off blacklisting due to legacy configuration: $legacyKey == 0")
            false
          } else {
            logWarning(s"Turning on blacklisting due to legacy configuration: $legacyKey > 0")
            true
          }
        }
    }
  }

  def getBlacklistTimeout(conf: SparkConf): Long = {
    Utils.timeStringAsMs(conf.getOption(BlacklistConfs.BLACKLIST_TIMEOUT_CONF).getOrElse {
      conf.getOption(BlacklistConfs.BLACKLIST_LEGACY_TIMEOUT_CONF).getOrElse {
        DEFAULT_TIMEOUT
      }
    })
  }

  /**
   * Verify that blacklist configurations are consistent; if not, throw an exception.  Should only
   * be called if blacklisting is enabled.
   *
   * The configuration for the blacklist is expected to adhere to a few invariants.  Default
   * values follow these rules of course, but users may unwittingly change one configuration
   * without making the corresponding adjustment elsewhere.  This ensures we fail-fast when
   * there are such misconfigurations.
   */
  def validateBlacklistConfs(conf: SparkConf): Unit = {

    def mustBePos(k: String, v: String): Unit = {
      throw new IllegalArgumentException(s"$k was $v, but must be > 0.")
    }

    Seq(
      BlacklistConfs.MAX_TASK_ATTEMPTS_PER_EXECUTOR,
      BlacklistConfs.MAX_TASK_ATTEMPTS_PER_NODE,
      BlacklistConfs.MAX_FAILURES_PER_EXEC_STAGE,
      BlacklistConfs.MAX_FAILED_EXEC_PER_NODE_STAGE
    ).foreach { config =>
      val v = conf.getInt(config, 1) // not the real default but good enough for this check
      if (v <= 0) {
        mustBePos(config, v.toString)
      }
    }

    val timeout = getBlacklistTimeout(conf)
    if (timeout <= 0) {
      // first, figure out where the timeout came from, to include the right conf in the message.
      conf.getOption(BlacklistConfs.BLACKLIST_TIMEOUT_CONF) match {
        case Some(t) =>
          mustBePos(BlacklistConfs.BLACKLIST_TIMEOUT_CONF, timeout.toString)
        case None =>
          mustBePos(BlacklistConfs.BLACKLIST_LEGACY_TIMEOUT_CONF, timeout.toString)
      }
    }

    val maxTaskFailures = conf.getInt(BlacklistConfs.MAX_TASK_FAILURES, 4)
    val maxNodeAttempts = conf.getInt(BlacklistConfs.MAX_TASK_ATTEMPTS_PER_NODE, 2)

    if (maxNodeAttempts >= maxTaskFailures) {
      throw new IllegalArgumentException(s"${BlacklistConfs.MAX_TASK_ATTEMPTS_PER_NODE} " +
        s"( = ${maxNodeAttempts}) was >= ${BlacklistConfs.MAX_TASK_FAILURES} " +
        s"( = ${maxTaskFailures} ).  Though blacklisting is enabled, with this configuration, " +
        s"Spark will not be robust to one bad node.  Decrease " +
        s"${BlacklistConfs.MAX_TASK_ATTEMPTS_PER_NODE}, increase" +
        s" ${BlacklistConfs.MAX_TASK_FAILURES}, " +
        s"or disable blacklisting with ${BlacklistConfs.BLACKLIST_ENABLED}")
    }
  }
}

/**
 * Holds all the conf keys related to blacklist.  In SPARK-8425, there is a ConfigBuilder
 * helper, so this code is all custom for the backport.
 */
private[spark] object BlacklistConfs {
  val BLACKLIST_ENABLED = "spark.blacklist.enabled"

  val MAX_TASK_FAILURES = "spark.task.maxFailures"

  val MAX_TASK_ATTEMPTS_PER_EXECUTOR = "spark.blacklist.task.maxTaskAttemptsPerExecutor"

  val MAX_TASK_ATTEMPTS_PER_NODE = "spark.blacklist.task.maxTaskAttemptsPerNode"

  val MAX_FAILURES_PER_EXEC = "spark.blacklist.application.maxFailedTasksPerExecutor"

  val MAX_FAILURES_PER_EXEC_STAGE = "spark.blacklist.stage.maxFailedTasksPerExecutor"

  val MAX_FAILED_EXEC_PER_NODE = "spark.blacklist.application.maxFailedExecutorsPerNode"

  val MAX_FAILED_EXEC_PER_NODE_STAGE = "spark.blacklist.stage.maxFailedExecutorsPerNode"

  val BLACKLIST_TIMEOUT_CONF = "spark.blacklist.timeout"

  val BLACKLIST_LEGACY_TIMEOUT_CONF = "spark.scheduler.executorTaskBlacklistTime"
}
