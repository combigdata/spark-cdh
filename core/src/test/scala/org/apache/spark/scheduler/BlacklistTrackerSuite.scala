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

import org.apache.spark.{SparkConf, SparkFunSuite}

class BlacklistTrackerSuite extends SparkFunSuite {

  test("blacklist still respects legacy configs") {
    val conf = new SparkConf().setMaster("local")
    assert(!BlacklistTracker.isBlacklistEnabled(conf))
    conf.set(BlacklistConfs.BLACKLIST_LEGACY_TIMEOUT_CONF, 5000L.toString)
    assert(BlacklistTracker.isBlacklistEnabled(conf))
    assert(5000 === BlacklistTracker.getBlacklistTimeout(conf))
    // the new conf takes precedence, though
    conf.set(BlacklistConfs.BLACKLIST_TIMEOUT_CONF, 1000L.toString)
    assert(1000 === BlacklistTracker.getBlacklistTimeout(conf))

    // if you explicitly set the legacy conf to 0, that also would disable blacklisting
    conf.set(BlacklistConfs.BLACKLIST_LEGACY_TIMEOUT_CONF, 0L.toString)
    assert(!BlacklistTracker.isBlacklistEnabled(conf))
    // but again, the new conf takes precendence
    conf.set(BlacklistConfs.BLACKLIST_ENABLED, true.toString)
    assert(BlacklistTracker.isBlacklistEnabled(conf))
    assert(1000 === BlacklistTracker.getBlacklistTimeout(conf))
  }

  test("check blacklist configuration invariants") {
    val conf = new SparkConf().setMaster("yarn-cluster")
    Seq(
      (2, 2),
      (2, 3)
    ).foreach { case (maxTaskFailures, maxNodeAttempts) =>
      conf.set(BlacklistConfs.MAX_TASK_FAILURES, maxTaskFailures.toString)
      conf.set(BlacklistConfs.MAX_TASK_ATTEMPTS_PER_NODE, maxNodeAttempts.toString)
      val excMsg = intercept[IllegalArgumentException] {
        BlacklistTracker.validateBlacklistConfs(conf)
      }.getMessage()
      assert(excMsg === s"${BlacklistConfs.MAX_TASK_ATTEMPTS_PER_NODE} " +
        s"( = ${maxNodeAttempts}) was >= ${BlacklistConfs.MAX_TASK_FAILURES} " +
        s"( = ${maxTaskFailures} ).  Though blacklisting is enabled, with this configuration, " +
        s"Spark will not be robust to one bad node.  Decrease " +
        s"${BlacklistConfs.MAX_TASK_ATTEMPTS_PER_NODE}, increase" +
        s" ${BlacklistConfs.MAX_TASK_FAILURES}, " +
        s"or disable blacklisting with ${BlacklistConfs.BLACKLIST_ENABLED}")
    }

    conf.remove(BlacklistConfs.MAX_TASK_FAILURES)
    conf.remove(BlacklistConfs.MAX_TASK_ATTEMPTS_PER_NODE)

    Seq(
      BlacklistConfs.MAX_TASK_ATTEMPTS_PER_EXECUTOR,
      BlacklistConfs.MAX_TASK_ATTEMPTS_PER_NODE,
      BlacklistConfs.MAX_FAILURES_PER_EXEC_STAGE,
      BlacklistConfs.MAX_FAILED_EXEC_PER_NODE_STAGE,
      BlacklistConfs.BLACKLIST_TIMEOUT_CONF
    ).foreach { config =>
      conf.set(config, "0")
      val excMsg = intercept[IllegalArgumentException] {
        BlacklistTracker.validateBlacklistConfs(conf)
      }.getMessage()
      assert(excMsg.contains(s"${config} was 0, but must be > 0."))
      conf.remove(config)
    }
  }
}
