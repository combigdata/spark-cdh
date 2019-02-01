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

package org.apache.spark.internal.config

import java.util.concurrent.TimeUnit

private[spark] object Cloudera {

  val DYN_ALLOCATION_FORCE_ENABLE =
    ConfigBuilder("spark.cloudera.dynamicAllocation.enabled.force")
      .doc("Force dynamic allocation to be on, even if no shuffle service is being used.")
      .booleanConf
      .createWithDefault(false)

  val DYN_ALLOCATION_SHUFFLE_TIMEOUT =
    ConfigBuilder("spark.cloudera.dynamicAllocation.shuffle.timeout")
      .doc("Idle timeout for executors that are currently holding shuffle blocks.")
      .timeConf(TimeUnit.SECONDS)
      .checkValue(_ >= 0L, "Shuffle timeout should be >= 0.")
      .createWithDefaultString("1h")

}
