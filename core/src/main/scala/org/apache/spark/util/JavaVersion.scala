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
package org.apache.spark.util

import scala.util.Try

import org.apache.commons.lang3.{JavaVersion => CommonJavaVersion, SystemUtils}

import org.apache.spark.internal.Logging

/**
 * This class exists just to avoid referencing JavaVersion.JAVA_9 from commons-lang3, due to a
 * classpath issue in CDH.  Though we could handle the problem in spark, it causes issues with hive
 * & oozie -- the easiest solution is just to reimplement this check.
 *
 * See more details on CDH-77734.
 */
private[spark] object JavaVersion extends Logging {

  private def javaVersionConst(version: Int): Option[CommonJavaVersion] = {
    Try {
      classOf[CommonJavaVersion]
        .getEnumConstants
        .filter { jv =>
          jv.asInstanceOf[Enum[_]].name() == s"JAVA_$version"
        }.head
    }.toOption
  }

  def isVersionAtLeast(version: Int): Boolean = {
    javaVersionConst(version).flatMap { v =>
      Try {
        // even if we successfully found the constant, this will fail if the runtime version
        // doesn't match a known constant.
        SystemUtils.isJavaVersionAtLeast(v)
      }.toOption
    }.getOrElse {
      // Most probably, the classpath doesn't have a good version of commons-lang3 on it.  As a
      // stop-gap, let's take a best guess (this is basically all the commons-lang3 code is doing
      // anyway).  And we don't support lower than java 1.8 anyway
      val specVersion = sys.props("java.specification.version")
      logInfo("It appears you have a bad version of commons-lang3 on the classpath.  Making " +
        "a best-guess at java version.")
      val normalizedSpecVersion = (if (specVersion == "1.8") "8" else specVersion).toInt
      normalizedSpecVersion >= version
    }
  }

}
