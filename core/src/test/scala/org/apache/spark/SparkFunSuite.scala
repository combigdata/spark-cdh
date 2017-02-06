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

package org.apache.spark

import java.util.Random

// scalastyle:off
import org.mockito.Mockito._
import org.scalatest.{FunSuite, Outcome}

import org.apache.spark._
import org.apache.spark.crypto.CryptoConf

/**
 * Base abstract class for all unit tests in Spark for handling common functionality.
 */
private[spark] abstract class SparkFunSuite extends FunSuite with Logging {
// scalastyle:on

  /**
   * Log the suite name and the test name before and after each test.
   *
   * Subclasses should never override this method. If they wish to run
   * custom code before and after each test, they should mix in the
   * {{org.scalatest.BeforeAndAfter}} trait instead.
   */
  final protected override def withFixture(test: NoArgTest): Outcome = {
    val testName = test.text
    val suiteName = this.getClass.getName
    val shortSuiteName = suiteName.replaceAll("org.apache.spark", "o.a.s")
    try {
      logInfo(s"\n\n===== TEST OUTPUT FOR $shortSuiteName: '$testName' =====\n")
      test()
    } finally {
      logInfo(s"\n\n===== FINISHED $shortSuiteName: '$testName' =====\n")
    }
  }

  /**
   * Runs a test twice, first time normally, second time with a mock SparkEnv with shuffle (I/O)
   * encryption turned on.
   *
   * The boolean parameter tells the test whether encryption is enabled for the run.
   */
  final protected def mockEncryptionTest(name: String)(fn: Boolean => Unit) {
    Seq(false, true).foreach { encrypt =>
      test(s"$name (encrypt = $encrypt)") {
        if (encrypt) {
          val conf = new SparkConf()
            .set(CryptoConf.SPARK_SHUFFLE_ENCRYPTION_ENABLED, encrypt.toString)
          val env = mock(classOf[SparkEnv])
          val key = new Array[Byte](16)
          new Random().nextBytes(key)
          val sm = new SecurityManager(conf, Some(key))
          when(env.securityManager).thenReturn(sm)
          SparkEnv.set(env)
        }
        try {
          fn(encrypt)
        } finally {
          SparkEnv.set(null)
        }
      }
    }
  }

  /**
   * Runs a test twice, initializing a SparkConf object with encryption off, then on. It's ok
   * for the test to modify the provided SparkConf.
   */
  final protected def encryptionTest(name: String)(fn: SparkConf => Unit) {
    Seq(false, true).foreach { encrypt =>
      test(s"$name (encrypt = $encrypt)") {
        val conf = new SparkConf()
          .set(CryptoConf.SPARK_SHUFFLE_ENCRYPTION_ENABLED, encrypt.toString)
        fn(conf)
      }
    }
  }


}
