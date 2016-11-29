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
package org.apache.spark.crypto

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets.UTF_8

import com.google.common.io.ByteStreams

import org.apache.spark.{SparkConf, SparkContext, SparkEnv, SparkFunSuite}
import org.apache.spark.crypto.CryptoConf._
import org.apache.spark.crypto.CryptoStreamUtils._

private[spark] class ShuffleEncryptionSuite extends SparkFunSuite {

  test("chimera configuration conversion") {
    val sparkKey1 = s"${SPARK_CHIMERA_CONF_PREFIX}a.b.c"
    val sparkVal1 = "val1"
    val chimeraKey1 = s"${CHIMERA_CONF_PREFIX}a.b.c"

    val sparkKey2 = s"${SPARK_CHIMERA_CONF_PREFIX.stripSuffix(".")}A.b.c"
    val sparkVal2 = "val2"
    val chimeraKey2 = s"${CHIMERA_CONF_PREFIX}A.b.c"

    val conf = new SparkConf()
    conf.set(sparkKey1, sparkVal1)
    conf.set(sparkKey2, sparkVal2)

    val props = toChimeraConf(conf)
    assert(props.getProperty(chimeraKey1) === sparkVal1)
    assert(!props.contains(chimeraKey2))
  }

  test("shuffle encryption key length should be 128 by default") {
    val conf = createConf()
    var key = CryptoStreamUtils.createKey(conf)
    val actual = key.length * (java.lang.Byte.SIZE)
    assert(actual === 128)
  }

  test("create 256-bit key") {
    val conf = createConf(SPARK_SHUFFLE_ENCRYPTION_KEY_SIZE_BITS -> "256")
    var key = CryptoStreamUtils.createKey(conf)
    val actual = key.length * (java.lang.Byte.SIZE)
    assert(actual === 256)
  }

  test("Test initial credentials with invalid key length") {
    intercept[IllegalArgumentException] {
      val conf = createConf(
        SPARK_SHUFFLE_ENCRYPTION_KEYGEN_ALGORITHM -> "AES",
        SPARK_SHUFFLE_ENCRYPTION_KEY_SIZE_BITS -> "328")
      CryptoStreamUtils.createKey(conf)
    }
  }

  test("encryption key propagation to executors") {
    val conf = createConf().setAppName("Crypto Test").setMaster("local-cluster[1,1,1024]")
    val sc = new SparkContext(conf)
    try {
      val content = "This is the content to be encrypted."
      val encrypted = sc.parallelize(Seq(1))
        .map { str =>
          val bytes = new ByteArrayOutputStream()
          val out = CryptoStreamUtils.wrapForEncryption(bytes, SparkEnv.get.conf)
          out.write(content.getBytes(UTF_8))
          out.close()
          bytes.toByteArray()
        }.collect()(0)

      assert(content != encrypted)

      val in = CryptoStreamUtils.wrapForEncryption(new ByteArrayInputStream(encrypted),
        sc.conf)
      val decrypted = new String(ByteStreams.toByteArray(in), UTF_8)
      assert(content === decrypted)
    } finally {
      sc.stop()
    }
  }

  private def createConf(extra: (String, String)*): SparkConf = {
    val conf = new SparkConf()
    extra.foreach { case (k, v) => conf.set(k, v) }
    conf.set(SPARK_SHUFFLE_ENCRYPTION_ENABLED, true.toString)
    conf
  }

}
