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

package org.apache.spark.deploy.security

import java.security.PrivilegedExceptionAction

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION
import org.apache.hadoop.minikdc.MiniKdc
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.util.Utils

class HadoopDelegationTokenManagerSuite extends SparkFunSuite {
  private val hadoopConf = new Configuration()

  ignore("default configuration") {
    val manager = new HadoopDelegationTokenManager(new SparkConf(false), hadoopConf, null)
    assert(manager.isProviderLoaded("hadoopfs"))
    assert(manager.isProviderLoaded("hbase"))
    assert(manager.isProviderLoaded("hive"))
    assert(manager.isProviderLoaded("kafka"))
  }

  test("disable hive credential provider") {
    val sparkConf = new SparkConf(false).set("spark.security.credentials.hive.enabled", "false")
    val manager = new HadoopDelegationTokenManager(sparkConf, hadoopConf, null)
    assert(manager.isProviderLoaded("hadoopfs"))
    assert(manager.isProviderLoaded("hbase"))
    assert(!manager.isProviderLoaded("hive"))
    assert(manager.isProviderLoaded("kafka"))
  }

  test("using deprecated configurations") {
    val sparkConf = new SparkConf(false)
      .set("spark.yarn.security.tokens.hadoopfs.enabled", "false")
      .set("spark.yarn.security.credentials.hive.enabled", "false")
    val manager = new HadoopDelegationTokenManager(sparkConf, hadoopConf, null)
    assert(!manager.isProviderLoaded("hadoopfs"))
    assert(manager.isProviderLoaded("hbase"))
    assert(!manager.isProviderLoaded("hive"))
    assert(manager.isProviderLoaded("kafka"))
  }

  test("SPARK-23209: obtain tokens when Hive classes are not available") {
    // This test needs a custom class loader to hide Hive classes which are in the classpath.
    // Because the manager code loads the Hive provider directly instead of using reflection, we
    // need to drive the test through the custom class loader so a new copy that cannot find
    // Hive classes is loaded.
    val currentLoader = Thread.currentThread().getContextClassLoader()
    val noHive = new ClassLoader() {
      override def loadClass(name: String, resolve: Boolean): Class[_] = {
        if (name.startsWith("org.apache.hive") || name.startsWith("org.apache.hadoop.hive")) {
          throw new ClassNotFoundException(name)
        }

        val prefixBlacklist = Seq("java", "scala", "com.sun.", "sun.")
        if (prefixBlacklist.exists(name.startsWith(_))) {
          return currentLoader.loadClass(name)
        }

        val found = findLoadedClass(name)
        if (found != null) {
          return found
        }

        val classFileName = name.replaceAll("\\.", "/") + ".class"
        val in = currentLoader.getResourceAsStream(classFileName)
        if (in != null) {
          val bytes = IOUtils.toByteArray(in)
          return defineClass(name, bytes, 0, bytes.length)
        }

        throw new ClassNotFoundException(name)
      }
    }

    Utils.withContextClassLoader(noHive) {
      val test = noHive.loadClass(NoHiveTest.getClass.getName().stripSuffix("$"))
      test.getMethod("runTest").invoke(null)
    }
  }

  test("SPARK-29082: do not fail if current user does not have credentials") {
    // SparkHadoopUtil overrides the UGI configuration during initialization. That normally
    // happens early in the Spark application, but here it may affect the test depending on
    // how it's run, so force its initialization.
    SparkHadoopUtil.get

    var kdc: MiniKdc = null
    try {
      // UserGroupInformation.setConfiguration needs default kerberos realm which can be set in
      // krb5.conf. MiniKdc sets "java.security.krb5.conf" in start and removes it when stop called.
      val kdcDir = Utils.createTempDir()
      val kdcConf = MiniKdc.createConf()
      kdc = new MiniKdc(kdcConf, kdcDir)
      kdc.start()

      val krbConf = new Configuration()
      krbConf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos")

      UserGroupInformation.setConfiguration(krbConf)
      val manager = new HadoopDelegationTokenManager(new SparkConf(false), krbConf, null)
      val testImpl = new PrivilegedExceptionAction[Unit] {
        override def run(): Unit = {
          assert(UserGroupInformation.isSecurityEnabled())
          val creds = new Credentials()
          manager.obtainDelegationTokens(creds)
          assert(creds.numberOfTokens() === 0)
          assert(creds.numberOfSecretKeys() === 0)
        }
      }

      val realUser = UserGroupInformation.createUserForTesting("realUser", Array.empty)
      realUser.doAs(testImpl)

      val proxyUser = UserGroupInformation.createProxyUserForTesting("proxyUser", realUser,
        Array.empty)
      proxyUser.doAs(testImpl)
    } finally {
      if (kdc != null) {
        kdc.stop()
      }
      UserGroupInformation.reset()
    }
  }
}

/** Test code for SPARK-23209 to avoid using too much reflection above. */
private object NoHiveTest {

  def runTest(): Unit = {
    try {
      val manager = new HadoopDelegationTokenManager(new SparkConf(), new Configuration(), null)
      require(!manager.isProviderLoaded("hive"))
    } catch {
      case e: Throwable =>
        // Throw a better exception in case the test fails, since there may be a lot of nesting.
        var cause = e
        while (cause.getCause() != null) {
          cause = cause.getCause()
        }
        throw cause
    }
  }

}
