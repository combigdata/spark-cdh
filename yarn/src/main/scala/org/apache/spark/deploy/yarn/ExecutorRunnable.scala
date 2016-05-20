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

package org.apache.spark.deploy.yarn

import java.io.File
import java.net.URI
import java.nio.ByteBuffer
import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, ListBuffer}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.NMClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}

import org.apache.spark.{Logging, SecurityManager, SparkConf, SparkException}
import org.apache.spark.launcher.YarnCommandBuilderUtils
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.util.Utils

class ExecutorRunnable(
    container: Option[Container],
    conf: YarnConfiguration,
    sparkConf: SparkConf,
    masterAddress: String,
    executorId: String,
    hostname: String,
    executorMemory: Int,
    executorCores: Int,
    appId: String,
    securityMgr: SecurityManager) extends Logging {

  var rpc: YarnRPC = YarnRPC.create(conf)
  var nmClient: NMClient = _

  def run(): Unit = {
    logDebug("Starting Executor Container")
    nmClient = NMClient.createNMClient()
    nmClient.init(conf)
    nmClient.start()
    startContainer()
  }

  def launchContextDebugInfo(): String = {
    val commands = prepareCommand()
    val env = prepareEnvironment()
    val localResources = prepareLocalResources

    s"""
    |===============================================================================
    |YARN executor launch context:
    |  env:
    |${Utils.redact(sparkConf, env.toSeq).map { case (k, v) => s"    $k -> $v\n" }.mkString}
    |  command:
    |    ${commands.mkString(" \\ \n      ")}
    |
    |  resources:
    |${localResources.map { case (k, v) => s"    $k -> $v\n" }.mkString}
    |===============================================================================""".stripMargin
  }

  def startContainer(): java.util.Map[String, ByteBuffer] = {
    val ctx = Records.newRecord(classOf[ContainerLaunchContext])
      .asInstanceOf[ContainerLaunchContext]
    val env = prepareEnvironment().asJava

    val localResources = prepareLocalResources
    ctx.setLocalResources(localResources.asJava)
    ctx.setEnvironment(env)

    val credentials = UserGroupInformation.getCurrentUser().getCredentials()
    val dob = new DataOutputBuffer()
    credentials.writeTokenStorageToStream(dob)
    ctx.setTokens(ByteBuffer.wrap(dob.getData()))

    val commands = prepareCommand()
    ctx.setCommands(commands.asJava)
    ctx.setApplicationACLs(
      YarnSparkHadoopUtil.getApplicationAclsForYarn(securityMgr).asJava)

    // If external shuffle service is enabled, register with the Yarn shuffle service already
    // started on the NodeManager and, if authentication is enabled, provide it with our secret
    // key for fetching shuffle files later
    if (sparkConf.getBoolean("spark.shuffle.service.enabled", false)) {
      val secretString = securityMgr.getSecretKey()
      val secretBytes =
        if (secretString != null) {
          // This conversion must match how the YarnShuffleService decodes our secret
          JavaUtils.stringToBytes(secretString)
        } else {
          // Authentication is not enabled, so just provide dummy metadata
          ByteBuffer.allocate(0)
        }
      ctx.setServiceData(Collections.singletonMap("spark_shuffle", secretBytes))
    }

    // Send the start request to the ContainerManager
    try {
      nmClient.startContainer(container.get, ctx)
    } catch {
      case ex: Exception =>
        throw new SparkException(s"Exception while starting container ${container.get.getId}" +
          s" on host $hostname", ex)
    }
  }

  private def prepareCommand(): List[String] = {
    // Extra options for the JVM
    val javaOpts = ListBuffer[String]()

    // Set the environment variable through a command prefix
    // to append to the existing value of the variable
    var prefixEnv: Option[String] = None

    // Set the JVM memory
    val executorMemoryString = executorMemory + "m"
    javaOpts += "-Xms" + executorMemoryString
    javaOpts += "-Xmx" + executorMemoryString

    // Set extra Java options for the executor, if defined
    sys.props.get("spark.executor.extraJavaOptions").foreach { opts =>
      javaOpts ++= Utils.splitCommandString(opts).map(YarnSparkHadoopUtil.escapeForShell)
    }
    sys.env.get("SPARK_JAVA_OPTS").foreach { opts =>
      javaOpts ++= Utils.splitCommandString(opts).map(YarnSparkHadoopUtil.escapeForShell)
    }
    sys.props.get("spark.executor.extraLibraryPath").foreach { p =>
      prefixEnv = Some(Client.getClusterPath(sparkConf, Utils.libraryPathEnvPrefix(Seq(p))))
    }

    javaOpts += "-Djava.io.tmpdir=" +
      new Path(
        YarnSparkHadoopUtil.expandEnvironment(Environment.PWD),
        YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR
      )

    // Certain configs need to be passed here because they are needed before the Executor
    // registers with the Scheduler and transfers the spark configs. Since the Executor backend
    // uses Akka to connect to the scheduler, the akka settings are needed as well as the
    // authentication settings.
    sparkConf.getAll
      .filter { case (k, v) => SparkConf.isExecutorStartupConf(k) }
      .foreach { case (k, v) => javaOpts += YarnSparkHadoopUtil.escapeForShell(s"-D$k=$v") }

    // Commenting it out for now - so that people can refer to the properties if required. Remove
    // it once cpuset version is pushed out.
    // The context is, default gc for server class machines end up using all cores to do gc - hence
    // if there are multiple containers in same node, spark gc effects all other containers
    // performance (which can also be other spark containers)
    // Instead of using this, rely on cpusets by YARN to enforce spark behaves 'properly' in
    // multi-tenant environments. Not sure how default java gc behaves if it is limited to subset
    // of cores on a node.
    /*
        else {
          // If no java_opts specified, default to using -XX:+CMSIncrementalMode
          // It might be possible that other modes/config is being done in
          // spark.executor.extraJavaOptions, so we dont want to mess with it.
          // In our expts, using (default) throughput collector has severe perf ramnifications in
          // multi-tennent machines
          // The options are based on
          // http://www.oracle.com/technetwork/java/gc-tuning-5-138395.html#0.0.0.%20When%20to%20Use
          // %20the%20Concurrent%20Low%20Pause%20Collector|outline
          javaOpts += "-XX:+UseConcMarkSweepGC"
          javaOpts += "-XX:+CMSIncrementalMode"
          javaOpts += "-XX:+CMSIncrementalPacing"
          javaOpts += "-XX:CMSIncrementalDutyCycleMin=0"
          javaOpts += "-XX:CMSIncrementalDutyCycle=10"
        }
    */

    // For log4j configuration to reference
    javaOpts += ("-Dspark.yarn.app.container.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR)
    YarnCommandBuilderUtils.addPermGenSizeOpt(javaOpts)

    val userClassPath = Client.getUserClasspath(sparkConf).flatMap { uri =>
      val absPath =
        if (new File(uri.getPath()).isAbsolute()) {
          Client.getClusterPath(sparkConf, uri.getPath())
        } else {
          Client.buildPath(Environment.PWD.$(), uri.getPath())
        }
      Seq("--user-class-path", "file:" + absPath)
    }.toSeq

    YarnSparkHadoopUtil.addOutOfMemoryErrorArgument(javaOpts)
    val commands = prefixEnv ++ Seq(
      YarnSparkHadoopUtil.expandEnvironment(Environment.JAVA_HOME) + "/bin/java",
      "-server") ++
      javaOpts ++
      Seq("org.apache.spark.executor.CoarseGrainedExecutorBackend",
        "--driver-url", masterAddress,
        "--executor-id", executorId,
        "--hostname", hostname,
        "--cores", executorCores.toString,
        "--app-id", appId) ++
      userClassPath ++
      Seq(
        s"1>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stdout",
        s"2>${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stderr")

    // TODO: it would be nicer to just make sure there are no null commands here
    commands.map(s => if (s == null) "null" else s).toList
  }

  private def setupDistributedCache(
      file: String,
      rtype: LocalResourceType,
      localResources: HashMap[String, LocalResource],
      timestamp: String,
      size: String,
      vis: String): Unit = {
    val uri = new URI(file)
    val amJarRsrc = Records.newRecord(classOf[LocalResource])
    amJarRsrc.setType(rtype)
    amJarRsrc.setVisibility(LocalResourceVisibility.valueOf(vis))
    amJarRsrc.setResource(ConverterUtils.getYarnUrlFromURI(uri))
    amJarRsrc.setTimestamp(timestamp.toLong)
    amJarRsrc.setSize(size.toLong)
    localResources(uri.getFragment()) = amJarRsrc
  }

  private def prepareLocalResources: HashMap[String, LocalResource] = {
    logInfo("Preparing Local resources")
    val localResources = HashMap[String, LocalResource]()

    if (System.getenv("SPARK_YARN_CACHE_FILES") != null) {
      val timeStamps = System.getenv("SPARK_YARN_CACHE_FILES_TIME_STAMPS").split(',')
      val fileSizes = System.getenv("SPARK_YARN_CACHE_FILES_FILE_SIZES").split(',')
      val distFiles = System.getenv("SPARK_YARN_CACHE_FILES").split(',')
      val visibilities = System.getenv("SPARK_YARN_CACHE_FILES_VISIBILITIES").split(',')
      for( i <- 0 to distFiles.length - 1) {
        setupDistributedCache(distFiles(i), LocalResourceType.FILE, localResources, timeStamps(i),
          fileSizes(i), visibilities(i))
      }
    }

    if (System.getenv("SPARK_YARN_CACHE_ARCHIVES") != null) {
      val timeStamps = System.getenv("SPARK_YARN_CACHE_ARCHIVES_TIME_STAMPS").split(',')
      val fileSizes = System.getenv("SPARK_YARN_CACHE_ARCHIVES_FILE_SIZES").split(',')
      val distArchives = System.getenv("SPARK_YARN_CACHE_ARCHIVES").split(',')
      val visibilities = System.getenv("SPARK_YARN_CACHE_ARCHIVES_VISIBILITIES").split(',')
      for( i <- 0 to distArchives.length - 1) {
        setupDistributedCache(distArchives(i), LocalResourceType.ARCHIVE, localResources,
          timeStamps(i), fileSizes(i), visibilities(i))
      }
    }

    logInfo("Prepared Local resources " + localResources)
    localResources
  }

  private def prepareEnvironment(): HashMap[String, String] = {
    val env = new HashMap[String, String]()
    val extraCp = sparkConf.getOption("spark.executor.extraClassPath")
    Client.populateClasspath(null, conf, sparkConf, env, extraCp)

    sparkConf.getExecutorEnv.foreach { case (key, value) =>
      // This assumes each executor environment variable set here is a path
      // This is kept for backward compatibility and consistency with hadoop
      YarnSparkHadoopUtil.addPathToEnvironment(env, key, value)
    }

    // Keep this for backwards compatibility but users should move to the config
    sys.env.get("SPARK_YARN_USER_ENV").foreach { userEnvs =>
      YarnSparkHadoopUtil.setEnvFromInputString(env, userEnvs)
    }

    // lookup appropriate http scheme for container log urls
    val yarnHttpPolicy = conf.get(
      YarnConfiguration.YARN_HTTP_POLICY_KEY,
      YarnConfiguration.YARN_HTTP_POLICY_DEFAULT
    )
    val httpScheme = if (yarnHttpPolicy == "HTTPS_ONLY") "https://" else "http://"

    // Add log urls
    container.foreach { c =>
      sys.env.get("SPARK_USER").foreach { user =>
        val containerId = ConverterUtils.toString(c.getId)
        val address = c.getNodeHttpAddress
        val baseUrl = s"$httpScheme$address/node/containerlogs/$containerId/$user"

        env("SPARK_LOG_URL_STDERR") = s"$baseUrl/stderr?start=-4096"
        env("SPARK_LOG_URL_STDOUT") = s"$baseUrl/stdout?start=-4096"
      }
    }

    System.getenv().asScala.filterKeys(_.startsWith("SPARK"))
      .foreach { case (k, v) => env(k) = v }
    env
  }
}
