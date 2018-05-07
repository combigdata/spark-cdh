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

package org.apache.spark.deploy.yarn.security

import java.io.{ByteArrayInputStream, DataInputStream}

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.mapred.Master
import org.apache.hadoop.security.Credentials

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.yarn.config._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._

private[security] class HDFSCredentialProvider extends ServiceCredentialProvider with Logging {
  // Token renewal interval, this value will be set in the first call,
  // if None means no token renewer specified, so cannot get token renewal interval.
  private var tokenRenewalInterval: Option[Long] = null

  override val serviceName: String = "hdfs"

  override def obtainCredentials(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    // NameNode to access, used to get tokens from different FileSystems
    val tokenRenewer = getTokenRenewer(hadoopConf)
    nnsToAccess(hadoopConf, sparkConf).foreach { dst =>
      getAllTokens(dst, tokenRenewer, hadoopConf, sparkConf, creds)
    }

    // Get the token renewal interval if it is not set. It will only be called once.
    if (tokenRenewalInterval == null) {
      tokenRenewalInterval = getTokenRenewalInterval(hadoopConf, sparkConf)
    }

    // Get the time of next renewal.
    tokenRenewalInterval.map { interval =>
      creds.getAllTokens.asScala
        .filter(_.getKind == DelegationTokenIdentifier.HDFS_DELEGATION_KIND)
        .map { t =>
          val identifier = new DelegationTokenIdentifier()
          identifier.readFields(new DataInputStream(new ByteArrayInputStream(t.getIdentifier)))
          identifier.getIssueDate + interval
      }.foldLeft(0L)(math.max)
    }
  }

  /**
   * CDH-68051: try a few times to get delegation tokens until the number of stored tokens
   * stabilizes. This is needed to try to fetch tokens for all available KMS servers until
   * HADOOP-14445 is properly fixed on supported releases.
   *
   * This relies on the fact that the KMS client libraries acquire tokens from the different
   * servers by going through the list in round-robin order.
   */
  private def getAllTokens(
      dst: Path,
      tokenRenewer: String,
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Unit = {
    logInfo("getting token for: " + dst)
    val dstFs = dst.getFileSystem(hadoopConf)

    def credentialCount(_creds: Credentials): Int = {
      _creds.numberOfTokens() + _creds.numberOfSecretKeys()
    }

    val maxAttempts = sparkConf.get(FS_CREDENTIALS_MAX_FETCH_ATTEMPTS)
    var lastCount = -1
    var remainingAttempts = maxAttempts
    while (remainingAttempts > 0 && (lastCount == -1 || lastCount != credentialCount(creds))) {
      remainingAttempts -= 1
      lastCount = credentialCount(creds)
      dstFs.addDelegationTokens(tokenRenewer, creds)
      logDebug(s"Last count: $lastCount; new count: ${credentialCount(creds)}")
    }

    if (remainingAttempts == 0) {
      logWarning(
        s"Reached maximum number of attempts ($maxAttempts) while waiting for tokens to " +
        "stabilize. Some tokens may be missing from user credentials.")
    }

    if (log.isDebugEnabled) {
      logDebug(s"Tokens for path $dst:")
      SparkHadoopUtil.get.dumpTokens(creds).foreach { t =>
        logDebug(s"  $t")
      }
    }
  }

  private def getTokenRenewalInterval(
      hadoopConf: Configuration, sparkConf: SparkConf): Option[Long] = {
    // We cannot use the tokens generated with renewer yarn. Trying to renew
    // those will fail with an access control issue. So create new tokens with the logged in
    // user as renewer.
    sparkConf.get(PRINCIPAL).flatMap { renewer =>
      val creds = new Credentials()
      nnsToAccess(hadoopConf, sparkConf).foreach { dst =>
        val dstFs = dst.getFileSystem(hadoopConf)
        dstFs.addDelegationTokens(renewer, creds)
      }
      val hdfsToken = creds.getAllTokens.asScala
        .find(_.getKind == DelegationTokenIdentifier.HDFS_DELEGATION_KIND)
      hdfsToken.map { t =>
        val newExpiration = t.renew(hadoopConf)
        val identifier = new DelegationTokenIdentifier()
        identifier.readFields(new DataInputStream(new ByteArrayInputStream(t.getIdentifier)))
        val interval = newExpiration - identifier.getIssueDate
        logInfo(s"Renewal Interval is $interval")
        interval
      }
    }
  }

  private def getTokenRenewer(conf: Configuration): String = {
    val delegTokenRenewer = Master.getMasterPrincipal(conf)
    logDebug("delegation token renewer is: " + delegTokenRenewer)
    if (delegTokenRenewer == null || delegTokenRenewer.length() == 0) {
      val errorMessage = "Can't get Master Kerberos principal for use as renewer"
      logError(errorMessage)
      throw new SparkException(errorMessage)
    }

    delegTokenRenewer
  }

  private def nnsToAccess(hadoopConf: Configuration, sparkConf: SparkConf): Set[Path] = {
    sparkConf.get(NAMENODES_TO_ACCESS).map(new Path(_)).toSet +
      sparkConf.get(STAGING_DIR).map(new Path(_))
        .getOrElse(FileSystem.get(hadoopConf).getHomeDirectory)
  }
}
