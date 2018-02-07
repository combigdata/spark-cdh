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

import java.io.Closeable

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.Credentials
import org.apache.hadoop.security.token.{Token, TokenIdentifier}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[security] class HBaseCredentialProvider extends ServiceCredentialProvider with Logging {

  override def serviceName: String = "hbase"

  override def obtainCredentials(
      hadoopConf: Configuration,
      sparkConf: SparkConf,
      creds: Credentials): Option[Long] = {
    val cl = Utils.getContextOrSparkClassLoader
    val connClass = cl.loadClass("org.apache.hadoop.hbase.client.Connection")
    val connFactory = cl.loadClass("org.apache.hadoop.hbase.client.ConnectionFactory")
      .getMethod("createConnection", classOf[Configuration])
    val obtainToken = cl.loadClass("org.apache.hadoop.hbase.security.token.TokenUtil")
      .getMethod("obtainToken", connClass)

    logDebug("Attempting to fetch HBase security token.")
    val conn = connFactory.invoke(null, hbaseConf(hadoopConf))
    try {
      val token = obtainToken.invoke(null, conn).asInstanceOf[Token[_ <: TokenIdentifier]]
      logInfo(s"Get token from HBase: ${token.toString}")
      creds.addToken(token.getService, token)
    } finally {
      conn.asInstanceOf[Closeable].close()
    }

    None
  }

  override def credentialsRequired(hadoopConf: Configuration): Boolean = {
    try {
      hbaseConf(hadoopConf).get("hbase.security.authentication") == "kerberos"
    } catch {
      case e: ClassNotFoundException =>
        logDebug("HBase not found in class path, HBase tokens will not be fetched.", e)
        false
    }
  }

  private def hbaseConf(conf: Configuration): Configuration = {
    val confCreate = Utils.getContextOrSparkClassLoader
      .loadClass("org.apache.hadoop.hbase.HBaseConfiguration")
      .getMethod("create", classOf[Configuration])
    confCreate.invoke(null, conf).asInstanceOf[Configuration]
  }

}
