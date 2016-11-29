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

import java.io.{InputStream, OutputStream}
import java.util.Properties
import javax.crypto.KeyGenerator

import com.intel.chimera.cipher._
import com.intel.chimera.random._
import com.intel.chimera.stream._

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.crypto.CryptoConf._
import org.apache.spark.deploy.SparkHadoopUtil

/**
 * A util class for manipulating file shuffle encryption and decryption streams.
 */
private[spark] object CryptoStreamUtils {
  // The initialization vector length in bytes.
  val IV_LENGTH_IN_BYTES = 16
  // The prefix of Crypto related configurations in Spark configuration.
  val SPARK_CHIMERA_CONF_PREFIX = "spark.shuffle.crypto."
  // The prefix for the configurations passing to Chimera library.
  val CHIMERA_CONF_PREFIX = "chimera.crypto."

  /**
   * Wrap an output stream for encryption if there's a key registered with the app's
   * SecurityManager.
   */
  def wrapForEncryption(os: OutputStream, conf: SparkConf): OutputStream = {
    Option(SparkEnv.get).flatMap(_.securityManager.getIOEncryptionKey()) match {
      case Some(key) =>
        createCryptoOutputStream(os, conf, key)
      case None =>
        os
    }
  }

  /**
   * Wrap an input stream for encryption if there's a key registered with the app's
   * SecurityManager.
   */
  def wrapForEncryption(is: InputStream, conf: SparkConf): InputStream = {
    Option(SparkEnv.get).flatMap(_.securityManager.getIOEncryptionKey()) match {
      case Some(key) =>
        createCryptoInputStream(is, conf, key)
      case None =>
        is
    }
  }

  /**
   * Helper method to wrap [[OutputStream]] with [[CryptoOutputStream]] for encryption.
   */
  def createCryptoOutputStream(
      os: OutputStream,
      sparkConf: SparkConf,
      key: Array[Byte]): CryptoOutputStream = {
    val properties = toChimeraConf(sparkConf)
    val iv: Array[Byte] = createInitializationVector(properties)
    os.write(iv)
    val transformationType = getCipherTransformationType(sparkConf)
    new CryptoOutputStream(transformationType, properties, os, key, iv)
  }

  /**
   * Helper method to wrap [[InputStream]] with [[CryptoInputStream]] for decryption.
   */
  def createCryptoInputStream(
      is: InputStream,
      sparkConf: SparkConf,
      key: Array[Byte]): CryptoInputStream = {
    val properties = toChimeraConf(sparkConf)
    val iv = new Array[Byte](IV_LENGTH_IN_BYTES)
    is.read(iv, 0, iv.length)
    val transformationType = getCipherTransformationType(sparkConf)
    new CryptoInputStream(transformationType, properties, is, key, iv)
  }

  /**
   * Get Chimera configurations from Spark configurations identified by prefix.
   */
  def toChimeraConf(conf: SparkConf): Properties = {
    val props = new Properties()
    conf.getAll.foreach { case (k, v) =>
      if (k.startsWith(SPARK_CHIMERA_CONF_PREFIX)) {
        props.put(CHIMERA_CONF_PREFIX + k.substring(SPARK_CHIMERA_CONF_PREFIX.length()), v)
      }
    }
    props
  }

  /**
   * Creates a new encryption key.
   */
  def createKey(conf: SparkConf): Array[Byte] = {
    val keyLen = conf.getInt(SPARK_SHUFFLE_ENCRYPTION_KEY_SIZE_BITS,
      DEFAULT_SPARK_SHUFFLE_ENCRYPTION_KEY_SIZE_BITS)
    val keyGenAlgorithm = conf.get(SPARK_SHUFFLE_ENCRYPTION_KEYGEN_ALGORITHM,
      DEFAULT_SPARK_SHUFFLE_ENCRYPTION_KEYGEN_ALGORITHM)
    val keyGen = KeyGenerator.getInstance(keyGenAlgorithm)
    keyGen.init(keyLen)
    keyGen.generateKey().getEncoded()
  }

  /**
   * Get the cipher transformation type
   */
  private[this] def getCipherTransformationType(sparkConf: SparkConf): CipherTransformation = {
    val transformationStr = sparkConf.get("spark.shuffle.crypto.cipher.transformation",
      "AES/CTR/NoPadding")
    CipherTransformation.values().find(
      (cipherTransformation: CipherTransformation) => cipherTransformation.getName() ==
          transformationStr).get
  }

  /**
   * This method to generate an IV (Initialization Vector) using secure random.
   */
  private[this] def createInitializationVector(properties: Properties): Array[Byte] = {
    val iv = new Array[Byte](IV_LENGTH_IN_BYTES)
    SecureRandomFactory.getSecureRandom(properties).nextBytes(iv)
    iv
  }
}
