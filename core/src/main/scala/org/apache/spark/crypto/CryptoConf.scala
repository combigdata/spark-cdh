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

import org.apache.spark.SparkConf

/**
 * CryptoConf is a class for Crypto configuration
 */
private[spark] object CryptoConf {
  /**
   * Constants and variables for spark shuffle file encryption
   */
  val SPARK_SHUFFLE_ENCRYPTION_ENABLED = "spark.shuffle.encryption.enabled"
  val SPARK_SHUFFLE_ENCRYPTION_KEYGEN_ALGORITHM = "spark.shuffle.encryption.keygen.algorithm"
  val DEFAULT_SPARK_SHUFFLE_ENCRYPTION_KEYGEN_ALGORITHM = "HmacSHA1"
  val SPARK_SHUFFLE_ENCRYPTION_KEY_SIZE_BITS = "spark.shuffle.encryption.keySizeBits"
  val DEFAULT_SPARK_SHUFFLE_ENCRYPTION_KEY_SIZE_BITS = 128

  /**
   * Check whether shuffle file encryption is enabled. It is disabled by default.
   */
  def isShuffleEncryptionEnabled(sparkConf: SparkConf): Boolean = {
    if (sparkConf != null) {
      sparkConf.getBoolean(SPARK_SHUFFLE_ENCRYPTION_ENABLED, false)
    } else {
      false
    }
  }

}

