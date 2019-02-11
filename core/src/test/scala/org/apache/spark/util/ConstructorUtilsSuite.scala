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

import org.apache.spark.SparkFunSuite

class ConstructorUtilsSuite extends SparkFunSuite {
  test("getCtorHelper") {
    // String should match against CharSequence; jLong should match against Long
    val ctor1 = ConstructorUtils.getCtorHelper(
      classOf[Foo], Seq(classOf[String], classOf[String], classOf[java.lang.Long]))
    assert(ctor1 != null)
    assert(ctor1.getParameterTypes.length == 3)

    // CharSequence does not match against String
    val ctor2 = ConstructorUtils.getCtorHelper(
      classOf[Foo], Seq(classOf[CharSequence], classOf[String], classOf[java.lang.Long]))
    assert(ctor2 == null)

    // exact match against secondary constructor
    val ctor3 = ConstructorUtils.getCtorHelper(
      classOf[Foo], Seq(classOf[String], classOf[java.lang.Integer]))
    assert(ctor3 != null)
    assert(ctor3.getParameterTypes.length == 2)

    // doesn't match primary constructor b/c of extra arg
    val ctor4 = ConstructorUtils.getCtorHelper(
      classOf[Foo], Seq(classOf[String], classOf[String], classOf[java.lang.Long], classOf[String]))
    assert(ctor4 == null)
  }
}

class Foo(
  x: String,
  y: CharSequence,
  z: Long
) {
  def this(x: String, z: java.lang.Integer) = this(null, null, 0L)
}
