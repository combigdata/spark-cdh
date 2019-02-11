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

import java.lang.reflect.Constructor

import scala.util.Try

import org.apache.commons.lang3.ClassUtils
import org.apache.commons.lang3.reflect.{ConstructorUtils => CommonConstructorUtils}

import org.apache.spark.internal.Logging

/**
 * Workaround a classpath issue which comes from using an old commons-lang3 on jdk11.  See CDH-77734
 * for more details.
 */
private[spark] object ConstructorUtils extends Logging {

  def getMatchingAccessibleConstructor(cls: Class[_], paramTypes: Class[_]*): Constructor[_] = {
    Try {
      CommonConstructorUtils.getMatchingAccessibleConstructor(cls, paramTypes: _*)
    }.getOrElse {
      getCtorHelper(cls, paramTypes)
    }
  }

  private[util] def getCtorHelper(cls: Class[_], paramTypes: Seq[Class[_]]): Constructor[_] = {
    // commons-lang3 has a somewhat complex ordering of constructors, which I'm not going to
    // try to reimplement here.  But hopefully we can handle the simple cases, and loudly warn in
    // other cases.
    val allCtors = cls.getConstructors()
    val matching = allCtors.filter { ctor =>
      // varargs not handled
      ctor.getParameterTypes().length == paramTypes.length &&
        ctor.getParameterTypes.zip(paramTypes).forall { case (ctorParamType, passedType) =>
          // this should be present in every version of commons-lang3 we might have
          ClassUtils.isAssignable(passedType, ctorParamType, true)
        }
    }
    if (matching.length == 0) {
      null
    } else {
      if (matching.length > 1) {
        logWarning(s"Using commons-lang3 workaround on jdk11.  Found multiple matching " +
          s"constructors for $cls with params $paramTypes.")
      }
      matching.head
    }
  }

}
