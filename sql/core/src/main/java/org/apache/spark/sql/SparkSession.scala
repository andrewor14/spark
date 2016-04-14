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

package org.apache.spark.sql

import scala.util.control.NonFatal

import org.apache.spark.SparkContext
import org.apache.spark.sql.internal.{SQLConf, SessionState}
import org.apache.spark.util.Utils


// TODO: This extends SQLContext for now because we pass that around everywhere.
// Before Spark 2.0, we should figure out a way to allow SparkSession and SQLContext
// reuse the existing code in a way that doesn't couple the two as much.

/**
 * The entry point to Spark execution.
 */
final class SparkSession private[spark](sparkContext: SparkContext)
  extends SQLContext(sparkContext) { self =>

  @transient
  protected[sql] override lazy val sessionState: SessionState = {
    val className = SparkSession.getSessionStateClassName
    try {
      val clazz = Utils.classForName(className)
      val ctor = clazz.getDeclaredConstructor(classOf[SQLContext])
      ctor.newInstance(self).asInstanceOf[SessionState]
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException("Unable to instantiate SparkSession!", e)
    }
  }

}


private object SparkSession {

  /**
   * Return the appropriate session state class name based on the catalog implementation.
   */
  private def getSessionStateClassName: String = {
    sys.props.get("spark.catalog").getOrElse("hive") match {
      case "hive" => "org.apache.spark.sql.hive.HiveSessionState"
      case "in-memory" => classOf[SessionState].getCanonicalName
      case invalid =>
        throw new IllegalArgumentException(
          s"Invalid setting of '${SQLConf.CATALOG_IMPLEMENTATION.key}': $invalid. " +
          s"Please specify either 'hive' or 'in-memory'.")
    }
  }

}
