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

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.lang.reflect.Field

import org.apache.hadoop.conf.Configuration

private[spark]
class SerializableConfiguration(@transient var value: Configuration) extends Serializable {
  SerializableConfigurationHelper.stripConfigurationPropertySources(value)

  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    value = new Configuration(false)
    value.readFields(in)
  }
}

// Note: *Helper to avoid mucking with SerializationID
private[spark] object SerializableConfigurationHelper {
  private val updatingResourceField: Field =
    classOf[Configuration].getDeclaredField("updatingResource")

  updatingResourceField.setAccessible(true)

  /**
   * The propertySources point at an extensive set of objects to describe the
   * source of the setter for each property.  This turns out to represent about
   * a third or more of the size of a Configuration.  The only place I can find
   * that Hadoop uses this is to determine if one particular field exists in the
   * default file or not.  This is done only on an internally created Configuration.
   * So, we're stripping that info here.  It makes caching less expensive, and
   * broadcasts should be slimmer.
   */
  def stripConfigurationPropertySources(conf: Configuration): Unit = {
    if (conf != null) {
      updatingResourceField.set(conf, new java.util.HashMap[String, Array[String]](1))
    }
  }
}
