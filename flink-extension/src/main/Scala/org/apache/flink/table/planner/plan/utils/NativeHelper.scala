package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.planner.plan.nodes.exec.glue.jni.{JniBridge, KafkaConsumerManager}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.security.UserGroupInformation

import java.net.URI
import java.security.PrivilegedExceptionAction

object NativeHelper {
  val currentUser: UserGroupInformation = UserGroupInformation.getCurrentUser
  def putJniBridgeResource(resourceId: String, hadoopConfig: Configuration): Unit = {
    JniBridge.resourcesMap.put(
      resourceId,
      (location: String) => {
        val fs = NativeHelper.currentUser.doAs(new PrivilegedExceptionAction[FileSystem] {
          override def run(): FileSystem = {
            FileSystem.get(new URI(location), hadoopConfig)
          }
        })
        fs
      })
  }

  def putKafkaConsumerResource(resourceId: String, kafkaConsumerManager: KafkaConsumerManager): Unit = {
    JniBridge.kafkaConsumerResourcesMap.put(
      resourceId,
      kafkaConsumerManager
    )
  }
}
