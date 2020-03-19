/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License
 */

package org.apache.toree.magic.builtin

import java.io.PrintStream
import java.util.{Map => JavaMap}

import com.google.common.base.Strings
import org.apache.toree.kernel.protocol.v5.MIMEType
import org.apache.toree.magic._
import org.apache.toree.magic.dependencies.IncludeOutputStream
import org.apache.toree.plugins.annotations.Event
import org.apache.toree.utils.ArgumentParsingSupport

import scala.reflect.internal.util.Collections

class Awsume
  extends LineMagic
    with ArgumentParsingSupport
    with IncludeOutputStream {

  // Lazy because the outputStream is not provided at construction
  private def printStream = new PrintStream(outputStream)

  private val _mfaToken = parser.accepts(
    "mfa-token",
    "Adds a mfa-token to the awsume command"
  ).withRequiredArg().ofType(classOf[Int])

  @Event(name = "awsume")
  override def execute(code: String): Unit = {
    import scala.collection.JavaConverters._
    val nonOptionalargs = parseArgs(code)
    val mfaTokenValue = get(_mfaToken).getOrElse(-1)

    if (nonOptionalargs.size == 1) {
      val awsume_result = if(mfaTokenValue == -1){
        runAwsumeCmd(nonOptionalargs.head)
      }else{
        runAwsumeCmd("%s --mfa-token %s".format(nonOptionalargs.head, mfaTokenValue))
      }


      if (awsume_result.contains("error")) {
        printStream.println(s"There was an error with awsume.")
        printHelp(outputStream, """%%Awsume profile --mfa-token mfa-token""")
      } else {
        awsume_result
          .split("\n")
          .foreach(cmd => {
            val cmdCleaned = cmd
              .replace("export ", "")
              .split("=")
            val envName = cmdCleaned.head
            val envValue = cmdCleaned(1)
            val mapTest =
              Map(envName -> envValue)
                .asJava
            setEnv(mapTest)
          })
      }

    }
  }

  def runAwsumeCmd(profile:String):String = {
    sys.process.Process("awsume %s -s".format(profile)).!!
  }
  def setEnv(newEnv: JavaMap[String, String]): Unit = {
    try {
      val processEnvironmentClass =
        Class.forName("java.lang.ProcessEnvironment")
      val theEnvironmentField =
        processEnvironmentClass.getDeclaredField("theEnvironment")
      theEnvironmentField.setAccessible(true)
      val env =
        theEnvironmentField.get(null).asInstanceOf[JavaMap[String, String]]
      env.putAll(newEnv)
      val theCaseInsensitiveEnvironmentField =
        processEnvironmentClass.getDeclaredField(
          "theCaseInsensitiveEnvironment")
      theCaseInsensitiveEnvironmentField.setAccessible(true)
      val cienv = theCaseInsensitiveEnvironmentField
        .get(null)
        .asInstanceOf[JavaMap[String, String]]
      cienv.putAll(newEnv)
    } catch {
      case e: NoSuchFieldException =>
        try {
          val classes = classOf[Collections].getDeclaredClasses()
          val env = System.getenv()
          for (cl <- classes) {
            if (cl.getName() == "java.util.Collections$UnmodifiableMap") {
              val field = cl.getDeclaredField("m")
              field.setAccessible(true)
              val obj = field.get(env)
              val map = obj.asInstanceOf[JavaMap[String, String]]
              map.clear()
              map.putAll(newEnv)
            }
          }
        } catch {
          case e2: Exception => e2.printStackTrace()
        }

      case e1: Exception => e1.printStackTrace()
    }
  }
}
