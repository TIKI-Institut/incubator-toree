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

import org.apache.toree.magic._
import org.apache.toree.magic.dependencies.{IncludeKernel, IncludeOutputStream}
import org.apache.toree.plugins.annotations.Event
import org.apache.toree.utils.ArgumentParsingSupport

class Awsume
  extends LineMagic
    with ArgumentParsingSupport
    with IncludeOutputStream
    with IncludeKernel {

  // Lazy because the outputStream is not provided at construction
  private def printStream = new PrintStream(outputStream)

  private val _mfaToken = parser.accepts(
    "mfa-token",
    "Adds a mfa-token to the awsume command"
  ).withRequiredArg().ofType(classOf[Int])

  @Event(name = "awsume")
  override def execute(code: String): Unit = {
    val nonOptionalargs = parseArgs(code)

    val mfaTokenValue = get(_mfaToken).getOrElse(-1)

    if (nonOptionalargs.size == 1) {
      val awsume_result = if (mfaTokenValue == -1) {
        runAwsumeCmd(nonOptionalargs.head)
      } else {
        runAwsumeCmd("%s --mfa-token %s".format(nonOptionalargs.head, mfaTokenValue))
      }


      if (awsume_result.contains("error")) {
        printStream.println(s"There was an error with awsume.")
        printHelp(outputStream, """%%Awsume profile --mfa-token mfa-token""")
      } else {

        val awsCredMap = awsume_result
          .split("\n")
          .map(cmd => {
            val cmdCleaned = cmd
              .replace("export ", "")
              .split("=")

            cmdCleaned.head -> cmdCleaned(1)
          }).toMap
        // set KEYs
        kernel.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
        kernel.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", awsCredMap("AWS_ACCESS_KEY_ID"))
        kernel.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", awsCredMap("AWS_SECRET_ACCESS_KEY"))
        kernel.sparkContext.hadoopConfiguration.set("fs.s3a.session.token", awsCredMap("AWS_SESSION_TOKEN"))

      }

    }
  }

  def runAwsumeCmd(profile: String): String = {
    sys.process.Process("awsume %s -s".format(profile)).!!
  }

}
