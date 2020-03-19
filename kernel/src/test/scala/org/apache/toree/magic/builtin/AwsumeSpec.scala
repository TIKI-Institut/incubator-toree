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

import java.io.OutputStream

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.toree.kernel.api.KernelLike
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSpec, Matchers}

class AwsumeSpec extends FunSpec with Matchers with MockitoSugar {
  describe("Awsume") {
    describe("#execute") {

      it("should set aws envs ") {
        val mockKernel = mock[KernelLike]
        val mockHadoopConfiguration = new Configuration()
        val mockSparkContext = mock[SparkContext]
        val mockOutputStream = mock[OutputStream]

        val awsumeMagic = spy(new Awsume)
        doReturn(mockOutputStream)
          .when(awsumeMagic).outputStream
        doReturn(mockKernel)
          .when(awsumeMagic).kernel
        doReturn(mockSparkContext)
          .when(mockKernel).sparkContext
        doReturn(mockHadoopConfiguration)
          .when(mockSparkContext).hadoopConfiguration

        val code = "test-profile"

        doReturn("export AWS_ACCESS_KEY_ID=ASIAS\nexport AWS_SECRET_ACCESS_KEY=TihKG2/X4\nexport AWS_SESSION_TOKEN=FwoGZXIvYXdzEPn\nexport AWS_SECURITY_TOKEN=FwoGZXIvYXdzEPn\nexport AWS_REGION=eu-central-1\n export AWS_DEFAULT_REGION=eu-central-1\nexport AWSUME_PROFILE=%s".format(code))
          .when(awsumeMagic).runAwsumeCmd(any[String])

        awsumeMagic.execute(code)

        mockHadoopConfiguration.get("fs.s3a.access.key") shouldBe "ASIAS"
        mockHadoopConfiguration.get("fs.s3a.secret.key") shouldBe "TihKG2/X4"
        mockHadoopConfiguration.get("fs.s3a.session.token") shouldBe "FwoGZXIvYXdzEPn"
        mockHadoopConfiguration.get("fs.s3a.endpoint") shouldBe "eu-central-1"

      }

      it("should set aws envs with mfa-token ") {

        val code = "test-profile --mfa-token 123456"
        val mockKernel = mock[KernelLike]
        val mockHadoopConfiguration = new Configuration()
        val mockSparkContext = mock[SparkContext]
        val mockOutputStream = mock[OutputStream]

        val awsumeMagic = spy(new Awsume)
        doReturn(mockOutputStream)
          .when(awsumeMagic).outputStream
        doReturn(mockKernel)
          .when(awsumeMagic).kernel
        doReturn(mockSparkContext)
          .when(mockKernel).sparkContext
        doReturn(mockHadoopConfiguration)
          .when(mockSparkContext).hadoopConfiguration

        doReturn("export AWS_ACCESS_KEY_ID=ASIAS\nexport AWS_SECRET_ACCESS_KEY=TihKG2/X4\nexport AWS_SESSION_TOKEN=FwoGZXIvYXdzEPn\nexport AWS_SECURITY_TOKEN=FwoGZXIvYXdzEPn\nexport AWS_REGION=eu-central-1\n export AWS_DEFAULT_REGION=eu-central-1\nexport AWSUME_PROFILE=test-profile")
          .when(awsumeMagic).runAwsumeCmd(any[String])

        awsumeMagic.execute(code)

        mockHadoopConfiguration.get("fs.s3a.access.key") shouldBe "ASIAS"
        mockHadoopConfiguration.get("fs.s3a.secret.key") shouldBe "TihKG2/X4"
        mockHadoopConfiguration.get("fs.s3a.session.token") shouldBe "FwoGZXIvYXdzEPn"

      }

      it("should print help with wrong command") {

        val mockOutputStream = mock[OutputStream]

        val awsumeMagic = spy(new Awsume)

        doReturn(mockOutputStream)
          .when(awsumeMagic).outputStream

        doReturn("Enter MFA token: Awsume error: EOF when reading a line")
          .when(awsumeMagic).runAwsumeCmd(any[String])

        awsumeMagic.execute("test")


      }

    }
  }
}
