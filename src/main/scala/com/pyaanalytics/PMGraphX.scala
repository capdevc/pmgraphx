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
package com.pyaanalytics

import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scopt.OptionParser
import scala.xml._

class VertexProperty()
case class AuthorProperty(name: String) extends VertexProperty
case class PaperProperty(pmid: Int) extends VertexProperty

case class Vertex(vid: VertexId, prop: VertexProperty)

object PMGraphX {

  case class PMGraphXConfig(vertexPath: String = "",
                            edgePath: String = "",
                            sparkMaster: String = "local",
                            userName: String = "spark")

  def hash64(string: String): Long = {
    string.map(_.toLong).foldLeft(1125899906842597L)((h: Long, c: Long) => 31 * h + c)
  }


  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[PMGraphXConfig]("PMGraphX") {

      arg[String]("sparkMaster") valueName("sparkMaster") action {
        (x, c) => c.copy(sparkMaster = x)
      }


      opt[String]('u', "userName") valueName("userName") action {
        (x, c) => c.copy(userName = x)
      }

      arg[String]("vertexPath") valueName("vertexPath") action {
        (x, c) => c.copy(vertexPath = x)
      }

      arg[String]("edgePath") valueName("edgePath") action {
        (x, c) => c.copy(edgePath = x)
      }
    }

    parser.parse(args, PMGraphXConfig()) match {
      case Some(config) => {
        val sparkConf = new SparkConf()
          .setAppName("Pubmed GraphX Stuff")
          .setMaster(config.sparkMaster)
          .set("spark.serializer", "org.apache.spark.serializer.KyroSerializer")
        sparkConf.registerKryoClasses(Array(classOf[Vertex],
                                            classOf[VertexProperty],
                                            classOf[AuthorProperty],
                                            classOf[PaperProperty]))
        val sc = new SparkContext(sparkConf)

        val vertexRDD: RDD[(VertexId, VertexProperty)] = sc.objectFile(config.vertexPath, 1)
        val edgeRDD: RDD[Edge[Null]] = sc.objectFile(config.edgePath, 1)

        val defVertex = (PaperProperty(0))
        val graph = Graph(vertexRDD, edgeRDD, defVertex)

        println(graph.numEdges)
        sc.stop()
      } case None => {
        System.exit(1)
      }
    }
  }
}
