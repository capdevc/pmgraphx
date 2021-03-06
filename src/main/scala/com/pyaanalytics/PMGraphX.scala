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
import scala.io.Source
import scala.xml._

class VertexProperty()
case class AuthorProperty(name: String) extends VertexProperty
case class PaperProperty(pmid: Int) extends VertexProperty

class VP2()
case class AP(name: String, evidence: Int, linked: Int) extends VP2
case class PP(pmid: Int, evidence: Boolean, cites: Boolean, cited: Int) extends VP2

case class Vertex(vid: VertexId, prop: VertexProperty)

object PMGraphX {

  case class PMGraphXConfig(inDir: String = "",
                            pmidFile: String = "",
                            outDir: String = ""
                            // sparkMaster: String = "local",
                            )
  implicit class CartProdable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for {x <- xs; y <- ys} yield (x, y)
  }

  def hash64(string: String): Long = {
    string.map(_.toLong).foldLeft(1125899906842597L)((h: Long, c: Long) => 31 * h + c)
  }


  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[PMGraphXConfig]("PMGraphX") {

      arg[String]("inDir") valueName("inDir") action {
        (x, c) => c.copy(inDir = x)
      }

      arg[String]("pmidFile") valueName("pmidFile") action {
        (x, c) => c.copy(pmidFile = x)
      }

      arg[String]("outDir") valueName("outDir") action {
        (x, c) => c.copy(outDir = x)
      }

      // arg[String]("sparkMaster") valueName("sparkMaster") action {
      //   (x, c) => c.copy(sparkMaster = x)
      // }
    }

    parser.parse(args, PMGraphXConfig()) match {
      case Some(config) => {
        val pmids = Source.fromFile(config.pmidFile).getLines map { _.toInt } toSet

        val sparkConf = new SparkConf()
          .setAppName("Pubmed GraphX Stuff")
          // .setMaster(config.sparkMaster)
          .set("spark.serializer", "org.apache.spark.serializer.KyroSerializer")
          .set("spark.executor.memory", "250g")
          .set("spark.driver.memory", "250g")

        sparkConf.registerKryoClasses(Array(classOf[Vertex],
                                            classOf[VertexProperty],
                                            classOf[AuthorProperty],
                                            classOf[PaperProperty]))
        val sc = new SparkContext(sparkConf)

        val vfileRDD: RDD[Vertex] = sc.objectFile(config.inDir ++ "/vertices", 1)
        val edgeRDD: RDD[Edge[Int]] = sc.objectFile(config.inDir ++ "/edges", 1)

        val vertexRDD: RDD[(VertexId, VertexProperty)] = vfileRDD map {
          vert => (vert.vid, vert.prop)
        }
        val defVertex = (PaperProperty(0))
        val graph = Graph(vertexRDD, edgeRDD, defVertex) groupEdges {case (x, y) => x + y}

        val evidenceGraph = graph.mapVertices(
          (vid, prop) => prop match {
            case prop: AuthorProperty => AP(prop.name, 0, 0)
            case prop: PaperProperty => PP(prop.pmid, pmids.contains(prop.pmid), false, 0)
          }
        )

        val linkedPapers: VertexRDD[(Boolean, Int)] = evidenceGraph.aggregateMessages(
          ec => ec.toEdgeTriplet.toTuple match {
            case (_, (dId, dAtt: AP), _) => Unit
            case (_, (dId, dAtt: PP), _) => if (pmids.contains(dAtt.pmid)) {
              ec.sendToDst(false, 1)
              ec.sendToSrc(true, 0)
            }
          },
          {case ((cs1, cd1), (cs2, cd2)) => (cs1 || cs2,  cd1 + cd2)}
        )

        val paperGraph = evidenceGraph.joinVertices(linkedPapers)(
          (vid, prop, u) => prop match {
            case prop: AP => AP(prop.name, prop.evidence, prop.linked)
            case prop: PP => PP(prop.pmid, prop.evidence, u._1, u._2)
          }
        )

        val authEdges: RDD[Edge[Int]] = paperGraph.collectNeighborIds(EdgeDirection.Out)
          .flatMap(
          vArray => (vArray._2.toTraversable cross vArray._2.toTraversable).map(xs => xs match {
                                                          case (x, y) => Edge(x, y, 1)
                                                        }
          )
        )

        val fullGraph = Graph(paperGraph.vertices, paperGraph.edges ++ authEdges)
          .groupEdges((x, y) => x + y)

        println(fullGraph.numEdges)
        // fullGraph.edges.saveAsTextFile(config.outDir ++ "/text_edges")
        fullGraph.edges.saveAsObjectFile(config.outDir ++ "/edges")
        // fullGraph.vertices.saveAsTextFile(config.outDir ++ "/text_vertices")
        fullGraph.vertices.saveAsObjectFile(config.outDir ++ "/vertices")
        sc.stop()
      } case None => {
        System.exit(1)
      }
    }
  }
}
