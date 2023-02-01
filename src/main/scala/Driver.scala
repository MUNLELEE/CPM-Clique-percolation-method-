import org.apache.spark.graphx.VertexId
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Driver {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("cpm")
        val sc = new SparkContext(conf)
        // 类初始化成员变量
        val P: mutable.Set[VertexId] = mutable.Set()
        val R: mutable.Set[VertexId] = mutable.Set()
        val X: mutable.Set[VertexId] = mutable.Set()
        val community: mutable.Set[mutable.Set[VertexId]] = mutable.Set()
        val inputDir: String = "data/sample.txt"
        //        val inputDir: String = "data/roadNet-TX.txt"
        val outputDir: String = "output"
        val cpmConf: CPMconfig = CPMconfig(inputDir, outputDir, 2)

        val cpm = new CPM(R, P, X, community, cpmConf)

        cpm.run(sc)
        sc.stop()
//var set1: mutable.Set[Int] = mutable.Set()
//        val set2: mutable.Set[Int] = mutable.Set(1, 2, 3)
//        set1 = set1.union(set2)
//        println(set1)
    }
}
