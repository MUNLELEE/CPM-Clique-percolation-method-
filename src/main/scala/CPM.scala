import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD

import scala.collection.{immutable, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Set

class CPM(var R: Set[VertexId], var P: Set[VertexId], var X: Set[VertexId], var community: Set[Set[VertexId]],
          var config: CPMconfig) {
    /**
     * 清空集合
     */
    def clearSet(): Unit = {
        R.clear()
        P.clear()
        X.clear()
        community.clear()
    }

     /**
     * 用于过滤团
     * @param storeC 存储所有极大团的集合
     * @param ts 过滤的阈值
     * @return 过滤后的所有图节点所在的集合
     */
    def filterClique(storeC: Set[Set[VertexId]], ts: Int): Set[VertexId] = {
        val res: Set[VertexId] = Set()
        storeC.foreach(setItem => {
            val size: Int = setItem.size
            if (size > ts) {
                res.union(setItem)  // 合并两个集合，里面的元素都是节点ID
            }
        })
        res
    }
    /**
     * 使用BornKerbosch算法寻找最大团
     * @param R 存在于极大团中的点
     * @param P 可能可以加入及打团的点
     * @param X 用于判重的点集合
     * @param ans 将最后寻找到的所有极大团保存
     * @param neighbor 存储每个节点的邻居节点，是一个map
     */
    def bornKerbosch(R: Set[VertexId], P: Set[VertexId], X: Set[VertexId], ans: Set[Set[VertexId]],
                     neighbor: Map[VertexId, Set[VertexId]]): Unit = {
        if (P.toList.isEmpty && X.toList.isEmpty) {  // 递归退出的条件
            ans.add(R)
        } else {
            val pivot: VertexId = P.union(X).head  // 枢纽元素
            val nu: mutable.Set[VertexId] = neighbor(pivot)
            for (v <- P.diff(nu)) {  // 取一个顶点
                val vNu: mutable.Set[VertexId] = neighbor(v)  // 所取顶点的邻居节点集合
                bornKerbosch(R + v, P.intersect(vNu), X.intersect(vNu), ans, neighbor)
                P.remove(v)
                X.add(v)
            }
        }
    }

    /**
     * 记录应该合并的团，也就是得到cpm算法的矩阵
     * @param community 传入的所有的社团集合
     * @return 得到的是与社团集合相对应的集合，其中每个存放的是相应社团与哪些索引社团应该合并
     */
    def cpmMatrix(community: Set[Set[VertexId]]): Array[Set[Int]] = {
        // 创建矩阵，因为如果是真正的矩阵空间占用较大，这里选择创建集合型矩阵
        val matrix: Array[mutable.Set[Int]] = new Array[mutable.Set[Int]](community.size)
        var i: Int = 0
        community.foreach(item => {
            var j: Int = 0
            matrix(i) = Set()
            community.foreach(item2 => {
                // 两者之间交集
                val lap: Int = item.intersect(item2).size
                if (i == j && lap >= config.kVal) { // 表示对角线，如果是对角线要大于k
                    matrix(i).add(j)
                }
                if (i != j && lap >= config.kVal - 1) {  // 表示非对角线，需要大于k-1
                    matrix(i).add(j)
                }
                j += 1
            })
            i += 1
        })
        matrix
    }

    /**
     * 进行合并操作，将需要合并的社团所有顶点放在集合中
     * @param community 使用BK算法划分好的极大团集合
     * @param idxStore 存储每个社团和哪些社团（索引）合并
     * @return 返回所有合并后的社团，每个社团的顶点放在一个集合里
     */
    def mergeAll(community: Set[Set[VertexId]], idxStore: Array[Set[Int]]): Set[Set[VertexId]] = {
        val tmpSeq: Seq[mutable.Set[VertexId]] = community.toSeq  // 转换为可索引
        val res: Set[Set[VertexId]] = Set()  // 返回结果集合
        val lenS: Int = idxStore.length
        for (i <- 0 until lenS) {
            var tmpSet: Set[VertexId] = Set()
            // idx变量是此集合和索引的哪个集合并集
            idxStore(i).foreach(idx => tmpSet = tmpSet.union(tmpSeq(idx)))
            res.add(tmpSet)
        }
        res
    }

    /**
     * 创建图
     * @param sc spark环境
     * @return 返回创建后的图
     */
    def createGraph(sc: SparkContext): Graph[VertexState, Long] = {
        // 创建图
        val lines: RDD[String] = sc.textFile(config.inputDir)
        val edges: RDD[Edge[Long]] = lines.map(line => {  // 边的集合
            val items: Array[String] = line.split(",")
            Edge(items(0).toLong, items(1).toLong, 1L)
        })
        // 1393382
        val arr: immutable.IndexedSeq[Int] = for (i <- 1 to 20) yield i
        val tmpVer: RDD[Int] = sc.makeRDD(arr)
        val vertices: RDD[(VertexId, VertexState)] = tmpVer.map(item => {
            (item, new VertexState())
        })
        val graph: Graph[VertexState, Long] = Graph(vertices, edges)
        graph
    }

    def run(sc: SparkContext): Unit = {
        val graph: Graph[VertexState, Long] = createGraph(sc).cache()
        graph.vertices.collect().foreach(tuple => P.add(tuple._1))  // P集合最开始初始化为全部顶点
        // 收集每个节点的邻居节点，只考虑节点的出边
        val tmp_neighbors: Map[VertexId, mutable.Set[VertexId]] = graph.collectNeighborIds(EdgeDirection.Out).map({
            case (vid, nbs) => {
                var tmpSet: mutable.Set[VertexId] = Set()
                nbs.foreach(item => tmpSet.add(item))
                (vid, tmpSet)
            }
        }).collect().toMap
        bornKerbosch(R, P, X, community, tmp_neighbors)
//        community.foreach(println(_))
        val idxStore: Array[mutable.Set[Int]] = cpmMatrix(community)
        val res: mutable.Set[mutable.Set[VertexId]] = mergeAll(community, idxStore)
        res.foreach(println(_))
        // 过滤操作感觉可以不做
//        val newVid: mutable.Set[VertexId] = filterClique(community, 1)  // 将团大小为1的顶点过滤
        // 根据剩下的节点重构图
//        val curGraph: Graph[VertexState, VertexId] = graph.subgraph(vpred = (vid, vs) => newVid.contains(vid))
//        clearSet()  // 清空集合
        // 重新获取邻居节点
//        val neighbors: Map[VertexId, mutable.Set[VertexId]] = curGraph.collectNeighborIds(EdgeDirection.Out).map({
//            case (vid, nbs) => {
//                val tmpSet: mutable.Set[VertexId] = Set()
//                nbs.foreach(item => tmpSet.add(item))
//                (vid, tmpSet)
//            }
//        }).collect().toMap
        // 重新计算团
//        curGraph.vertices.collect().map(tuple => P.add(tuple._1))
//        bornKerbosch(R, P, X, community, neighbors)  // 重新得到极大团
        // 得到cpm矩阵
//        val idxStore: Array[mutable.Set[VertexId]] = cpmMatrix(community)
        // 进行合并
//        val resCommunity: mutable.Set[mutable.Set[VertexId]] = mergeAll(community, idxStore)
//        resCommunity.foreach(println(_))
    }

}
