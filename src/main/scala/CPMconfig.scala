import org.apache.spark.SparkContext

/**
 * 配置类
 * @param inputDir 输入数据的路径
 * @param outputDir 输出数据的路径
 * @param kVal k派系值
 */
case class CPMconfig(inputDir: String, outputDir: String, kVal: Int)
