package com.angejia.dw.hadoop.spark


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.jblas.DoubleMatrix



/**
 * 测试数据 /data/log/recommend/ml-100k/u.data
 */
object CollaborativeFilteringTest {

    def main(args: Array[String])  {
        val inventoryIBCF = new CollaborativeFilteringTest()
        inventoryIBCF.run()
    }
}

/**
 * 看了又看算法
 */
class CollaborativeFilteringTest {
  
    
    def run(): Unit = {
        this.suanfa()
    }


    def suanfa(): Unit = {
        // SPARK 运行环境配置
        val conf = new SparkConf()
        conf.setAppName("InventoryIBCF")
        conf.setMaster("local[2]")
        //conf.set("spark.ui.port", "36000")

        // SPARK 上下文配置
        val sc = new SparkContext(conf)

 
        /**
         * 提取有效特征
         * 1. 数据清洗
         * 2. 载入数据
         * 3. 格式化数据
         */
        //原始数据
        val rawData = sc.textFile("/data/log/recommend/ml-100k/u.data")
        //println(rawData.first())

        // 把行分割成数组，并且读取数组前 3 个原始
        // 参数(类型)推断
        val rawRatings = rawData.map(_.split("\t").take(3))
        // 正常写法
        //val rawRatings = rawData.map(line => line.split("\t").take(3))     

        //把数据转换成 Rating 对象
        val ratings = rawRatings.map {
            // 模式匹配
            case Array(user, movie, rating) => Rating(user.toInt, movie.toInt, rating.toInt)
        }


        /**
         * 训练推荐模型 
         * 使用: (ALS)最小二乘法,是求解矩阵分解问题的最优方法
         * 矩阵分解: 
         *   显示矩阵分解: ALS.train(ratings, rank, iterations, lambda) 用来处理直接获得的数据,一般是用户访问,收藏,评分等数据 
         *   隐式矩阵分解: ALS.trainImplicit(ratings, rank, iterations) 用处理间接才能获得的数据,需要在用户与物品的交互中才能得到的数据,如看了电影的次数,购买了某个产品等
         * 
         * 矩阵分解参数:
         *   rank : ALS 模型中的因子个数,值越大会越好,但是训练模型和保存时的开销就越大
         *   iterations : 运行迭代次数,经过少数次数迭代后 ALS 模型便已能收敛为一个比较合理的好模型
         *   lambda : 控制模型的正规化过程, 从而控制模型的过拟合情况
         */
        var rank = 50
        var iterations = 10
        var lambda = 0.01

        // 训练模型,返回 MatrixFactorizationModel 对象,返回 用户因子 RDD 和 物品因子 RDD
        val model = ALS.train(ratings, rank, iterations, lambda)

        //val productFeatures = model.productFeatures    // 物品因子
        //val userFeatures = model.userFeatures          // 用户因子
        val K = 10    // 推荐数量


        /**
         * 使用推荐模型
         * 
         * 用户推荐模型: 利用相似用户的评级来计算对某个用户的推荐
         *   给指定用户推荐物品,通常以 "前 K 个" 形式展现, 即通过模型求出用户可能喜好程度最高的前 K 个商品
         *   这个过程通过计算每个商品的预计得分, 按照得分机型排序实现
         *   
         * 物品推荐模型: 依赖用户接触过的物品与候选物品之间的相似度来获得推荐
         *   给定一个物品, 有哪些物品与它相似,相似的确切定义取决于所使用的模型,相似度是通过某种方式比较表示两个物品的向量二得到的
         *   相似度衡量方法
         *     皮尔森相关系数(Pearson correlation)
         *     针对实数响亮的余弦相似度(cosine similarity)
         *     针对二元向量的杰卡德相似系数(Jaccard similarity)
         */

        /**
         * 用户推荐
         */
        // 计算给定用户 -> 给定物品的预计得分
        model.predict(789, 123)

        // 以(user,item) ID对类型的 RDD 对象为输入, 返回多个用户和物品的预测, 
        //model.predict(userFeatures)

        // 为每个用户生成前 K 个推荐物品
        val userId = 789
        
        val topKRecs = model.recommendProducts(userId, K)
         //println(topKRecs.mkString("\n"))

        /**
         * 物品推荐
         */
        // 线性代数库,求向量点积 ,创建一个 Array[Double] 类型的向量
        val aMatrix = new DoubleMatrix(Array(1.0,2.0,3.0))

       
        var itemId = 567
        val itemFactor = model.productFeatures.lookup(itemId).head
        val itemVector = new DoubleMatrix(itemFactor)
        
         // 计算物品与自己的相似度 - Test
        //val itemX = this.cosineSimilarity(itemVector, itemVector)
        //println(itemX)


        // 求出物品与各个物品的余弦相似度
        val sims = model.productFeatures.map { case (id, factor) => 
            val factorVector = new DoubleMatrix(factor)
            val sim = cosineSimilarity(factorVector,itemVector)
            (id, sim)
        }


        // 按照物品相似度排序,取出与物品 567 最相似前 10 个物品
        val sortedSims = sims.top(K)(
            Ordering.by[(Int, Double), Double] {
                case (id, similarity) => similarity 
            }
        )

        // 打印出这 10 个与给定物品最相似的物品
        val result = sortedSims.take(10).mkString("\n")
        println(result)
    }

    
    /**
     * 计算连个向量之间的余弦相似度, 余弦相似度是两个向量在 n 维空间里两者夹角的读书
     * 它是两个向量的点积与各向量范数(或长度)的乘积的商
     * 相似度的取值在 -1 到 1 主键
     *  1 表示完全相似
     *  0 表示两者互不相关(即无相关性)
     *  -1 表示两者不相关, 还表示它们完全不相同
     */
    def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix) : Double = {
        vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
    }
}