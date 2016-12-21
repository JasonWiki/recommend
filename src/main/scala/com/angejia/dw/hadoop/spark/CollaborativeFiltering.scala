package com.angejia.dw.hadoop.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS,MatrixFactorizationModel,Rating}
import org.jblas.DoubleMatrix

/**
 * Spark Mllib 协同过滤
 */
class CollaborativeFiltering extends Serializable {

    // 训练得出的模型
    var trainModel: MatrixFactorizationModel = null


    /**
     * 提取有效特征
     * rdd: RDD[Array[Int]]   一个处理过的 RDD 对象, 结构为 RDD[Array[Int]] 
     * 分别是 用户,主题,评分
     */
    def characteristics(rdd: RDD[Array[Int]]): RDD[Rating] = {
        //把 RDD 数据转换成 Rating 对象
        val ratings: RDD[Rating] = rdd.map {
            // 模式匹配
            case Array(user, item, rating) => Rating(user.toInt, item.toInt, rating.toInt)
        }
        ratings.cache()
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

    /**
     * 显示矩阵分解
     */
    def train(ratings: RDD[Rating], rank: Int = 50, iterations: Int = 10, lambda: Double = 0.01) : MatrixFactorizationModel = {
        val model: MatrixFactorizationModel = ALS.train(ratings, rank, iterations, lambda)
        this.trainModel = model
        model
    }

    /**
     * 隐式矩阵分解
     */
    def trainImplicit(ratings: RDD[Rating], rank: Int = 50, iterations: Int = 10) : MatrixFactorizationModel = {
        val model: MatrixFactorizationModel = ALS.trainImplicit(ratings, rank, iterations)
        this.trainModel = model
        model 
    }


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
     * 用户推荐 - 单个用户推荐最得分最高的 K 个物品
     * userId: 需要推荐的用户 Id
     * K: 匹配分数最高的前 K 个物品
     */
    def userRecommendItem(userId: Int, K: Int) : Array[Rating] = {
        val topKRecs: Array[Rating] = this.trainModel.recommendProducts(userId, K)
        topKRecs
    }


    /**
     * 用户推荐 - 用户物品推荐预测得分
     */
    def userPredict(user: Int, product: Int): Double = {
        val predictionScore = this.trainModel.predict(user, product)
        predictionScore
    }


    /**
     * 用户推荐 - 批量用户推荐物品推荐得分
     */
    def userPredict(usersProducts: RDD[(Int,Int)]): RDD[Rating] = {
        val predictionScore = this.trainModel.predict(usersProducts)
        predictionScore
    }


    /**
     * 物品余弦相似度计算
     * 返回 (item ID, 因子分数) 这是一个 pair RDD
     */
    def itemCosineSimilarity(itemId: Int) : RDD[(Int, Double)] = {
         // 线性代数库,求向量点积 ,创建一个 Array[Double] 类型的向量

        // item 因子 从模型中,取回对应的因子
        val itemFactor: Array[Double] = this.trainModel.productFeatures.lookup(itemId).head

        // item 向量
        val itemVector: DoubleMatrix = new org.jblas.DoubleMatrix(itemFactor)

        // 求出本物品与各个物品的余弦相似度
        val sims: RDD[(Int, Double)] = this.trainModel.productFeatures.map { case (id, factor) => 
            val factorVector = new org.jblas.DoubleMatrix(factor)
            val sim = this.cosineSimilarity(factorVector,itemVector)
            (id, sim)
        }
        sims
    }


    /**
     * 物品推荐 - top 推荐
     */
    def itemRecommendItem(sims: RDD[(Int, Double)], K: Int) : Array[(Int, Double)] = {
         // 按照物品相似度排序,取出与本物品最相似前 K 个物品
        val sortedSims: Array[(Int, Double)] = sims.top(K)(    // top 是分布式计算出前 K 个结果
            Ordering.by[(Int, Double), Double] {
                case (id, similarity) => similarity 
            }
        )
 
        // 打印出这 10 个与给定物品最相似的物品
        //val result = sortedSims.take(10).mkString("\n")
        //println(result)
        sortedSims
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