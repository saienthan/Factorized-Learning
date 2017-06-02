import org.apache.spark.rdd.PairRDDFunctions
import breeze.linalg._
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import scala.math.exp

val t = sc.parallelize(Seq(DenseVector(1.0, 2.0, 1.1, 2.2), DenseVector(4.0, 3.0, 3.3, 4.4), DenseVector(5.0, 6.0, 3.3, 4.4), DenseVector(8.0, 7.0, 1.1, 2.2), DenseVector(9.0, 1.0, 3.3, 4.4)))
val y = sc.parallelize(Seq(1.0, -1.0, 0.0, 1.0, 0.0))
var w = DenseVector(1.0, 1.0, 1.0, 1.0)
val alpha = 0.1


//morpheus
//inpmorph s /home/saienthan/FactLearning/data/s.txt 5x2
//inpmorph r /home/saienthan/FactLearning/data/r.txt 2x2
//var t = s.join(r)
//var matProd = y.zip(t.map(x => 1 + exp(x.dot(w)))).map{case (y, x) => y / x}
var matProd = y.zip(t.map(x => 1 + exp(x.dot(w)))).map(x => x._1 / x._2)
w = w + alpha * t.zip(matProd).map{case (a, b) => a * b} .reduce((accum, v) => accum + v)
//morpheus
