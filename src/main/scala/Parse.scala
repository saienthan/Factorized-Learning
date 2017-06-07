import org.apache.spark.rdd.PairRDDFunctions
import breeze.linalg._
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import scala.collection.mutable.Map
import scala.util.matching.Regex
import scala.io.Source
import util.control.Breaks._
import scala.collection.mutable.ListBuffer

object Parse{

  //abstract class Op
  trait Op extends Product with Serializable
  case object Plus extends Op
  case object Minus extends Op
  case object Mult extends Op
  case object Dot extends Op
  case object Div extends Op
  case object Zip extends Op
  case object Slice extends Op
  case object Proj extends Op
  case object Concat extends Op
  case object Map extends Op
  case object MapValues extends Op
  case object Join extends Op
  case object ReduceByKey extends Op
  case object Reduce extends Op
  case object Asn extends Op
  case object AsnVar extends Op
  case object AsnVal extends Op

  case object Exp extends Op


  abstract class Expr 
  case class Blnk() extends Expr
  case class Lit(n:Int) extends Expr 
  case class Var(x:String) extends Expr 
  case class Bin(e1:Expr, e2:Expr, o:Op) extends Expr 
  case class Trip(e1:Expr, e2:Expr, e3:Expr, o:Op) extends Expr 
  case class Un(e:Expr, o:Op) extends Expr 
  case class Lam(xs:List[String], e: Expr) extends Expr 
  case class App(e:Expr, m:String, args: List[Expr]) extends Expr

  def LMM(e1: Expr, e2: Expr): Expr = {
    //Bin ( Lam ( List("x"), Bin ( Bin ( Var("x"), Lit(1), Proj), Bin ( Var("x"), Lit(2), Proj), Mult )), Bin ( e1, e2, Zip), Map )
    Bin(e1, Lam(List("x"), Bin(Var("x"), e2, Dot)),MapValues)
  }


  def RMM(e1: Expr, e2: Expr): Expr = {
    //Bin ( Lam ( List("x"), Bin ( Bin ( Var("x"), Lit(1), Proj), Bin ( Var("x"), Lit(2), Proj), Mult )), Bin ( e1, e2, Zip), Map )
    Bin(Lam(List("acc", "v"), Bin(Var("acc"), Var("v"), Plus)), Bin(Lam(List("x"), Bin(P_d2("x", 1, 2), P_d2("x", 2, 2), Dot)), Bin(e1, e2, Zip), Map), Reduce)

  }

  def P_d2(x: String, i1: Int, i2: Int): Expr = {
    Bin(Bin(Var(x), Lit(i1), Proj), Lit(i2), Proj)
  }

  def transform(e: Expr): Expr = e match {
    case Bin(Bin(Bin(a,Bin(b,c, Join),Zip),Bin(Bin(Lam(List("x"),Bin(Bin(Bin(Var("x"),Lit(2),Proj),Lit(1),Proj),Var("x"),Mult)),Lit(2),Proj),Lit(2),Proj),Map),Lam(List("acc","v"),Bin(Var("acc"),Var("v"),Plus)),Reduce) =>
      transform(Bin(Bin(Bin(App(Blnk(), "customZip", List(a, b)), Lam(List("x"), Bin(P_d2("x", 2, 2),Bin(Var("x"), Lit(1), Proj), Mult)), Map), Lam(List("acc", "v"), Bin(Var("acc"), Var("v"), Plus)), Reduce),Bin(Bin(Bin(Bin(App(Blnk(), "customZip", List(Bin(b, Lam(List("x"), Bin(Var("x"), Lit(1), Proj)), Map),a)), Lam(List("acc", "v"), Bin(Var("acc"), Var("v"), Plus)), ReduceByKey), c, Join), Lam(List("x"), Bin(P_d2("x", 2, 2),P_d2("x", 2, 1), Mult)), Map), Lam(List("acc", "v"), Bin(Var("acc"), Var("v"), Plus)), Reduce) , Concat))
    case Bin(Bin(App(Blnk(), "customZip", List(a,Bin(b,c, Join))),Lam(List("x"), Bin(Bin(Var("x"),Lit(1),Proj),Bin(Var("x"),Lit(2),Proj),Mult)),Map),Lam(List("acc","v"),Bin(Var("acc"),Var("v"),Plus)),Reduce) =>
    //case Bin(Bin(App(Blnk(), "customZip", List(a,Bin(b,c, Join))),Bin(Bin(Lam(List("x"),Bin(Bin(Bin(Var("x"),Lit(2),Proj),Lit(1),Proj),Var("x"),Mult)),Lit(2),Proj),Lit(2),Proj),Map),Lam(List("acc","v"),Bin(Var("acc"),Var("v"),Plus)),Reduce) =>
      transform(Bin(Bin(Bin(App(Blnk(), "customZip", List(a, b)), Lam(List("x"), Bin(P_d2("x", 2, 2),Bin(Var("x"), Lit(1), Proj), Mult)), Map), Lam(List("acc", "v"), Bin(Var("acc"), Var("v"), Plus)), Reduce),Bin(Bin(Bin(Bin(App(Blnk(), "customZip", List(Bin(b, Lam(List("x"), Bin(Var("x"), Lit(1), Proj)), Map),a)), Lam(List("acc", "v"), Bin(Var("acc"), Var("v"), Plus)), ReduceByKey), c, Join), Lam(List("x"), Bin(P_d2("x", 2, 2),P_d2("x", 2, 1), Mult)), Map), Lam(List("acc", "v"), Bin(Var("acc"), Var("v"), Plus)), Reduce) , Concat))

    case Bin(Bin(a, b, Join), Lam(List("x"), Bin(Var("x"), c, Dot)), Map) => transform(Bin(Bin(Bin(a, Lam(List("x"), Bin(Var("x"), Trip(c, Lit(0), Var(toStr(a)+"d"), Slice), Dot)),MapValues), Bin(b, Lam(List("x"), Bin(Var("x"), Trip(c, Var(toStr(a)+"d"), Bin(Var(toStr(a)+"d"), Var(toStr(b)+"d"), Plus), Slice), Dot)),MapValues), Join) ,Lam(List("x"), Bin(Bin(Var("x"), Lit(1), Proj), Bin(Var("x"),Lit(2), Proj), Plus)),  MapValues))//LMM match
    //case Bin(Bin(a, b, Join), Lam(List("x"), Bin(Bin(Var("x"), Lit(2), Proj), c, Dot)), Map) => transform(Bin(Bin(Bin(a, Lam(List("x"), Bin(Var("x"), Trip(c, Lit(0), Var(toStr(a)+"d"), Slice), Dot)),MapValues), Bin(b, Lam(List("x"), Bin(Var("x"), Trip(c, Var(toStr(a)+"d"), Bin(Var(toStr(a)+"d"), Var(toStr(b)+"d"), Plus), Slice), Dot)),MapValues), Join) ,Lam(List("x"), Bin(Bin(Var("x"), Lit(1), Proj), Bin(Var("x"),Lit(2), Proj), Plus)),  MapValues))//LMM match

    case Bin(e1, e2, o) => Bin(transform(e1), transform(e2), o)
    case Un(e, o) => Un(transform(e), o)
    case Lam(xs, e) => Lam(xs, transform(e))
    case App(e, m, args) => App(transform(e), m, args.map(x=>transform(x)))
    case _ => e
  }

  def toStr(e: Expr):String = e match {
    case Blnk() => ""
    case Lit(n) => n.toString
    case Var(x) =>  x
    case Bin(e1, e2, o) => o match {
      case Map|Join|Zip|ReduceByKey|Reduce|MapValues|Dot => toStr(e1) + opToStr(o) + toStr(e2) + ")"
      case Concat => "DenseVector(" + toStr(e1) + ".toArray" + "++" + toStr(e2) + ".toArray)"
      case Plus|Minus|Mult|Div|Proj|Asn => toStr(e1) + opToStr(o) + toStr(e2)
      case AsnVar => "var " + toStr(e1) + opToStr(o) + toStr(e2)
      case AsnVal => "val " + toStr(e1) + opToStr(o) + toStr(e2)
    }
    case Un(e, o) => o match {
      case Exp => "exp(" + toStr(e) + ")"
    }
    case Trip(e1, e2, e3, o) => o match {
      case Slice => toStr(e1) + ".slice(" + toStr(e2) + "," + toStr(e3) + ")"
    }
        case Lam(xs, e) =>  
          val tempString = xs.mkString(",")
          if(tempString.startsWith("(")){
            val temp = xs.mkString(",") + "=>" + toStr(e)
            return temp
          }
          else{
            val temp = "(" + xs.mkString(",") + ")" + "=>" + toStr(e)
            return temp
          }
        case App(e, m, args) => 
          if(e == Blnk())
             m + "(" + args.map(x=>toStr(x)).mkString(",") + ")"
          else
             toStr(e) + "." + m + "(" + args.map(x=>toStr(x)).mkString(",") + ")"

  }

  def toStrExpand(e: Expr, i: Int):String = e match {
    case Lit(n) => n.toString
    case Var(x) =>  x
    case Bin(e1, e2, o) => o match {
      case Map|Join|Zip|ReduceByKey|Reduce|MapValues =>  
        val temp = toStrExpand(e1, i*2) + opToStr(o) + toStrExpand(e2, i*2+1) + ")" 
        println("val t"+i+" = "+ temp)
        return "t"+i
      case Dot =>  
        val temp = toStrExpand(e1, i*2) + opToStr(o) + toStrExpand(e2, i*2+1) + ")" 
        return temp
      case Concat => "DenseVector(" + toStrExpand(e1, i*2) + ".toArray" + "++" + toStrExpand(e2, i*2 + 1) + ".toArray)"
      case Plus|Minus|Mult|Div|Proj =>  
        val temp = toStrExpand(e1, i*2) + opToStr(o) + toStrExpand(e2, i*2+1)
        return temp 
      case Asn =>
        toStr(e1) + opToStr(o) + toStrExpand(e2, i*2+1)
      case AsnVal =>
        "val " + toStr(e1) + opToStr(o) + toStrExpand(e2, i*2+1)
      case AsnVar =>
        "var " + toStr(e1) + opToStr(o) + toStrExpand(e2, i*2+1)
      }   
    case Trip(e1, e2, e3, o) => o match {
      case Slice => toStr(e1) + ".slice(" + toStr(e2) + "," + toStr(e3) + ")"
    }
    case Un(e, o) => o match {
        case Exp =>  
          val temp = opToStr(o) + toStrExpand(e, i+1) + ")" 
          return temp
      }   
      case Lam(xs, e) =>  
          val tempString = xs.mkString(",")
          if(tempString.startsWith("(")){
            val temp = xs.mkString(",") + "=>" + toStrExpand(e, i+1)
            return temp
      }
          else{
            val temp = "(" + xs.mkString(",") + ")" + "=>" + toStrExpand(e, i+1)
            return temp
      }
      case App(e, m, args) => 
        if(e == Blnk())
           m + "(" + args.map(x=>toStr(x)).mkString(",") + ")"
        else
           toStr(e) + "." + m + "(" + args.map(x=>toStr(x)).mkString(",") + ")"
  }

  var opToStr = scala.collection.mutable.Map(
    Exp -> "exp(",
    ReduceByKey -> ".reduceByKey(",
    Reduce -> ".reduce(",
    Join -> ".join(",
    Dot -> ".dot(",
    Concat -> ".concat(",
    Proj -> "._",
    Zip -> ".zip(",
    Map -> ".map(",
    MapValues -> ".mapValues(",
    Plus -> "+",
    Minus -> "-",
    Mult -> "*",
    Div -> "/",
    Asn -> "=",
    AsnVar -> "=",
    AsnVal -> "="
    )
  var strToOp = scala.collection.mutable.Map(
    "exp" -> Exp,
    "join" -> Join,
    "dot" -> Dot,
    "map" -> Map,
    "mapValues" -> MapValues,
    "zip" -> Zip,
    "reduce" -> Reduce,
    "reduceByKey" -> ReduceByKey,
    "concat" -> Concat,
    "+" -> Plus,
    "-" -> Minus,
    "*" -> Mult,
    "/" -> Div
  )

def parse(input: String):Expr = {
  val eq = """^(var\s+|val\s+){0,1}(\w+)\s*=[^>]\s*(.*)""".r
  val met = """([\w.\d]+)\.(\w+)\((.*)\)""".r
  val func = """(\w+)\((.*)\)""".r
  val lam = """\(?(.*?)\)?\s*=>\s*(.*)""".r
  val lit = """(\d+)""".r
  val varn = """([a-zA-Z0-9_]+)""".r
  val bas = """(.*?)\s*(\+|-|\*|\/)\s*(.*)""".r
  val proj = """(.+)\._(\d+)""".r
  val un = """exp\((.*)\)""".r

  input match {
    case eq(varn, lhs, rhs) => // = 
      if(varn.startsWith("var "))
        Bin(parse(lhs), parse(rhs), AsnVar) 
      else if(varn.startsWith("val "))
        Bin(parse(lhs), parse(rhs), AsnVal) 
      else 
        Bin(parse(lhs), parse(rhs), Asn)
    case lit(num) =>
      Lit(num.toInt)
    case varn(name) =>
      Var(name)
    case met(e1, fname, e2) =>
      if(strToOp.contains(fname)){
        //println(e1, e2, strToOp(fname))
          Bin(parse(e1), parse(e2), strToOp(fname))
      }
      else{
        App(parse(e1), fname, e2.split(",").map(x => parse(x.trim)).toList)
      }
    case func(fname, e2) => App(Blnk(), fname, e2.split(",").map(x => parse(x.trim)).toList)
    case lam(vars, e2) => 
      Lam(vars.split(",").map(_.trim).toList, parse(e2))
    case bas(e1, op, e2) =>
      Bin(parse(e1), parse(e2), strToOp(op))
    case proj(varn, lit) =>
      Bin(parse(varn), parse(lit), Proj)
    case un(e1) =>
      Un(parse(e1), Exp)
    case (_) =>
      println("Unmatched ", input)
      Var("s")
  }


}

def mergeExprs(e1: Expr, e2: Expr):Expr = {
  def merge(lhs: Expr, rhs: Expr, orig: Expr):Expr = {
    orig match {
      case Var(x) =>  
        if(orig == lhs) {
          rhs
        }
        else {
          orig
        }
      case Bin(e1, e2, o) => Bin(merge(lhs, rhs, e1), merge(lhs, rhs, e2), o)
      case Un(e, o) => Un(merge(lhs, rhs, e), o)
      case Lam(xs, e) => Lam(xs, merge(lhs, rhs, e))
      case App(e, m, args) => App(merge(lhs, rhs, e), m, args.map(x=> merge(lhs, rhs, x)))
      case (_) => 
        orig
    }
  }
  e1 match {
    case Bin(lhs, rhs, op) => 
      merge(lhs, rhs, e2)
  }
}
def prepare[T: ClassTag](rdd: RDD[T], n: Int) =
      rdd.zipWithIndex.sortBy(_._2, true, n).keys

def customZip[T: ClassTag, U: ClassTag](rdd1: RDD[T], rdd2: RDD[U]) = {
      val n = rdd1.partitions.size + rdd2.partitions.size
            prepare(rdd1, n).zip(prepare(rdd2, n))
}


def main(args: Array[String]) {


  var startParsing = false

  val filename = "/home/saienthan/Factorized-Learning/data/temp.scala"
  val matchinp = """^(//){0,1}(.*)""".r
    var Exprs = new ListBuffer[Expr]()
  try {
    for (line <- Source.fromFile(filename).getLines()) {
      breakable {
        if(line == "//morpheus"){
          startParsing = !startParsing
        break
        }
        if(startParsing == true){
          val matchinp(prefix, suffix) = line
          (prefix, suffix) match {
            case(null, s) => Exprs += parse(s)
            case(p, s)  => 
              if(s.startsWith("inpmorph")){
                val name :: path :: dims = s.stripPrefix("inpmorph ").split(" ").toList
                println("val "+ name + " = sc.textFile(\"" + path +"\").map(line=>line.split(\" \")).map(x=>(x(0).toInt, DenseVector(x.drop(1).map(y=>y.toDouble))))")
                println("val "+ name +"n = " + dims(0).split("x")(0))
                println("val "+ name +"d = " + dims(0).split("x")(1))
              }
              else {
                Exprs += parse(s)
              }
          }
        }
        else{
          println(line)
        }
      }
    }
  } catch {
    case ex: Exception => println(ex)
  }
  //Exprs.foreach(println)
  val mergedExpr = Exprs.reduceRight(mergeExprs)
  val expr = mergedExpr
  //println(toStr(transform(expr)))
  //val expr = App(Blnk(), "customZip", List(Var("y"), Var("s"))) 
  //Bin(Var(t3),Bin(Lam(List(a),Bin(Bin(Var(a),Lit(1),Proj),Var(a),Div)),Lit(2),Proj),Map)
  //println(parse("a._1/a._2"))
  println(toStrExpand(transform(expr), 1))
}
}
