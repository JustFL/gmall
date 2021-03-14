package disaster

object TestParentheses {
  def main(args: Array[String]): Unit = {

    val add = (x:Int, y:Int) => x + y
    println(add(1,2))

    val add1 = (x:Int) => x + 1
    println(add1{1})
  }
}
