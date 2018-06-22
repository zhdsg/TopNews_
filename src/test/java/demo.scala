import java.util

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

/**
 * Created by Administrator on 2018/6/12/012.
 */
object demo {


  /**
   * 题目一
   * @param array 数组
   * @param N 元素个数
   * @return
   */
  def breakchain(array : Array[Int] ,N : Int) : Int={
    var i_max = array.length
    if(array.length > N){
      i_max =N
    }
    var min =Int.MaxValue
    for( i <- 0  to   i_max -4 ){
      for( j <- i+2 to  i_max - 2 ){
         min  = Math.min(array(i)+array(j),min)
      }
    }
    min
  }

  /**
   * 题目二
   * @param array 无序数组
   * @param M 前M个
   * @return
   */
  def getMaxNum(array:Array[Int],M :Int) : Array[Int] ={

    array.sortBy(f=> f).takeRight(M)

  }



  /**
   * 题目四
   * @param array
   * @return
   */
  def printArray(array :Array[Array[Int]] ) : Array[Int]={
    val array_return =new ArrayBuffer[Int]()
    var x = -1
    var y =0
    var Right =true
    var Down =false
    var up =false
    var left =false
    var flag =""
    var temp_x =array(0).length
    var temp_y=array.length-1
    var tt =0
    while( tt  < array(0).length * array.length ){

      if(Right){

        for( i <- 1 to temp_x ){
          x +=1
          array_return += array(y)(x)
          tt += 1
        }
        flag ="d"
        temp_x -=1
        Right=false
      }else if(Down){

        for( i <- 1 to temp_y ){
          y += 1
          array_return += array(y)(x)
          tt += 1
        }

        flag ="l"
        temp_y -=1
        Down=false
      }else if(left){
        for( i <- 1 to temp_x ){
          x -= 1
          array_return += array(y)(x)
          tt += 1
        }
        flag ="u"
        temp_x -=1
        left=false
      }else if(up){
        for( i <- 1 to temp_y ){
          y -= 1
          array_return += array(y)(x)
          tt += 1
        }
        flag ="r"
        temp_y -=1
        up=false
      }
    flag match {
      case "r" => Right=true
      case "l" => left=true
      case "u" => up=true
      case "d" => Down =true
    }

    }
    array_return.toArray
  }

  /**
   * 题目五
   * 最坏60次
   * 两部手机 一开始先到60层 （摔碎记作A，未摔碎记作B）
   * 如果是A了
   * 那就从1层开始 ，如果没A 就去60-120的中间层 90层 依次类推
   * 如果最终是59层的话，需要尝试60次
   */


  def main(args: Array[String]) {
    //测试一
    val array = Array(5,2,4,6,3,7)


   println( breakchain(array ,6))
    //测试二
    getMaxNum(array,3).foreach(  print(_))

    //
     val array1 =Array(
    Array(1,2,3,4),
    Array(5,6,7,8),
    Array(12,11,10,9),
    Array(113,14,15,16)

    )
    printArray(array1).foreach(f=> print(f + ","))
  }

}
