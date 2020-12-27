import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, Statement}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

/**
  * Created by jxy on 2020/12/25 0025 14:11
  */
object Test {

  import java.sql.Connection
  import java.sql.DriverManager
  import java.sql.PreparedStatement
  import java.sql.ResultSet
  import java.sql.SQLException

  @throws[ClassNotFoundException]
  @throws[SQLException]
  def main(args: Array[String]): Unit = { //1.定义参数
    val driver = "org.apache.phoenix.jdbc.PhoenixDriver"
    val url = "jdbc:phoenix:Master:2181"
    //2.加载驱动
    Class.forName(driver)
    //3.创建连接
    val connection = DriverManager.getConnection(url)
    //4.预编译SQL
    val preparedStatement = connection.prepareStatement("SELECT * FROM student")
    //5.查询获取返回值
    val resultSet = preparedStatement.executeQuery
    //6.打印结果
    while ( {
      resultSet.next
    }) System.out.println(resultSet.getString(1) + resultSet.getString(2))
    //7.关闭资源
    resultSet.close()
    preparedStatement.close()
    connection.close()
  }
}
