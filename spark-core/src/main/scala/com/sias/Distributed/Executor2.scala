package com.sias.Distributed

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

/**
 * @author Edgar
 * @create 2022-11-02 15:15
 * @faction:
 */
object Executor2 {
  def main(args: Array[String]): Unit = {

    /*1.先创建一个服务器*/
    val Server = new ServerSocket(8888)
    println("等待着，客户端向服务端发送数据")

    /*2.等待着客户端的连接
    *   客户端连接过来之后，就是这个Socket
    *   服务器Server，利用这个accept方法，去接受客户端
    *   接受的客户端就是Socket，然后客户端进行消息的输出*/
    val client: Socket = Server.accept()
    val in: InputStream = client.getInputStream
    val objIn = new ObjectInputStream(in)
    val task: SubTask = objIn.readObject().asInstanceOf[SubTask]
    val ints: List[Int] = task.compute()
    println("计算节点计算的结果为：" + ints)
    objIn.close()
    client.close()
    Server.close()
  }
}
