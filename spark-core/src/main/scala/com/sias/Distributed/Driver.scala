package com.sias.Distributed

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

/**
 * @author Edgar
 * @create 2022-11-02 15:14
 * @faction:
 */
object Driver {
  def main(args: Array[String]): Unit = {
    /*1.客户端，去连接服务器*/
    val client1 = new Socket("localhost", 9999)
    val client2 = new Socket("localhost", 8888)
    /*2.创建一个输出流
    *   为什么是输出流的方式呢，首先这个是往另外一个程序中去写数据
    *   这会程序是在内存中先执行，然后在把数据发送到另外一个地方，
    *   主要的就是内存，内存往外面发送数据，就是OutPut的方式*/
    val out: OutputStream = client1.getOutputStream
    val objOut1 = new ObjectOutputStream(out)

    val task1 = new task1()
    val SubTask = new SubTask
    SubTask.logic=task1.logic
    /*01.取前面两个数据，*/
    SubTask.data=task1.data.take(2)


    objOut1.writeObject(SubTask)
    objOut1.flush()
    objOut1.close()
    client1.close()
    println("客户端数据发送好完毕")


    val out2: OutputStream = client2.getOutputStream
    val objOut2 = new ObjectOutputStream(out2)

    val task2 = new task1()
    val SubTask2 = new SubTask
    SubTask2.logic=task2.logic
    /*02.取最后两个数据*/
    SubTask2.data=task2.data.takeRight(2)


    objOut2.writeObject(SubTask2)
    objOut2.flush()
    objOut2.close()
    client2.close()
    println("客户端数据发送好完毕")
  }
}
