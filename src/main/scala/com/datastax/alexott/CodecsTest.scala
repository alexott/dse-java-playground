package com.datastax.alexott

import com.datastax.driver.core.{Cluster, TypeCodec}
import com.datastax.driver.extras.codecs.jdk8.OptionalCodec
import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._


object CodecsTest {
  def main(args: Array[String]): Unit = {

    val cluster = Cluster.builder().addContactPoint("10.200.176.39").build()
    val intCodec =  TypeCodec.cint()
    val optionalIntCodec = new OptionalCodec[java.lang.Integer](intCodec)
    cluster.getConfiguration.getCodecRegistry.register(optionalIntCodec)
    val javaIntType = optionalIntCodec.getJavaType()
    val session = cluster.connect()

    for (row <- session.execute("select id, c1, v1 from test.st1 where id = 2").all().asScala) {
      println("id=" + row.get("id", javaIntType).asScala
        + ", c1=" + row.get("c1", javaIntType).asScala
        + ", v1=" + row.get("v1", javaIntType).asScala)
    }
    session.close()
    cluster.close()

  }
}
