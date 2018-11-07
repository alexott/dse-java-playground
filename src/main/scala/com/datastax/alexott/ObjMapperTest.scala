import com.datastax.driver.core.Cluster
import com.datastax.driver.mapping.MappingManager
import com.datastax.driver.mapping.annotations.{Column, PartitionKey, Table}

import scala.annotation.meta.field

// create table test.scala_test(id int primary key, t text, tm timestamp);
// insert into test.scala_test(id,t,tm) values (1,'t1','2018-11-07T00:00:00Z') ;


@Table(name = "scala_test")
class TObj {
  @PartitionKey
  var id: Integer = 0;
  var t: String = "";
  var tm: java.util.Date = new java.util.Date();

  def this(idval: Integer, tval: String, tmval: java.util.Date) = {
    this();
    this.id = idval;
    this.t = tval;
    this.tm = tmval;
  }

  override def toString: String = {
    "{id=" + id + ", t='" + t + "', tm='" + tm + "'}"
  }
}

@Table(name = "scala_test")
case class TObjC(@(PartitionKey @field) id: Integer, t: String, tm: java.util.Date) {
  def this() {
    this(0, "", new java.util.Date())
  }
}

// case class with renamed field
@Table(name = "scala_test")
case class TObjCR(@(PartitionKey @field) id: Integer, @(Column @field)(name = "t") text: String, tm: java.util.Date) {
  def this() {
    this(0, "", new java.util.Date())
  }
}


object ObjMapperTest {

  def main(args: Array[String]): Unit = {

    val cluster = Cluster.builder().addContactPoint("192.168.0.10").build();
    val session = cluster.connect()
    val manager = new MappingManager(session)

    val mapperClass = manager.mapper(classOf[TObj], "test")
    val objClass = mapperClass.get(new Integer(1))
    println("Obj(1)='" + objClass + "'")

    val mapperCaseClass = manager.mapper(classOf[TObjC], "test")
    val objCaseClass = mapperCaseClass.get(new Integer(1))
    println("Obj(1)='" + objCaseClass + "'")

    val mapperCaseClassRenamed = manager.mapper(classOf[TObjCR], "test")
    val objCaseClassRenamed = mapperCaseClassRenamed.get(new Integer(1))
    println("Obj(1)='" + objCaseClassRenamed + "'")

    mapperCaseClassRenamed.save(TObjCR(2, "test 2", new java.util.Date()))

    session.close()
    cluster.close()

  }

}