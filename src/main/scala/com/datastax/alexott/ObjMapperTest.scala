import com.datastax.driver.core.Cluster
import com.datastax.driver.mapping.{MappingManager, Result}
import com.datastax.driver.mapping.annotations.{Accessor, Column, Param, PartitionKey, Query, Table}
import com.datastax.spark.connector.cql.ClusteringColumn

import scala.annotation.meta.field
import scala.collection.JavaConverters

// create table test.scala_test(id int primary key, t text, tm timestamp);
// insert into test.scala_test(id,t,tm) values (1,'t1','2018-11-07T00:00:00Z') ;
// create table test.scala_test2(id int, c int, t text, tm timestamp, primary key (id, c));
// insert into test.scala_test2(id,c, t,tm) values (1,1,'t1','2018-11-07T00:00:00Z') ;
// insert into test.scala_test2(id,c, t,tm) values (1,2,'t2','2018-11-08T00:00:00Z') ;


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

@Table(name = "scala_test2", keyspace = "test")
case class TObjC2(@(PartitionKey @field) id: Integer, @(ClusteringColumn @field)(index = 0) c: java.lang.Integer,
                  t: String, tm: java.util.Date) {
  def this() {
    this(0, 0, "", new java.util.Date())
  }
}

@Accessor
trait ObjectAccessor {
  @Query("SELECT * from test.scala_test2 where id = :id")
  def getById(@Param id: java.lang.Integer): Result[TObjC2]
}


object ObjMapperTest {

  def main(args: Array[String]): Unit = {

    val cluster = Cluster.builder().addContactPoint("10.200.176.40").build();
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

    val accessor = manager.createAccessor(classOf[ObjectAccessor])
    val rs = accessor.getById(1)
    for (r <- JavaConverters.asScalaIteratorConverter(rs.iterator()).asScala) {
      println("r=" + r)
    }


    session.close()
    cluster.close()

  }

}