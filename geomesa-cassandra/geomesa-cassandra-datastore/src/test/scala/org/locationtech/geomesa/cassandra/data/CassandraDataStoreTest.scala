package org.locationtech.geomesa.cassandra.data

import java.io.File

import com.datastax.driver.core.Cluster
import com.vividsolutions.jts.geom.Coordinate
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataUtilities, Transaction, DataStore, DataStoreFinder}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime
import org.junit.{Assert, BeforeClass, Test}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

object CassandraDataStoreTest {
  @BeforeClass
  def startServer() = {
    val storagedir = File.createTempFile("cassandra","sd")
    storagedir.delete()
    storagedir.mkdir()

    System.setProperty("cassandra.storagedir", storagedir.getPath)

    EmbeddedCassandraServerHelper.startEmbeddedCassandra(30000L)
    val cluster = new Cluster.Builder().addContactPoints("127.0.0.1").withPort(9142).build()
    val session = cluster.connect()
    val cqlDataLoader = new CQLDataLoader(session)
    cqlDataLoader.load(new ClassPathCQLDataSet("init.cql", false, false))
  }

}

class CassandraDataStoreTest  {

  @Test
  def testDSAccess(): Unit = {
    val ds = getDataStore
    Assert.assertNotNull(ds)
  }


  @Test
  def testCreateSchema(): Unit = {
    val ds = getDataStore
    Assert.assertNotNull(ds)

    ds.createSchema(SimpleFeatureTypes.createType("test:test", "name:String,age:Int,*geom:Point:srid=4326,dtg:Date"))

    Assert.assertTrue("Type name is not in the list of typenames", ds.getTypeNames.contains("test"))
  }

  @Test
  def testWrite(): Unit = {
    import org.locationtech.geomesa.utils.geotools.Conversions._

    val ds = getDataStore
    val sft = SimpleFeatureTypes.createType("test:test", "name:String,age:Int,*geom:Point:srid=4326,dtg:Date")
    if(!ds.getTypeNames.contains("test")) {
      ds.createSchema(sft)
    }

    val gf = JTSFactoryFinder.getGeometryFactory

    val fs = ds.getFeatureSource("test").asInstanceOf[SimpleFeatureStore]
    fs.addFeatures(
      DataUtilities.collection(Array(
        SimpleFeatureBuilder.build(sft, Array("john",10,gf.createPoint(new Coordinate(-75.0, 35.0)), new DateTime("2016-01-01T00:00:00.000Z")).asInstanceOf[Array[AnyRef]], "1"),
        SimpleFeatureBuilder.build(sft, Array("jane",20,gf.createPoint(new Coordinate(-75.0, 38.0)), new DateTime("2016-01-07T00:00:00.000Z")).asInstanceOf[Array[AnyRef]], "1")
      ))
    )

    Assert.assertEquals("Unexpected count of features", fs.getFeatures.features().toList.length, 2)
  }

  def getDataStore: DataStore = {
    import scala.collection.JavaConversions._
    DataStoreFinder.getDataStore(
      Map(
        CassandraDataStoreParams.CONTACT_POINT.getName -> "127.0.0.1:9142",
        CassandraDataStoreParams.KEYSPACE.getName -> "geomesa_cassandra",
        CassandraDataStoreParams.NAMESPACEP.getName -> "http://geomesa.org"
      )
    )
  }

}
