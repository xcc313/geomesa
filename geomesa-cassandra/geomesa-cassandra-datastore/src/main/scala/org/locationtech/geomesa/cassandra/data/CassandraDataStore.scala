package org.locationtech.geomesa.cassandra.data

import java.io.Serializable
import java.math.BigInteger
import java.net.URI
import java.nio.ByteBuffer
import java.util
import java.util.{Date, UUID}

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, DefaultRetryPolicy, TokenAwarePolicy}
import com.google.common.collect.HashBiMap
import com.vividsolutions.jts.geom.{Envelope, Geometry, Point}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.simple.DelegateSimpleFeatureReader
import org.geotools.data.store._
import org.geotools.data.{FeatureWriter => FW, _}
import org.geotools.feature.collection.DelegateSimpleFeatureIterator
import org.geotools.feature.simple.{SimpleFeatureBuilder, SimpleFeatureTypeBuilder}
import org.geotools.feature.{AttributeTypeBuilder, NameImpl}
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.util.KVP
import org.joda.time.{DateTime, Seconds, Weeks}
import org.locationtech.geomesa.cassandra.data.CassandraDataStore.FieldSerializer
import org.locationtech.geomesa.convert.SimpleFeatureConverter
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType
import org.locationtech.geomesa.utils.text.{ObjectPoolFactory, WKBUtils}
import org.locationtech.sfcurve.zorder.ZCurve2D
import org.opengis.feature.`type`.{AttributeDescriptor, Name}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

object CassandraDataStore {
  import scala.collection.JavaConversions._

  val typeMap = HashBiMap.create[Class[_], DataType]
  typeMap.putAll(Map(
    classOf[Integer]    -> DataType.cint(),
    classOf[Long]       -> DataType.bigint(),
    classOf[Float]      -> DataType.cfloat(),
    classOf[Double]     -> DataType.cdouble(),
    classOf[Boolean]    -> DataType.cboolean(),
    classOf[BigDecimal] -> DataType.decimal(),
    classOf[BigInteger] -> DataType.varint(),
    classOf[String]     -> DataType.text(),
    classOf[Date]       -> DataType.timestamp(),
    classOf[UUID]       -> DataType.uuid(),
    classOf[Point]      -> DataType.blob()
  ))

  def getSchema(name: Name, table: TableMetadata): SimpleFeatureType = {
    val cols = table.getColumns.filterNot { c => c.getName == "pkz" || c.getName == "z31" || c.getName == "fid" }
    val attrTypeBuilder = new AttributeTypeBuilder()
    val attributes = cols.map { c =>
      val it = typeMap.inverse().get(c.getType)
      attrTypeBuilder.binding(it).buildDescriptor(c.getName)
    }
    // TODO: allow user data to set dtg field
    val dtgAttribute = attributes.find(_.getType.getBinding.isAssignableFrom(classOf[java.util.Date])).head
    val sftBuilder = new SimpleFeatureTypeBuilder()
    sftBuilder.addAll(attributes)
    sftBuilder.setName(name.getLocalPart)
    val sft = sftBuilder.buildFeatureType()
    sft.getUserData.put(RichSimpleFeatureType.DEFAULT_DATE_KEY, dtgAttribute.getLocalName)
    sft
  }

  sealed trait FieldSerializer {
    def serialize(o: java.lang.Object): java.lang.Object
    def deserialize(o: java.lang.Object): java.lang.Object
  }
  case object GeomSerializer extends FieldSerializer {
    override def serialize(o: Object): AnyRef = {
      val geom = o.asInstanceOf[Point]
      ByteBuffer.wrap(WKBUtils.write(geom))
    }

    override def deserialize(o: Object): AnyRef = WKBUtils.read(o.asInstanceOf[ByteBuffer].array())
  }

  case object DefaultSerializer extends FieldSerializer {
    override def serialize(o: Object): AnyRef = o
    override def deserialize(o: Object): AnyRef = o
  }

  object FieldSerializer {
    def apply(attrDescriptor: AttributeDescriptor): FieldSerializer = {
      if(classOf[Geometry].isAssignableFrom(attrDescriptor.getType.getBinding)) GeomSerializer
      else DefaultSerializer
    }
  }

}

class CassandraDataStore(session: Session, keyspaceMetadata: KeyspaceMetadata, ns: URI) extends ContentDataStore {
  import scala.collection.JavaConversions._

  override def createFeatureSource(contentEntry: ContentEntry): ContentFeatureSource =
    new CassandraFeatureStore(contentEntry)

  override def createSchema(featureType: SimpleFeatureType): Unit = {
    val cols =
      featureType.getAttributeDescriptors.map { ad =>
        s"${ad.getLocalName}  ${CassandraDataStore.typeMap(ad.getType.getBinding).getName.toString}"
      }.mkString(",")
    val colCreate = s"(pkz int, z31 bigint, fid text, $cols, PRIMARY KEY (pkz, z31))"
    val stmt = s"create table ${featureType.getTypeName} $colCreate"
    session.execute(stmt)
  }


  override def createContentState(entry: ContentEntry): ContentState =
    new CassandraContentState(entry, session, keyspaceMetadata.getTable(entry.getTypeName))

  override def createTypeNames(): util.List[Name] =
    keyspaceMetadata.getTables.map { t => new NameImpl(ns.toString, t.getName) }.toList
}

class CassandraContentState(entry: ContentEntry, val session: Session, val tableMetadata: TableMetadata) extends ContentState(entry) {
  import scala.collection.JavaConversions._

  val sft: SimpleFeatureType = CassandraDataStore.getSchema(entry.getName, tableMetadata)
  val attrNames = sft.getAttributeDescriptors.map(_.getLocalName)
  val selectClause = (Array("fid") ++ attrNames).mkString(",")
  val table = sft.getTypeName
  val deserializers = sft.getAttributeDescriptors.map { ad => FieldSerializer(ad) }
  val geoTimeQuery =  session.prepare(s"select $selectClause from $table where (pkz = ?) and (z31 >= ?) and (z31 <= ?)")
  val builderPool = ObjectPoolFactory(getBuilder, 10)
  private def getBuilder = {
    val builder = new SimpleFeatureBuilder(sft)
    builder.setValidating(java.lang.Boolean.FALSE)
    builder
  }


}

class CassandraFeatureStore(entry: ContentEntry) extends ContentFeatureStore(entry, Query.ALL) {

  private lazy val contentState = entry.getState(getTransaction).asInstanceOf[CassandraContentState]

  override def getWriterInternal(query: Query, i: Int): FW[SimpleFeatureType, SimpleFeature] =
    new CassandraFeatureWriter(contentState.sft, contentState.session)

  override def buildFeatureType(): SimpleFeatureType = contentState.sft

  override def getBoundsInternal(query: Query): ReferencedEnvelope = new ReferencedEnvelope(-180.0, 180.0, -90.0, 90.0, DefaultGeographicCRS.WGS84)

  override def getCountInternal(query: Query): Int = 10

  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    import org.locationtech.geomesa.filter._
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConversions._

    // TODO: currently we assume that the query has a dtg between predicate and a bbox
    val re = query.getFilter.accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, DefaultGeographicCRS.WGS84).asInstanceOf[Envelope]
    val (lx, ly, ux, uy) = (re.getMinX, re.getMinY, re.getMaxX, re.getMaxY)
    val (dtgFilters, _) = partitionPrimaryTemporals(decomposeAnd(query.getFilter), contentState.sft)
    val interval = FilterHelper.extractInterval(dtgFilters, contentState.sft.getDtgField)
    val sew = CassandraPrimaryKey.epochWeeks(interval.getStart).getWeeks
    val eew = CassandraPrimaryKey.epochWeeks(interval.getEnd).getWeeks

    val rows =
      (sew to eew).flatMap { dt =>
        val dtshift = dt << 16

        val dtg = new DateTime(0).plusWeeks(dt)
        val minz = CassandraPrimaryKey(dtg, re.getMinX, re.getMinY)
        val maxz = CassandraPrimaryKey(dtg, re.getMaxX, re.getMaxY)

        val zranges = org.locationtech.geomesa.cassandra.data.CassandraPrimaryKey.SFC2D.toRanges(minz.x, minz.y, maxz.x, maxz.y)
        zranges.flatMap { case (l, u) => (l to u).map { i => (dtshift + i).toInt } }
      }

    val z3ranges =
      org.locationtech.geomesa.cassandra.data.CassandraPrimaryKey.SFC3D.ranges((lx, ux), (ly, uy), (0, Weeks.weeks(1).toStandardSeconds.getSeconds))

    val queries =
      rows.flatMap { r =>
        z3ranges.map { z =>
         contentState.geoTimeQuery.bind(r: java.lang.Integer, z._1: java.lang.Long, z._2: java.lang.Long)
        }
      }

    val features = contentState.builderPool.withResource { builder =>
      queries.map { q => contentState.session.executeAsync(q) }.flatMap { f => f.get().iterator().map(r => convertRowToSF(r, builder)) }
    }

    new DelegateSimpleFeatureReader(contentState.sft, new DelegateSimpleFeatureIterator(features.iterator))
  }

  def convertRowToSF(r: Row, builder: SimpleFeatureBuilder): SimpleFeature = {
    val attrs = contentState.deserializers.zipWithIndex.map { case (d, idx) => d.deserialize(r.getObject(idx + 1)) }
    val fid = r.getString(0)
    builder.reset()
    builder.buildFeature(fid, attrs.toArray)
  }
}

class CassandraDataStoreFactory extends AbstractDataStoreFactory {

  override def createDataStore(map: util.Map[String, Serializable]): DataStore = {
    val cp = CONTACT_POINT.lookUp(map).asInstanceOf[String]
    val ks = KEYSPACE.lookUp(map).asInstanceOf[String]
    val ns = NAMESPACEP.lookUp(map).asInstanceOf[URI]
    val cluster =
      Cluster.builder()
        .addContactPoint(cp)
        .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
        .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
        .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
        .build()
    val session = cluster.connect(ks)
    new CassandraDataStore(session, cluster.getMetadata.getKeyspace(ks), ns)
  }

  override def createNewDataStore(map: util.Map[String, Serializable]): DataStore = ???

  override def getDescription: String = "GeoMesa Cassandra Data Store"

  override def getParametersInfo: Array[Param] = Array(CONTACT_POINT, KEYSPACE, NAMESPACEP)

  val CONTACT_POINT = new Param("geomesa.cassandra.contact.point"  , classOf[String], "URL to Cassandra",   true)
  val KEYSPACE      = new Param("geomesa.cassandra.keyspace"       , classOf[String], "Cassandra Keyspace", true)
  val NAMESPACEP    = new Param("namespace", classOf[URI], "uri to a the namespace", false, null, new KVP(Parameter.LEVEL, "advanced"))
}


object CassandraPrimaryKey {

  case class Key(idx: Int, x: Double, y: Double, dk: Int, z: Int)

  def unapply(idx: Int): Key = {
    val dk = idx >> 16
    val z = idx & 0x000000ff
    val (x, y) = SFC2D.toPoint(z)
    Key(idx, x, y, dk, z)
  }

  def apply(dtg: DateTime, x: Double, y: Double): Key = {
    val dk = epochWeeks(dtg).getWeeks << 16
    val z = SFC2D.toIndex(x, y).toInt
    val (rx, ry) = SFC2D.toPoint(z)
    val idx = dk + z
    Key(idx, rx, ry, dk, z)
  }

  val EPOCH = new DateTime(0)

  def epochWeeks(dtg: DateTime) = Weeks.weeksBetween(EPOCH, new DateTime(dtg))

  def secondsInCurrentWeek(dtg: DateTime, weeks: Weeks) =
    Seconds.secondsBetween(EPOCH, dtg).getSeconds - weeks.toStandardSeconds.getSeconds

  val SFC2D = new ZCurve2D(math.pow(2,5).toInt)
  val SFC3D = new Z3SFC
}

class CassandraFeatureWriter(sft: SimpleFeatureType, session: Session) extends FW[SimpleFeatureType, SimpleFeature] {
  import CassandraDataStore._
  import org.locationtech.geomesa.utils.geotools.Conversions._
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConversions._

  val cols = sft.getAttributeDescriptors.map { ad => ad.getLocalName }
  val serializers = sft.getAttributeDescriptors.map { ad => FieldSerializer(ad) }
  val geomField = sft.getGeomField
  val geomIdx   = sft.getGeomIndex
  val dtgField  = sft.getDtgField.get
  val dtgIdx    = sft.getDtgIndex.get
  val insert = session.prepare(s"INSERT INTO ${sft.getTypeName} (pkz, z31, fid, ${cols.mkString(",")}) values (${Seq.fill(3+cols.length)("?").mkString(",")})")

  private var curFeature: SimpleFeature = null

  override def next(): SimpleFeature = {
    curFeature = new ScalaSimpleFeature(UUID.randomUUID().toString, sft)
    curFeature
  }

  override def remove(): Unit = ???

  override def hasNext: Boolean = true

  override def write(): Unit = {

    val geom = curFeature.point
    val geo = ByteBuffer.wrap(WKBUtils.write(geom))
    val x = geom.getX
    val y = geom.getY
    val dtg = new DateTime(curFeature.getAttribute(dtgIdx).asInstanceOf[java.util.Date])
    val weeks = CassandraPrimaryKey.epochWeeks(dtg)

    val secondsInWeek = CassandraPrimaryKey.secondsInCurrentWeek(dtg, weeks)
    val pk = CassandraPrimaryKey(dtg, x, y)
    val z3 = CassandraPrimaryKey.SFC3D.index(x, y, secondsInWeek)
    val z31 = z3.z

    val bindings = Array(Int.box(pk.idx), Long.box(z31): java.lang.Long, curFeature.getID) ++
      curFeature.getAttributes.zip(serializers).map { case (o, ser) => ser.serialize(o) }
    session.execute(insert.bind(bindings: _*))
    curFeature = null
  }

  override def getFeatureType: SimpleFeatureType = sft

  override def close(): Unit = {}
}
