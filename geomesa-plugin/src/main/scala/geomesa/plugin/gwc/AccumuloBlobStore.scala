package geomesa.plugin.gwc

import com.google.common.hash.Hashing
import com.google.common.io.ByteStreams
import geomesa.utils.text.ObjectPoolFactory
import java.nio.charset.StandardCharsets.UTF_8
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{BatchWriterConfig, ZooKeeperInstance}
import org.apache.accumulo.core.data.{Range => ARange, Value, Mutation}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.geowebcache.io.ByteArrayResource
import org.geowebcache.mime.MimeType
import org.geowebcache.storage.blobstore.file.FilePathGenerator
import org.geowebcache.storage.{BlobStoreListener, TileRange, TileObject, BlobStore}


class AccumuloBlobStore(var table: String,
                        var user: String,
                        var password: String,
                        var instance: String,
                        var zookeepers: String) extends BlobStore {

  val pathGenerator = new FilePathGenerator(table)
  val conn = new ZooKeeperInstance(instance, zookeepers).getConnector(user, new PasswordToken(password))
  val tableOps = conn.tableOperations()
  if(!tableOps.exists(table)) tableOps.create(table)

  val bw = conn.createBatchWriter(table, new BatchWriterConfig)
  val scannerPool = ObjectPoolFactory(conn.createScanner(table, new Authorizations))

  private val NULLBYTE = new Text(Array[Byte](0.toByte))

  private val META = "~META"
  override def putLayerMetadata(layerName: String, key: String, value: String): Unit = {
    val m = new Mutation(s"${META}_$layerName")
    m.put(NULLBYTE, new Text(key), new Value(value.getBytes(UTF_8)))
    bw.addMutation(m)
    bw.flush()
  }

  override def getLayerMetadata(layerName: String, key: String): String = {
    scannerPool.withResource { scanner =>
      val row = s"${META}_$layerName"
      val cq = new Text(key)
      scanner.setRange(new ARange(row, true, row, true))
      scanner.fetchColumn(NULLBYTE, cq)
      val results = scanner.iterator()
      if(results.hasNext) new String(results.next().getValue.get(), UTF_8)
      else null
    }
  }

  override def rename(oldLayerName: String, newLayerName: String): Boolean = true

  override def removeListener(listener: BlobStoreListener): Boolean = true

  override def addListener(listener: BlobStoreListener): Unit = {}

  override def destroy(): Unit = {}

  override def clear(): Unit = {}

  override def put(tileObj: TileObject): Unit = {
    val mimeType = MimeType.createFromFormat(tileObj.getBlobFormat)
    val path = pathGenerator.tilePath(tileObj, mimeType)
    val key = new Text(path.getPath)
    val m = new Mutation(key)
    val bytes = ByteStreams.toByteArray(tileObj.getBlob.getInputStream)
    val md5 = Hashing.md5().hashBytes(bytes).toString
    m.put(new Text(""), new Text(md5), new Value(bytes))
    bw.addMutation(m)
    bw.flush()
  }

  override def get(tileObj: TileObject): Boolean = {
    val mimeType = MimeType.createFromFormat(tileObj.getBlobFormat)
    val path = pathGenerator.tilePath(tileObj, mimeType)
    val key = new Text(path.getPath)
    scannerPool.withResource { scanner =>
      scanner.setRange(new ARange(key, true, key, true))
      val iter = scanner.iterator()
      if(iter.hasNext) {
        val v = iter.next().getValue
        tileObj.setBlob(new ByteArrayResource(v.get()))
        true
      } else false
    }
  }

  override def delete(obj: TileRange): Boolean = true

  override def delete(obj: TileObject): Boolean = true

  override def deleteByGridsetId(layerName: String, gridSetId: String): Boolean = true

  override def delete(layerName: String): Boolean = true

}