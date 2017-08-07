import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import magellan.{Point, Polygon}
import org.apache.spark.sql.magellan.dsl.expressions._
import scala.io.Source;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext._;
import org.apache.log4j.Logger;
import scala.collection.immutable.Vector.empty
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.SQLContext;
import com.mongodb.spark._;



class Geospacial {

}
   case class PolygonRecord(polygon: magellan.Polygon, rowId2: Int)

object Geospacial {
  
    def main(arg: Array[String]) {
         val jobName = "Geopacial"
         val conf = new SparkConf().setAppName(jobName)
         val sc = new SparkContext(conf)
         val sqlContext = new org.apache.spark.sql.SQLContext(sc);
         val arquivoLocations = arg(0)
         val arquivoRequest = arg(1)
         val collection = arg(2)
    
         import sqlContext.implicits._
         
         //Carregamento do Arquivo
         var df_requests = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load(arquivoRequest)
         df_requests = df_requests.withColumn("point", point(df_requests("latitude"), df_requests("longitude")).as("point"));
         var df_locais = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load(arquivoLocations)
         df_locais = df_locais.select("name","lat","long")
         
         def CriarPoligono(lat:Double, long:Double) : Array[magellan.Point] = {
            var poligono = new Array[magellan.Point](5) 
            poligono(0)= magellan.Point(lat+0.0003, long+0.0003)
            poligono(1)= magellan.Point(lat-0.0003, long-0.0003)
            poligono(2)= magellan.Point(lat+0.0003, long-0.0003)
            poligono(3)= magellan.Point(lat-0.0003, long+0.0003)
            poligono(4)= magellan.Point(lat+0.0003, long+0.0003)
            return poligono
         }
         
         val CriarPoligono_udf = udf(CriarPoligono _)
         
         df_locais = df_locais.withColumn("Poligono", CriarPoligono_udf(df_locais("lat"), df_locais("long")))
         
         var rdd_locais_lat = df_locais.select("lat").rdd.map(r => r(0).asInstanceOf[Double]).collect()
         
         var rdd_locais_long = df_locais.select("long").rdd.map(r => r(0).asInstanceOf[Double]).collect()
         
         //---Cria Array de Array de Points para futuramente jogar Polygon em cada elemento desse Array
         
         var Pontos_Poligonos = new Array[Array[magellan.Point]](rdd_locais_lat.size)
          for (i <- 0 to rdd_locais_lat.size-1){
              Pontos_Poligonos(i) = CriarPoligono(rdd_locais_lat(i),rdd_locais_long(i))
          }



//---Joga Polygon em cada elemento do array e coloca cada polygon na seq. Em seguida transforma seq em dataframe

     
        var seq = scala.collection.mutable.ArrayBuffer.empty[PolygonRecord];
        for (i <- 0 to rdd_locais_lat.size-1){
          seq+=PolygonRecord((magellan.Polygon(Array(0), Pontos_Poligonos(i))),i)
        }
        seq.toSeq
        var df_poligonos = sc.parallelize(seq).toDF()



//---Adicionar ID as colunas para join do df_locais com os poligonos do magellan

      //df_locais
      df_locais = df_locais.withColumn("rowId1", monotonically_increasing_id()) 

      // join
      df_locais = df_locais.join(df_poligonos, col("rowId1") === col("rowId2"))
      
      //---Verifica interseccao

    var df_intersecao = df_requests.join(df_locais).where($"point" within $"polygon")
    df_intersecao = df_intersecao.select("name","long","lat","rowId1","publicId","latitude","longitude", "dateTime")      
    MongoSpark.save(df_intersecao.write.option("collection", collection).mode("append"))   
 }
    
}