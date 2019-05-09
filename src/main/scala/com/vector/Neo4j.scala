package com.vector

import java.io._
import java.nio.file.{Files, Paths}
import java.util.concurrent.CompletionStage

import org.neo4j.driver.v1._

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._


object bdNeo4j {
  def condicion(param: Any):String = param match {
      case param:String => "=\"" + param + "\""
      case param:List[_]  => {
        val list: List[String] = for (a <- param) yield "\"" + a + "\""
        " IN [" + list.mkString(",") + "]"
      }
  }
  def qListPromCodigo(codigo: Any): String = {
    val p1 =      "MATCH (c:Producto)-[*0..]->()<-[r:APLICA_A|:APLICA_PARCIAL_A]-(p:Promocion) USING INDEX c:Producto(name)"
    val p2 = p1 + " WHERE c.name" + condicion(codigo)
    val p3 = p2 + " RETURN DISTINCT p.name AS a, p.tipo AS b, p.numpro AS c, p.desc AS d, type(r) AS e, c.name AS f"
    p3
  }
  def qListExploCodigo(codigo: Any): String = {
    val p1 =      "MATCH (c:Producto)-[*0..]->()<-[r:APLICA_A]-(p:Promocion) USING INDEX c:Producto(name)"
    val p2 = p1 + " WHERE c.name" + condicion(codigo)
    val p3 = p2 + " RETURN DISTINCT p.name AS a, p.tipo AS b, p.numpro AS c, p.desc AS d, type(r) AS e, c.name AS f"
    p3
  }
  def qListExploCodigoplugin(codigo: Any): String = {
    val p1 =      "MATCH (c:Producto) USING INDEX c:Producto(name)"
    val p2 = p1 + " WHERE c.name" + condicion(codigo)
    val p22 = p2 + " CALL eci.getpromos(id(c)) YIELD node"
    val p3 = p22 + " RETURN node.name AS a, node.tipo AS b, node.numpro AS c, node.desc AS d, \"APLICA\" AS e, c.name AS f"
    p3
  }
  def qListExploCodigopluginFiltrado(codigo: Any): String = {
    val valor = codigo match {
      case codigo:String => codigo
      case codigo:List[_]  => codigo.head
    }
    val p1 =      "MATCH (c:Producto) "
    val p2 = p1 + " WHERE c.name =~ '"+ valor +"'" //'Cod[0-9]+' 'Cod9.....'; en código vendrá directamente el Regex
    val p22 = p2 + " CALL eci.getpromos(id(c)) YIELD node"
    val p3 = p22 + " RETURN node.name AS a, node.tipo AS b, node.numpro AS c, node.desc AS d, \"APLICA\" AS e, c.name AS f"
    p3
  }
  def queriesTest(nombre: String,codigo: Any, workerId: String, total:Int,commits:Int ): String = {
    val valor = codigo match {
      case codigo:String => codigo
      case codigo:List[_]  => codigo.head
    }
    nombre match {
      case "generateGraphExploProductos1"  => "CALL eci.gengraphExploProductosTest(" + valor + ",1,"+ total.toString +","+ commits.toString +")"
      case "generateGraphExploProductos2"  => "CALL eci.gengraphExploProductosTest(" + valor + ",2,"+ total.toString +","+ commits.toString +")"
      case "generateGraphExploInicial"  => "call eci.gengraphECIestruc"
      case "deleteGraphExplo"  => "call apoc.periodic.iterate(\"MATCH (n) return n\", \"DETACH DELETE n\", {batchSize:1000})"
    }
  }
  def qExplosionado(nombre: String,codigo: Any, workerId: String ): String = {
    val valor = codigo match {
      case codigo:String => codigo
      case codigo:List[_]  => codigo.head
    }
    nombre match {
      case "numberExploNodes" =>      "MATCH (n) WHERE n.name=~ 'x_.*' RETURN Count(n) AS total"
      case "deleteExploNodes" =>      "MATCH (n) WHERE n.name=~ 'x_.*' DELETE n"
      case "deleteExploRelations1"  => "MATCH (n)-[r:X_PERTENECE_A | :X_SE_VENDE_EN | :X_APLICA_A | :X_APLICA_PARCIAL_A | :X_APLICA_COMB_A_MER | :X_APLICA_COMB_A_ORG ]-() delete r"
      case "deleteExploRelations2"  => "MATCH (n)-[r:X_ASIGNADO_A | :X_SEGUIDO_DE ]-() delete r"
      case "generateGraphExploEstruc"  => "CALL eci.gengraphExploMod2estruc()"
      case "generateGraphExploProductos1"  => "CALL eci.gengraphExploMod2productos(" + valor + ",1)"
      case "createExploIndex"  => "CREATE INDEX ON :x_Producto(name)"
      case "generateGraphExploProductos2"  => "CALL eci.gengraphExploMod2productos(" + valor + ",2)"
      case "exportExplosionadoPOC"  => "MATCH (c:x_Producto {name: '" + valor + "'}) CALL eci.getpromosv2op1POC(id(c),'0') YIELD merpromo, cen  RETURN merpromo[0] AS a, merpromo[1] AS b, toInt(merpromo[2]) AS c, merpromo[3] AS d, merpromo[4] AS e, c.name AS f, cen AS g"
      case "exportExplosionadoECI"  => "MATCH (c:x_Producto {name: '" + valor + "'}) CALL eci.getpromosECI(id(c),'0') YIELD merpromo, cen  RETURN merpromo[0] AS a, merpromo[1] AS b, toInt(merpromo[2]) AS c, merpromo[3] AS d, merpromo[4] AS e, c.name AS f, cen AS g"
      case "ORIGINAL_exportExplosionado"  => "MATCH (c:x_Producto {name: '" + valor + "'}) CALL eci.getpromosv2op1(id(c),'0') YIELD name, tipo, numpro, desc, rel, cen RETURN name AS a, tipo AS b, numpro AS c, desc AS d, rel AS e, c.name AS f, cen AS g"
      case "exportExplosionado_like"  => "MATCH (c:x_Producto) WHERE c.name =~ '" + valor + "' CALL eci.getpromosv2(id(c)) YIELD name, tipo, numpro, desc, rel, cen RETURN name AS a, tipo AS b, numpro AS c, desc AS d, rel AS e, c.name AS f, cen AS g"
      // valor -> "999]->(p) where c.name='x_Cod1"
      case "exportExplosionado_lista_fuerza bruta"  => "MATCH (c:x_Producto)-[:X_SEGUIDO_DE*0.." + valor + "' CALL eci.getpromosv2(id(p)) YIELD name, tipo, numpro, desc, rel, cen RETURN name AS a, tipo AS b, numpro AS c, desc AS d, rel AS e, p.name AS f, cen AS g"
      case "exportExplosionado_lista_opt traversal mercanciaPOC"  => "MATCH (c:x_Producto)-[:X_SEGUIDO_DE*0.." + valor + "' CALL eci.getpromosv2op1POC(id(p),'" + workerId + "') YIELD merpromo, cen  RETURN merpromo[0] AS a, merpromo[1] AS b, toInt(merpromo[2]) AS c, merpromo[3] AS d, merpromo[4] AS e, p.name AS f, cen AS g"
      case "exportExplosionado_lista_opt traversal mercanciaECI"  => "MATCH (c:x_Producto)-[:X_SEGUIDO_DE*0.." + valor + "' CALL eci.getpromosECI(id(p),'" + workerId + "') YIELD merpromo, cen  RETURN merpromo[0] AS a, merpromo[1] AS b, toInt(merpromo[2]) AS c, merpromo[3] AS d, merpromo[4] AS e, p.name AS f, cen AS g"
      case "ORIGINAL_exportExplosionado_lista_opt traversal mercancia"  => "MATCH (c:x_Producto)-[:X_SEGUIDO_DE*0.." + valor + "' CALL eci.getpromosv2op1(id(p),'" + workerId + "') YIELD name, tipo, numpro, desc, rel, cen RETURN name AS a, tipo AS b, numpro AS c, desc AS d, rel AS e, p.name AS f, cen AS g"
      case "borradocacheMercancia" => "CALL eci.storage.clear()"


      //MATCH (n) WHERE n.name=~ 'x_.*' RETURN n
        //CREATE (n:x_Familia { name: 'x_Familia1'})
        //CREATE (n:x_Fabricante { name: 'x_Fabri2'})
        //CREATE (n:x_Fabricante { name: 'x_Fabri1'})
        //MATCH (a:x_Familia),(b:x_Fabricante) WHERE a.name = 'x_Familia1' AND b.name = 'x_Fabri1' CREATE (b)-[r:x_PERTENECE_A]->(a) RETURN type(r)
    }
  }
  def qNumberNodes: String = {
    val p1 =      "MATCH (n) RETURN Count(n) AS total"
    p1
  }
  def recordToString (record: Record): String = {
    val p1 = record.get("a").asString() + "," + record.get("b").asString() + "," + record.get("c").asInt()
    p1 + "," + record.get("d").asString() + "," + record.get("e").asString() + "," + record.get("f").asString()
  }
  def procesarRecordconMap (result: StatementResult): List[String] = {
    result.asScala
        .toList
        .map(record => record.get("a").asString() + "," + record.get("b").asString() + "," + record.get("c").asInt() + "," + record.get("d").asString() + "," + record.get("e").asString() + "," + record.get("f").asString())
  }
  def procesarRecordconForp (result: StatementResult): List[String] = {
    val list = for (record <- result.asScala) yield {
      val p1 = record.get("a").asString() + "," + record.get("b").asString() + "," + record.get("c").asInt()
      p1 + "," + record.get("d").asString() + "," + record.get("e").asString() + "," + record.get("f").asString()
    }
    list.toList
  }
}


class Neo4j(pool: Driver, exploPath: String, modeloGraph: String){

  //private val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  case class User(name: String, last_name: String, age: Int, city: String)
  val driver = pool
  val path = exploPath

  def whileLoop(cond : =>Boolean, block : =>Unit) : Unit =
    if(cond) {
      block
      whileLoop(cond, block)
    }

  def promosParcialesAplicables (record: Vector[Promocion]): Vector[Promocion] = {
    val listapromos: Vector[Promocion] = record.filter(x => x.codigos.length == x.numcod)
    listapromos
  }
  def promosParcialesAplicablesv2 (record: Vector[Explosionado]): Vector[Explosionado] = {
    val listapromos: Vector[Explosionado] = record.filter(x => x.codigos.length == x.numcod)
    listapromos
  }

  def getNumberNodes (index: Int): Long = {
    val script = if (index ==1 )
                      bdNeo4j.qNumberNodes
                  else
                      bdNeo4j.qExplosionado("numberExploNodes","","0")
    val session = driver.session
    val result: StatementResult = session.run(script)
    val total: Long = if (result.hasNext()) result.next().get ("total").asLong() else 0L
    session.close()
    total
  }

  //Grafo Versión 2
  def generagraph_mod2  = {

    val session = driver.session
    val resultdr: StatementResult = session.run(bdNeo4j.qExplosionado("deleteExploRelations","","0"))
    val resultdn: StatementResult = session.run(bdNeo4j.qExplosionado("deleteExploNodes","","0"))
    val result: StatementResult = session.run(bdNeo4j.qExplosionado("generateGraphExploEstruc","","0"))
    //precarga primer millón de datos del grafo tipo 1
    0 to 9 map {i =>
      session.run(bdNeo4j.qExplosionado("generateGraphExploProductos1", (3 + 100000*i).toString,"0"))
    }
    //precarga segundo millón de datos del grafo tipo 2
    10 to 19 map {i =>
      session.run(bdNeo4j.qExplosionado("generateGraphExploProductos2", (3 + 100000*i).toString,"0"))
    }
    session.close()
  }
  def exportAsociatepromotionsAsync_mod2(codigo: Any, html: Int, sufijo: String, bc: ExecutionContext): Future[(Vector[Explosionado], List[String])] = {
    implicit val ec: ExecutionContext = bc
    val date = System.currentTimeMillis()
    println("codigoAsync " + codigo + "direc " + path)
    val script = bdNeo4j.qExplosionado("exportExplosionado",codigo,"0")
    val session = driver.session
    val cursorStage: CompletionStage[StatementResultCursor]  = session.runAsync(script) //.thenComposeAsync( cursor => cursor.listAsync( record => record.get())
    val cursorFuture = toScala(cursorStage)

    //val basename = """C:/DATOSP~1/Vector/PoC/PLAYPR~1/poc-bigfiles-upload/Uploadfiles/explosionado/"""
    val basename = path
    val filenameoutv2 =  basename + "explosionadov2" + sufijo + ".csv"  // "C:\\Datos PCM\\Vector\\PoC\\Play projects\\poc-bigfiles-upload\\Uploadfiles\\"
    val fout: File = new File(filenameoutv2)
    val filenameoutv2log =  basename + "explosionadov2_log" + sufijo + ".csv"  // "C:\\Datos PCM\\Vector\\PoC\\Play projects\\poc-bigfiles-upload\\Uploadfiles\\"
    val flogout: File = new File(filenameoutv2log)


    val result1 = cursorFuture.flatMap(f => toScala(f.listAsync()))(bc)
    result1.map {
      case result3 =>
        val result = result3.listIterator()
        var record_fetch: Vector[Explosionado] = Vector()
        var indice_promos = new scala.collection.mutable.HashMap[String, Int]
        var parcial_promo: Vector[Explosionado] = Vector()
        var indice_parcial_promos = new scala.collection.mutable.HashMap[String, Int]

        val out = new BufferedWriter(new FileWriter(fout))
        val logout = new BufferedWriter(new FileWriter(flogout))
        logout.write("tiempo ms en procesar : "+ (System.currentTimeMillis() - date).toString + "\r\n")
        val datew = System.currentTimeMillis()
        var contador = 0
        var cod_cen = ""
        var num_p_g =0
        var num_p_otras =0
        var num_p_eliminadas =0

        if (result.hasNext) {
          while (result.hasNext()) {
            val record = result.next()

            //hacemos alguna lógica para simular una hipotética carga de proceso
            if (cod_cen!=record.get("f").asString() + "," + record.get("g").asString()) {
              cod_cen = record.get("f").asString() + "," + record.get("g").asString()
              num_p_g = 0
              num_p_otras = 0
              num_p_eliminadas = 0
            }else{
              if ("G" == record.get("b").asString())  {
                num_p_g = num_p_g +1
                if (num_p_g>1) num_p_eliminadas = num_p_eliminadas +1
              }else{
                num_p_otras = num_p_otras +1
              }
            }

            out.write(record.get("f").asString() + "," + record.get("g").asString() + "," + record.get("a").asString()+","+record.get("b").asString()+","+ record.get("c").asInt()+","+ record.get("d").asString()+","+ record.get("e").asString() + "\r\n")
            contador = contador +1

            if (html ==1) {
              val rec_promo = new Explosionado(record.get("a").asString(), record.get("b").asString(), record.get("c").asInt(), record.get("d").asString(), record.get("e").asString(), List(record.get("f").asString()), record.get("g").asString())

              if (rec_promo.numcod == 1) {
                record_fetch = record_fetch :+ rec_promo
                indice_promos += rec_promo.codpromo -> (record_fetch.length - 1)
              } else {
                if (indice_parcial_promos.exists(_._1 == rec_promo.codpromo)) {
                  var promotmp = parcial_promo(indice_parcial_promos(rec_promo.codpromo))
                  promotmp.codigos = promotmp.codigos ::: rec_promo.codigos
                } else {
                  parcial_promo = parcial_promo :+ rec_promo
                  indice_parcial_promos += rec_promo.codpromo -> (parcial_promo.length - 1)
                }
              }
            }
          }
          out.close()
          logout.write("numero de promos: "+ contador + "\r\n")
          logout.write("tiempo ms en escribir: "+ (System.currentTimeMillis() - datew).toString + "\r\n")
          logout.write("tiempo ms total: "+ (System.currentTimeMillis() - date).toString + "\r\n")
          logout.close()
        } else {
          out.write("Ningun acierto "+ "\r\n")
          out.close()
          logout.write("numero de promos: "+ 0 + "\r\n")
          logout.write("tiempo ms: "+ (System.currentTimeMillis() - date).toString + "\r\n")
          logout.close()
          "Ningún acierto"
        }
        record_fetch ++= promosParcialesAplicablesv2(parcial_promo)
        session.closeAsync()
        (record_fetch, List((System.currentTimeMillis() - date).toString))
    }(bc)
    //val explo = new Explosionado("Codprom1","A",0,"descrip","R", List(),"Codcen1")
    //Future((Vector(explo), List("")))
  }

  //---------------------------------------------------
  def exportAsociatepromotions_mod2(metodo: String, codigo: Any, html: Int, sufijop: String, overwrite: Int, filewrite: Int,workerId: String): (Vector[Explosionado], List[String]) = {

    val date = System.currentTimeMillis()
    println("codigo " + codigo)

    val sufijomodelo = if (modeloGraph.equals("POC")) "POC" else "ECI"

    val script =
      if (html ==1)
        bdNeo4j.qExplosionado("exportExplosionado"+sufijomodelo,codigo,"0")
      else
      if (metodo == "lista_fuerza bruta")
        bdNeo4j.qExplosionado("exportExplosionado_lista_fuerza bruta",codigo,"0")
      else
      if (metodo == "lista_opt traversal mercancia")
        bdNeo4j.qExplosionado("exportExplosionado_lista_opt traversal mercancia"+sufijomodelo,codigo,workerId)
      else
        bdNeo4j.qExplosionado("exportExplosionado_like",codigo,"0")
    val session = driver.session
    val result: StatementResult = session.run(script)

    //val basename = """C:/DATOSP~1/Vector/PoC/PLAYPR~1/poc-bigfiles-upload/Uploadfiles/explosionado/"""
    val basename = path
    val sufijo = if(overwrite == 0) sufijop else overwrite
    val filenameoutv2 =  basename + "explosionadov2" + sufijo + ".csv"  // "C:\\Datos PCM\\Vector\\PoC\\Play projects\\poc-bigfiles-upload\\Uploadfiles\\"
    val fout: File = new File(filenameoutv2)
    val filenameoutv2log =  basename + "explosionadov2_log" + sufijo + ".csv"  // "C:\\Datos PCM\\Vector\\PoC\\Play projects\\poc-bigfiles-upload\\Uploadfiles\\"
    val flogout: File = new File(filenameoutv2log)

    var record_fetch: Vector[Explosionado] = Vector()

    val out = new BufferedWriter(new FileWriter(fout))
    val logout = new BufferedWriter(new FileWriter(flogout))
    logout.write("tiempo ms en procesar : "+ (System.currentTimeMillis() - date).toString + "\r\n")
    val datew = System.currentTimeMillis()

    if (result.hasNext()) {
      //record_fetch = procesar_result_campos(result,out,logout,html,filewrite,date,datew)
      record_fetch = procesar_result_List_agregada(result,out,logout,html,filewrite,date,datew)
    }
    session.close()
    (record_fetch, List((System.currentTimeMillis() - date).toString))
  }

  def procesar_result_List_agregada(result: StatementResult, out: BufferedWriter, logout: BufferedWriter, html: Int, filewrite: Int, date: Long, datew: Long): Vector[Explosionado] = {
    var record_fetch: Vector[Explosionado] = Vector()
    var record_post: ListBuffer[Record] = ListBuffer()


    var indice_parcial_promos = new scala.collection.mutable.HashMap[String, Int]
    var contador = 0
    var cod_cen = ""
    var num_p_g =0
    var num_p_otras =0
    var num_p_eliminadas =0

    var rotura_ordenacion="sincodigo"
    var i_promos_combinadas = new scala.collection.mutable.HashMap[String, Record]

    if (result.hasNext()) {
      while (result.hasNext()) {
        val record = result.next()
        //hacemos alguna lógica para simular una hipotética carga de proceso
        if (cod_cen!=record.get("f").asString() + "," + record.get("g").asList().get(0)) {
          cod_cen = record.get("f").asString() + "," + record.get("g").asList().get(0)
          num_p_g = 0
          num_p_otras = 0
          num_p_eliminadas = 0
        }else{
          if ("G" == record.get("b").asString())  {
            num_p_g = num_p_g +1
            if (num_p_g>1) num_p_eliminadas = num_p_eliminadas +1
          }else{
            num_p_otras = num_p_otras +1
          }
        }
        val numpromos = record.get("c").asInt()
        val codpromo = record.get("a").asString()
        val codcentro = record.get("g").asList().get(0).toString
        val codpro = record.get("f").asString()
        val rec_promo = new Explosionado(record.get("a").asString(), record.get("b").asString(), numpromos, record.get("d").asString(), record.get("e").asString(), List(record.get("f").asString()), record.get("g").asList().toString) //record.get("g").asList().get(0).toString)
        //if (html ==1) {

          if ((rec_promo.codigos.head) != rotura_ordenacion) { // + rec_promo.codcentro) != rotura_ordenacion) {
            //cuando cambia el código producto, el cálculo de promociones combinadas se resetea
            val (record_fetch_desagregado: Vector[Explosionado],cont_desagregado) = desagrega_procesa_PromoHits_combinados(record_post,out,logout,html,filewrite)
            record_fetch = record_fetch ++ record_fetch_desagregado
            contador = contador +cont_desagregado
            rotura_ordenacion = rec_promo.codigos.head
            i_promos_combinadas.clear()
            record_post.clear()
          }
          if (rec_promo.numcod == 1) {
            val (record_fetch_desagregado,cont_desagregado) = desagrega_PromoHit_directos(record,out,logout,html,filewrite)
            record_fetch = record_fetch ++ record_fetch_desagregado//rec_promo
            //[desagrega_PromoHit] out.write(record.get("f").asString() + "," + record.get("g").asList().get(0).toString + "," + record.get("a").asString() + "," + record.get("b").asString() + "," + numpromos + "," + record.get("d").asString() + "," + record.get("e").asString() + "\r\n")
            contador = contador +cont_desagregado
          } else {
            if (i_promos_combinadas.exists(_._1 == (rec_promo.codpromo))) {
              //agrega a record actual al post y el original que ya existía
              record_post = record_post :+ record //rec_promo
              record_post = record_post :+ i_promos_combinadas(rec_promo.codpromo)
              /*record_fetch = record_fetch :+ rec_promo //si existe es que al menos está dos veces y hay que incluir la promoción
              out.write(record.get("f").asString() + "," + record.get("g").asList().get(0).toString + "," + record.get("a").asString() + "," + record.get("b").asString() + "," + numpromos + "," + record.get("d").asString() + "," + record.get("e").asString() + "\r\n")
              contador = contador +1*/
            } else {
              i_promos_combinadas += (rec_promo.codpromo) -> record //rec_promo //(parcial_promo.length - 1)
            }
          }
      }
      val (record_fetch_desagregado: Vector[Explosionado],cont_desagregado) = desagrega_procesa_PromoHits_combinados(record_post,out,logout,html,filewrite)
      record_fetch = record_fetch ++ record_fetch_desagregado
      contador = contador +cont_desagregado
      out.close()
      logout.write("numero de promos: "+ contador + "\r\n")
      logout.write("tiempo ms en escribir: "+ (System.currentTimeMillis() - datew).toString + "\r\n")
      logout.write("tiempo ms total: "+ (System.currentTimeMillis() - date).toString + "\r\n")
      logout.close()
    } else {
      out.write("Ningun acierto "+ "\r\n")
      out.close()
      logout.write("numero de promos: "+ 0 + "\r\n")
      logout.write("tiempo ms: "+ (System.currentTimeMillis() - date).toString + "\r\n")
      logout.close()
      "Ningún acierto"
    }
    record_fetch
  }
  def desagrega_PromoHit_directos(record: Record, out: BufferedWriter, logout: BufferedWriter, html: Int, filewrite: Int): (Vector[Explosionado],Int) = {
    var record_fetch: Vector[Explosionado] = Vector()
    var contador: Int =0
    val lista = record.get("g").asList()
    lista.forEach(c => {
      if (html==1) {
        val rec_promo = new Explosionado(record.get("a").asString(), record.get("b").asString(), record.get("c").asInt(), record.get("d").asString(), record.get("e").asString(), List(record.get("f").asString()), c.toString)
        record_fetch = record_fetch :+ rec_promo
        out.write(record.get("f").asString() + "," + c + "," + record.get("a").asString() + "," + record.get("b").asString() + "," + record.get("c").asInt() + "," + record.get("d").asString() + "," + record.get("e").asString() + "\r\n")
      } else {
        if (filewrite==1) out.write(record.get("f").asString() + "," + c + "," + record.get("a").asString() + "," + record.get("b").asString() + "," + record.get("c").asInt() + "," + record.get("d").asString() + "," + record.get("e").asString() + "\r\n")
      }
      contador = contador +1
    })
    return (record_fetch,contador)
  }
  def desagrega_procesa_PromoHits_combinados(record_post: ListBuffer[Record], out: BufferedWriter, logout: BufferedWriter, html: Int, filewrite: Int): (Vector[Explosionado],Int) = {
    var record_fetch: Vector[Explosionado] = Vector()
    var contador: Int =0
    var rotura_ordenacion="sincodigo"
    var i_promos_combinadas = new scala.collection.mutable.HashMap[String, Int]

    record_post.foreach(record => {
      val lista = record.get("g").asList()
      val codpromo = record.get("a").asString()
      val codpro = record.get("f").asString()
      lista.forEach(c => {
        if ((codpro)!=rotura_ordenacion) {
          rotura_ordenacion=codpro
          i_promos_combinadas.clear()
        }
        if (i_promos_combinadas.exists(_._1 == (codpromo + c))) {
          if (html==1) {
            val rec_promo = new Explosionado(record.get("a").asString(), record.get("b").asString(), record.get("c").asInt(), record.get("d").asString(), record.get("e").asString(), List(record.get("f").asString()), c.toString)
            record_fetch = record_fetch :+ rec_promo
            out.write(record.get("f").asString() + "," + c + "," + record.get("a").asString() + "," + record.get("b").asString() + "," + record.get("c").asInt() + "," + record.get("d").asString() + "," + record.get("e").asString() + "\r\n")
          } else {
            if (filewrite==1) out.write(record.get("f").asString() + "," + c + "," + record.get("a").asString() + "," + record.get("b").asString() + "," + record.get("c").asInt() + "," + record.get("d").asString() + "," + record.get("e").asString() + "\r\n")
          }
          contador = contador +1
        } else {
          i_promos_combinadas += (codpromo + c) -> 1
        }
      })
    })
    return (record_fetch,contador)
  }

  def procesar_result_campos(result: StatementResult, out: BufferedWriter, logout: BufferedWriter, html: Int, filewrite: Int, date: Long, datew: Long): Vector[Explosionado] = {
    var record_fetch: Vector[Explosionado] = Vector()

    var indice_promos = new scala.collection.mutable.HashMap[String, Int]
    var parcial_promo: Vector[Explosionado] = Vector()
    var indice_parcial_promos = new scala.collection.mutable.HashMap[String, Int]
    var contador = 0
    var cod_cen = ""
    var num_p_g =0
    var num_p_otras =0
    var num_p_eliminadas =0

    var rotura_ordenacion="sincodigo"
    var i_promos_combinadas = new scala.collection.mutable.HashMap[String, Int]

    if (result.hasNext()) {
      while (result.hasNext()) {
        val record = result.next()
        //hacemos alguna lógica para simular una hipotética carga de proceso
        if (cod_cen!=record.get("f").asString() + "," + record.get("g").asString()) {
          cod_cen = record.get("f").asString() + "," + record.get("g").asString()
          num_p_g = 0
          num_p_otras = 0
          num_p_eliminadas = 0
        }else{
          if ("G" == record.get("b").asString())  {
            num_p_g = num_p_g +1
            if (num_p_g>1) num_p_eliminadas = num_p_eliminadas +1
          }else{
            num_p_otras = num_p_otras +1
          }
        }
        if (html ==1) {



          val rec_promo = new Explosionado(record.get("a").asString(), record.get("b").asString(), record.get("c").asInt(), record.get("d").asString(), record.get("e").asString(), List(record.get("f").asString()), record.get("g").asString())
          if ((rec_promo.codigos.head + rec_promo.codcentro) != rotura_ordenacion) {
            //cuando cambia tanto el centro como elcódigo producto , el cálculo de promociones combinadas se resetea
            rotura_ordenacion = rec_promo.codigos.head + rec_promo.codcentro
            i_promos_combinadas.clear()
          }
          if (rec_promo.numcod == 1) {
            record_fetch = record_fetch :+ rec_promo
            indice_promos += rec_promo.codpromo -> (record_fetch.length - 1)
            out.write(record.get("f").asString() + "," + record.get("g").asString() + "," + record.get("a").asString() + "," + record.get("b").asString() + "," + record.get("c").asInt() + "," + record.get("d").asString() + "," + record.get("e").asString() + "\r\n")
            contador = contador +1

          } else {
            if (i_promos_combinadas.exists(_._1 == (rec_promo.codpromo))) {
              record_fetch = record_fetch :+ rec_promo //si existe es que al menos está dos veces y hay que incluir la promoción
              out.write(record.get("f").asString() + "," + record.get("g").asString() + "," + record.get("a").asString() + "," + record.get("b").asString() + "," + record.get("c").asInt() + "," + record.get("d").asString() + "," + record.get("e").asString() + "\r\n")
              contador = contador +1

            } else {
              parcial_promo = parcial_promo :+ rec_promo
              i_promos_combinadas += (rec_promo.codpromo) -> 1 //(parcial_promo.length - 1)
              //indice_parcial_promos += (rec_promo.numcod + rec_promo.codcentro + rec_promo.codpromo) -> (parcial_promo.length - 1)
            }
          }
        } else {
          //Sólo imprime lineas en el FILE pero con la misma lógica
          val numpromos = record.get("c").asInt()
          val codpromo = record.get("a").asString()
          val codcentro = record.get("g").asString()
          val codpro = record.get("f").asString()

          if ((codpro + codcentro)!=rotura_ordenacion) {
            //cuando cambia tanto el centro como elcódigo producto , el cálculo de promociones combinadas se resetea
            rotura_ordenacion=codpro + codcentro
            i_promos_combinadas.clear()
          }

          if (numpromos == 1) {
            if (filewrite==1) out.write(codpro + "," + codcentro + "," + codpromo +","+record.get("b").asString()+","+ numpromos +","+ record.get("d").asString()+","+ record.get("e").asString() + "\r\n")
            contador = contador +1
          } else {
            if (i_promos_combinadas.exists(_._1 == (codpromo))) {
              if (filewrite==1) out.write(codpro + "," + codcentro + "," + codpromo+","+record.get("b").asString()+","+numpromos +","+record.get("d").asString()+","+ record.get("e").asString() + "\r\n")
              contador = contador +1
            } else {
              i_promos_combinadas += (codpromo) -> 1 //(parcial_promo.length - 1)
            }
          }
        }
      }
      out.close()
      logout.write("numero de promos: "+ contador + "\r\n")
      logout.write("tiempo ms en escribir: "+ (System.currentTimeMillis() - datew).toString + "\r\n")
      logout.write("tiempo ms total: "+ (System.currentTimeMillis() - date).toString + "\r\n")
      logout.close()
    } else {
      out.write("Ningun acierto "+ "\r\n")
      out.close()
      logout.write("numero de promos: "+ 0 + "\r\n")
      logout.write("tiempo ms: "+ (System.currentTimeMillis() - date).toString + "\r\n")
      logout.close()
      "Ningún acierto"
    }

    record_fetch
  }
  //----------------------------------------------------
  // Grafo Versión 1
  /*
  * Objetivo: obtener promos que aplican a una lista de códigos.
  * tipo se usa para decidir si se usa Cyper, Cypher explosionado o Cypher plugin
  * Ejecuta el Cypher -> que devuelve todos las promociones asociadas a cada código.
  * Aplica la lógica necesaria para descartar los que aplican de los que no.
  */
  def getAplicablepromotions(codigo: Any, tipo: Int): (Vector[Promocion], List[String]) = {

    val script = tipo match {
      case 0  => bdNeo4j.qListPromCodigo(codigo)
      case 1  => bdNeo4j.qListExploCodigo(codigo)
      case _  => bdNeo4j.qListExploCodigoplugin(codigo)
    }
    val date = System.currentTimeMillis()
    val session = driver.session
    val result: StatementResult = session.run(script)

    var record_fetch: Vector[Promocion] = Vector()
    var indice_promos = new scala.collection.mutable.HashMap[String,Int]
    var parcial_promo: Vector[Promocion] = Vector()
    var indice_parcial_promos = new scala.collection.mutable.HashMap[String,Int]
    if (result.hasNext()) {
      while (result.hasNext()) {
        val record = result.next()
        val rec_promo = new Promocion( record.get("a").asString(), record.get("b").asString() ,record.get("c").asInt(), record.get("d").asString() ,record.get("e").asString() ,List(record.get("f").asString()))
        if (rec_promo.numcod == 1 ) {
          record_fetch = record_fetch :+ rec_promo
          indice_promos += rec_promo.codpromo -> (record_fetch.length - 1)
        } else {
          if (indice_parcial_promos.exists(_._1 == rec_promo.codpromo)) {
            var promotmp = parcial_promo(indice_parcial_promos(rec_promo.codpromo))
            promotmp.codigos = promotmp.codigos ::: rec_promo.codigos
          } else {
            parcial_promo = parcial_promo :+ rec_promo
            indice_parcial_promos += rec_promo.codpromo -> (parcial_promo.length - 1)
          }
        }
      }
    }else{
      "Ningún acierto"
    }
    record_fetch = promosParcialesAplicables(parcial_promo) ++ record_fetch
    val dur2 = System.currentTimeMillis()
    val durd2 = dur2 - date
    session.close()
    (record_fetch, List(durd2.toString ))
  }
  /*
  * Objetivo: obtener promos que aplican a una lista de códigos.
  * Exactamente igual que "getAplicablepromotions", pero llama a Neo4J de forma asíncrona.
  */
  def getAplicablepromotionsAsync(codigo: Any, tipo: Int, bc: ExecutionContext): Future[(Vector[Promocion], List[String])] = {
    implicit val ec: ExecutionContext = bc
    val script = tipo match {
      case 0  => bdNeo4j.qListPromCodigo(codigo)
      case 1  => bdNeo4j.qListExploCodigo(codigo)
      case _  => bdNeo4j.qListExploCodigoplugin(codigo)
    }

    val session = driver.session
    val cursorStage: CompletionStage[StatementResultCursor]  = session.runAsync(script)
    val cursorFuture = toScala(cursorStage)

    //val result = cursorFuture.foreach(r => r.nextAsync())
    val result1 = cursorFuture.flatMap(f => toScala(f.listAsync()))(bc)
    result1.map {
      case result3 =>
        val result = result3.listIterator()
        var record_fetch: Vector[Promocion] = Vector()
        var indice_promos = new scala.collection.mutable.HashMap[String, Int]
        var parcial_promo: Vector[Promocion] = Vector()
        var indice_parcial_promos = new scala.collection.mutable.HashMap[String, Int]
        if (result.hasNext()) {
          while (result.hasNext()) {
            val record = result.next()
            val rec_promo = new Promocion(record.get("a").asString(), record.get("b").asString(), record.get("c").asInt(), record.get("d").asString(), record.get("e").asString(), List(record.get("f").asString()))
            if (rec_promo.numcod == 1) {
              record_fetch = record_fetch :+ rec_promo
              indice_promos += rec_promo.codpromo -> (record_fetch.length - 1)
            } else {
              if (indice_parcial_promos.exists(_._1 == rec_promo.codpromo)) {
                var promotmp = parcial_promo(indice_parcial_promos(rec_promo.codpromo))
                promotmp.codigos = promotmp.codigos ::: rec_promo.codigos
              } else {
                parcial_promo = parcial_promo :+ rec_promo
                indice_parcial_promos += rec_promo.codpromo -> (parcial_promo.length - 1)
              }
            }
          }
        } else {
          "Ningún acierto"
        }
        record_fetch ++= promosParcialesAplicables(parcial_promo)
        session.closeAsync()
        (record_fetch, List(2.toString))
    }(bc)
  }
  /*
  * Objetivo: obtener promos asociados a una lista de códigos.
  * tipo se usa para decidir si se usa Cyper, Cypher explosionado, Cypher plugin o Cypher plugin que procesa múltiples nodos
  * Ejecuta el Cypher -> que devuelve todos las promociones asociadas a cada código.
  * Escribe en fichero el resultado.
  */
  def getAsociatepromotionsAsync(codigo: Any, tipo: Int, bc: ExecutionContext): Future[(Vector[Promocion], List[String])] = {
    implicit val ec: ExecutionContext = bc
    val script = tipo match {
      case 0  => bdNeo4j.qListPromCodigo(codigo)
      case 1  => bdNeo4j.qListExploCodigo(codigo)
      case 2  => bdNeo4j.qListExploCodigoplugin(codigo)
      case _  => bdNeo4j.qListExploCodigopluginFiltrado(codigo)
    }
    val date = System.currentTimeMillis()

    val basename = """C:/DATOSP~1/Vector/PoC/PLAYPR~1/poc-bigfiles-upload/Uploadfiles/explosionado/"""
    val filenameout =  basename + "explosionado.csv"  // "C:\\Datos PCM\\Vector\\PoC\\Play projects\\poc-bigfiles-upload\\Uploadfiles\\"
    val fout: File = new File(filenameout)
    //var salida: Vector[String] = Vector()

    var record_fetch: Vector[Promocion] = Vector()

    val session = driver.session

    //val result: StatementResult = session.run(script)
    val cursorStage: CompletionStage[StatementResultCursor]  = session.runAsync(script)
    val cursorFuture = toScala(cursorStage)
    val result1 = cursorFuture.flatMap(f => toScala(f.listAsync()))(bc)
    result1.map {
      case result3 =>
        val result = result3.listIterator()
        //var indice_promos = new scala.collection.mutable.HashMap[String,Int]
        //var parcial_promo: Vector[Promocion] = Vector()
        //var indice_parcial_promos = new scala.collection.mutable.HashMap[String,Int]

        //val out = new BufferedOutputStream(new FileOutputStream(fout))
        val out = new BufferedWriter(new FileWriter(fout)) //abre fichero en modalidad overwrite, if second parameter is true will be append mode
        if (result.hasNext()) {
          while (result.hasNext()) {
            //escribirlo en el file
            val record = result.next()
            //salida = salida :+ record.get("f").asString() + "," + record.get("a").asString() + "," + record.get("b").asString() + "," + record.get("c").asInt() + "," + record.get("d").asString() + "," + record.get("e").asString() + "\r\n"
            out.write(record.get("f").asString() + "," + record.get("a").asString()+","+record.get("b").asString()+","+ record.get("c").asInt()+","+ record.get("d").asString()+","+ record.get("e").asString()+"\r\n")
          }
          out.close()
        } else {
          //escribirlo en el file
          out.write("Ningún acierto")
          out.close()
        }
      session.close()
    //record_fetch = promosParcialesAplicables(parcial_promo) ++ record_fetch
	  val dur2 = System.currentTimeMillis()
    val durd2 = dur2 - date
    /*try {
      val out = new BufferedWriter(new FileWriter(fout)) //abre fichero en modalidad overwrite, if second parameter is true will be append mode
      salida.map(st => out.write(st))
      out.close()
    }catch {
      case e: IOException => logger.info(e.printStackTrace.toString)
    }
    val dur3 = System.currentTimeMillis()
    val durd3 = dur3 - dur2*/
    (record_fetch, List(durd2.toString ))
    }(bc)
  }

  /*
 * Prueba no usada actualmente.
 * Y estaría asociada a dar resultados de promociones asociadas (no aplicables)
 */
  def getDEPRECATEDAsociatepromotionsAsync(codigo: Any, tipo: Int, bc: ExecutionContext): Future[(Vector[Promocion], List[String])] = {
    //implicit val ec: ExecutionContext = bc
    val script = if (tipo==0) bdNeo4j.qListPromCodigo(codigo) else bdNeo4j.qListExploCodigo(codigo)
    val session = driver.session
    val cursorStage: CompletionStage[StatementResultCursor]  = session.runAsync(script)

    val result: CompletionStage[Record] = cursorStage.thenCompose(f => f.nextAsync())

    val salida: CompletionStage[(Vector[Promocion], List[String])] = result.thenApply( (record: Record) => {
      val record_fetch = Vector(new Promocion(record.get("a").asString(), record.get("b").asString(), record.get("c").asInt(), record.get("d").asString(), record.get("e").asString(), List(record.get("f").asString())))
      //println (" Procesando CompletionStage--- de " + date.toString + " en " + durd2.toString)
      session.closeAsync()
      (record_fetch, List(2.toString))
    })
    toScala(salida)

  }

}