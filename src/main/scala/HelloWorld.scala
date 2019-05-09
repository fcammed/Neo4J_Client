import java.util.concurrent.TimeUnit

import com.vector.{LaunchPromo, Neo4j, bdNeo4j, launchercomo, LauncherExplo}
import org.neo4j.driver.v1.summary.ResultSummary
import org.neo4j.driver.v1.{Config, GraphDatabase}

import scala.concurrent.{Future, blocking}

class Neo4jPoolConnection () {
  val hostneo4j = "localhost"
  val portneo4j =  "7687"
  val poolsizeneo4j =  100
  val explofilepath =  """"C:/DATOSP~1/Vector/PoC/PLAYPR~1/poc-bigfiles-upload/Uploadfiles/explosionado/""""
  val overwrite =  0
  val filewrite = 1
  val poolactors = 1
  val querynumbers = 20
  val queryprimero = 1
  val querylongitud = 10000
  val modelograph = "POC"



  def neo_url(h: String, p: String): String = "bolt://"+ h + ":" + p

  val driver = GraphDatabase.driver(neo_url(hostneo4j, portneo4j), Config.build()
    .withConnectionTimeout( 15, TimeUnit.MINUTES )
    .withMaxConnectionLifetime( 30, TimeUnit.MINUTES )
    .withMaxConnectionPoolSize(poolsizeneo4j)
    .toConfig)
}

object HelloWorld {

  def graphExplogenerationInicial(neo4jpc:Neo4jPoolConnection) = {
    val date = System.currentTimeMillis()

    println("start: delete database")
    val db11 = new Neo4j(neo4jpc.driver, neo4jpc.explofilepath, neo4jpc.modelograph)
    //val resultdr1: ResultSummary = db11.driver.session.run(bdNeo4j.queriesTest("deleteGraphExplo")).consume()
    val resultdr1: ResultSummary = db11.driver.session.run(bdNeo4j.queriesTest("deleteGraphExplo", "0","0",1,1)).consume()
    db11.driver.session().close()
    println("end: delete database")

    println("start: create estructura básica")
    val db3 = new Neo4j(neo4jpc.driver, neo4jpc.explofilepath, neo4jpc.modelograph)
    //val result: ResultSummary = db3.driver.session.run(bdNeo4j.queriesTest("generateGraphExploInicial")).consume()
    val result: ResultSummary = db3.driver.session.run(bdNeo4j.queriesTest("generateGraphExploInicial", "0","0",1,1)).consume()
    db3.driver.session().close()
    println("end: create estructura básica")
    println("Database básica creada en " + (System.currentTimeMillis()-date).toString + "ms")
  }

  def graphExplogenerationECI(neo4jpc:Neo4jPoolConnection, prueba: String, ntotal: Int, totalcall: Int) = {
    //ntotal -> número total de nodos esperado que se desea crear
    //cuantos -> número de ejecuciones
    //totalcall -> número de nodos en cada ejecución
    val cuantos: Int = if ((ntotal % totalcall)>0 )( ntotal / totalcall ) +1 else ( ntotal / totalcall )
    println("exploECI: cuantos:" + cuantos)
    val date = System.currentTimeMillis()
    var total = totalcall //nodos en cada ejecuciòn
    val commits = 10000 //cantidad de nodos en los que se hace commit
    0 to (cuantos -1 ) map {i =>
      if ((i * totalcall)> ntotal) {
        total = ntotal - ((i - 1 ) * totalcall)
      }
      val datei = System.currentTimeMillis()
      println("start: create nodes producto " + i + " ;->" + "call eci.gengraphECIproductos(" + ((totalcall *i)+1).toString + "," + prueba + "," + total + "," + commits + ")")
      val dbp = new Neo4j(neo4jpc.driver, neo4jpc.explofilepath, neo4jpc.modelograph)
      dbp.driver.session.run("call eci.gengraphECIproductos(" + ((totalcall *i)+1).toString + "," + prueba + "," + total + "," + commits + ")").consume()
      dbp.driver.session().close()
      println("end: create nodes producto " + i +", en " + (System.currentTimeMillis()-datei).toString + "ms")
    }
    println("Database creada en " + (System.currentTimeMillis()-date).toString + "ms")
  }
  //--------------------------------------------------------------------------------------------

  def graphExplogenerationB(neo4jpc:Neo4jPoolConnection) = {
    val date = System.currentTimeMillis()

    println("start: delete relations1")
    val db11 = new Neo4j(neo4jpc.driver, neo4jpc.explofilepath, neo4jpc.modelograph)
    val resultdr1: ResultSummary = db11.driver.session.run(bdNeo4j.qExplosionado("deleteExploRelations1","","0")).consume()
    db11.driver.session().close()
    println("end: delete relations1")

    println("start: delete relations2")
    val db12 = new Neo4j(neo4jpc.driver, neo4jpc.explofilepath, neo4jpc.modelograph)
    val resultdr2: ResultSummary = db12.driver.session.run(bdNeo4j.qExplosionado("deleteExploRelations2","","0")).consume()
    db12.driver.session().close()
    println("end: delete relations2")

    println("start: delete nodes")
    val db2 = new Neo4j(neo4jpc.driver, neo4jpc.explofilepath, neo4jpc.modelograph)
    val resultdn: ResultSummary = db2.driver.session.run(bdNeo4j.qExplosionado("deleteExploNodes","","0")).consume()
    db2.driver.session().close()
    println("end: delete nodes")

    println("start: create estructura básica")
    val db3 = new Neo4j(neo4jpc.driver, neo4jpc.explofilepath, neo4jpc.modelograph)
    val result: ResultSummary = db3.driver.session.run(bdNeo4j.qExplosionado("generateGraphExploEstruc","","0")).consume()
    db3.driver.session().close()
    println("end: create estructura básica")
    println("Database básica creada en " + (System.currentTimeMillis()-date).toString + "ms")
  }

  def graphExplogeneration(neo4jpc:Neo4jPoolConnection, prueba: String, cuantos: Int) = {
      val date = System.currentTimeMillis()

    val total = 100000
    val commits = 10000

      //precarga primer millón de datos del grafo tipo 1
    0 to (cuantos -1 ) map {i =>
      val datei = System.currentTimeMillis()
      println("start: create nodes producto " + i)
      val dbp = new Neo4j(neo4jpc.driver, neo4jpc.explofilepath, neo4jpc.modelograph)
      if (prueba.equals("1"))
      //dbp.driver.session.run(bdNeo4j.qExplosionado("generateGraphExploProductos1", (3 + 100000*i).toString,"0")).consume()
        dbp.driver.session.run(bdNeo4j.queriesTest("generateGraphExploProductos1", (3 + 100000*i).toString,"0",total,commits)).consume()
      else
      //dbp.driver.session.run(bdNeo4j.qExplosionado("generateGraphExploProductos2", (3 + 100000*i).toString,"0")).consume()
        dbp.driver.session.run(bdNeo4j.queriesTest("generateGraphExploProductos2", (3 + 100000*i).toString,"0",total,commits)).consume()
      dbp.driver.session().close()
      println("end: create nodes producto " + i +", en " + (System.currentTimeMillis()-datei).toString + "ms")
    }
      //precarga segundo millón de datos del grafo tipo 2
      /*10 to 309 map {i =>
        println("start: create nodes producto " + i)
        val dbp = new Neo4j(neo4jpc.driver, neo4jpc.explofilepath)
        if (prueba.equals("1"))
          dbp.driver.session.run(bdNeo4j.queriesTest("generateGraphExploProductos1", (3 + 100000*i).toString,"0",total,commits)).consume()
        else
          dbp.driver.session.run(bdNeo4j.queriesTest("generateGraphExploProductos2", (3 + 100000*i).toString,"0",total,commits)).consume()
        dbp.driver.session().close()
        println("end: create nodes producto " + i )
      }*/
      println("Database creada en " + (System.currentTimeMillis()-date).toString + "ms")
  }

  def graphCreateIndex(neo4jpc:Neo4jPoolConnection) = {
    val date = System.currentTimeMillis()
    println("start: create index")
    val db4 = new Neo4j(neo4jpc.driver, neo4jpc.explofilepath, neo4jpc.modelograph)
    val resultci: ResultSummary = db4.driver.session.run(bdNeo4j.qExplosionado("createExploIndex","","0")).consume()
    db4.driver.session().close()
    println("end: create index")
    println("Database creada en " + (System.currentTimeMillis()-date).toString + "ms")
  }

  def main(args: Array[String]): Unit = {
    //args.foreach(arg => {println(arg)})
    val mode = args(0)
    val agrupando = if (args.length >= 2) if ((args(1)) == null) "10000" else args(1) else "10000"
    val streaming_mode = if (args.length >= 3) if ((args(2)) == null) "true" else args(2) else "true"
    val payload = if (args.length >= 4) if ((args(3)) == null) "1" else args(3) else "1"

    val neo4jpc: Neo4jPoolConnection = new Neo4jPoolConnection()
    val db = new Neo4j(neo4jpc.driver, neo4jpc.explofilepath, neo4jpc.modelograph)
    val formatter = java.text.NumberFormat.getIntegerInstance()
    def gettotal(index: Int) = formatter.format(db.getNumberNodes(index))
    val total_mod1 = gettotal(1)
    println("Numero de nodos " + total_mod1)
    if (mode.equals("eciB")) graphExplogenerationInicial(neo4jpc)
    if (mode.equals("eciG")) graphExplogenerationECI(neo4jpc,"1",8750000,33333) //263
    if (mode.equals("eciT")) graphExplogenerationECI(neo4jpc,"1",333330,33333) //10
    if (mode.equals("1")) graphExplogenerationB(neo4jpc)
    if (mode.equals("2")) graphExplogeneration(neo4jpc,"1",155)
    if (mode.equals("4")) graphExplogeneration(neo4jpc,"2",155)
    if (mode.equals("Ind")) graphCreateIndex(neo4jpc)
    if (mode.equals("T")) {
      println("Iniciando Commodities")
      val como = new launchercomo(args,"T")
      println("Iniciando Promociones")
      val promo = new LaunchPromo(args,"T")
    }
    if (mode.equals("Tc")) new launchercomo(args,"T")
    if (mode.equals("Pc1") || mode.equals("Pc2")) new launchercomo(args,"P")
    if (mode.equals("Tp")) new LaunchPromo(args,"T")
    if (mode.equals("Pp1") || mode.equals("Pp2")) new LaunchPromo(args,"P")
    if (mode.equals("ExpT")) new LauncherExplo(args,agrupando,true, streaming_mode, payload)
    if (mode.equals("ExpC")) new LauncherExplo(args,agrupando,false, streaming_mode, payload)
    if (mode.equals("help")) {
      println("Argumentos:");
      println("\"eciB\" - Borra el grafo actual y regenera los nodos básicos del grafo de prueba ECI");
      println("\"eciG\" - Añade al grafo base, nodos de prueba con la volumetría y distribución total de ECI");
      println("\"eciT\" - Añade al grafo base, un conjunto reducido de nodos de prueba ECI");
      println("\"1\" - Borra el grafo actual y regenera los nodos básicos del grafo de prueba PoC");
      println("\"2\" - Añade al grafo base, nodos de prueba del modelo PoC de tipo 1");
      println("\"4\" - Añade al grafo base, nodos de prueba del modelo PoC de tipo 2");
      println("\"Ind\" - Crea Índice");
      println("\"T\" - Importacion completa; Primero commodities y después Promociones");
      println("\"Tc\" - Importa commodities completo");
      println("\"Pc1\" o \"Pc2\" - Importa commodities parcial");
      println("\"Tp\" - Importa promociones completo");
      println("\"Pp1 o \"Pp2\" - Importa promociones parcial");
      println("\"ExpT\" \"tamaño\"- Explosiona un número de Barras en modo Test (repitiendo los n-'bolque_optima primeras Barras)");
      println("                   - Parámetro \"tamaño\" determina la cantidad");
      println("\"ExpC\" \"tamaño_pagina\"  \"streaming_mode\" \"payload\" - Explosionado Completo de todas las Barras, recorriendo la lista de Barras según paginación.");
      println("                          - Parámetro \"tamaño_pagina\": tamaño de página");
      println("                          - Parámetro \"streaming_mode\": true->ejecuta versión 'Cypher streaming', false->la Original, que agrega antes de responder");
      println("                          - Parámetro \"payload\" 1->Mapper, 2->Map a mano, 3->List a mano, 4->String entre comillas, 5->String con separador |6->String Fijo largo (14 promos), 7->Texto corto prueba");
    }
    val total_mod2 = gettotal(1)
    println("Numero de nodos " + total_mod2)
    neo4jpc.driver.close()
  }
}