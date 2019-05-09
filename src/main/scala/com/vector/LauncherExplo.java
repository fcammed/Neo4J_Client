package com.vector;

import org.neo4j.driver.internal.value.MapValue;
import org.neo4j.driver.v1.*;
import sun.net.www.http.HttpClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class LauncherExplo {
	static String bloque_optimo = "1000";

	public LauncherExplo(String[] args, String total,Boolean test, String streaming_mode, String payload) {
		if (test)
			calc_test(args[0], total);
		else
			calc_completo(args[0], total, streaming_mode, payload);
	}

	public static void main(String[] args) {
		String mode;
		if (args.length == 0) {
			mode = "mio";
		} else {
			mode = args[0];
		}
		calc_completo(mode,bloque_optimo,"true","5");
	}

	public static void calc_completo(String arg, String total, String streaming_mode , String payload) {
		String mode = arg;
		String l_bloque_optimo = bloque_optimo;
		if (!total.equals("")) l_bloque_optimo = total;
		if (mode.equals("")) mode = "mio";
		if (streaming_mode.equals("")) streaming_mode = "true";
		if (payload.equals("")) payload = "1";
		System.out.println("Explo Completo -> Arg: " + mode + " ;Bloque:" + l_bloque_optimo + " ;Streaming_mode:" + streaming_mode+ " ;payload:" + payload);

		Driver driver = getDriver();
		Session session = driver.session();

		StatementResult resultP = session.run("call eci.getBarrasPagination(" + l_bloque_optimo + ")");
		List<Long> inicioPagina = new ArrayList<Long>();
		List<Long> finPagina = new ArrayList<Long>();
		//finish = resultP.single().get("atEnd", false);
		while(resultP.hasNext()) {
			Record rec = resultP.next();
			Double datos = rec.get("startId").asDouble();
			Double pagfin = rec.get("endId").asDouble();
			inicioPagina.add(datos.longValue());
			finPagina.add(pagfin.longValue());
		}
		//System.out.println("List de nodos páginas:" + inicioPagina);

		long iniTraversal = System.currentTimeMillis();
		System.out.println("Llamando al primero");
		//StatementResult resultCa = session.run("MATCH (c:BARRA)-[:SEGUIDO_DE*0..10000]->(p) where ID(c) = 224 CALL eci.exploitCommoditiesNode(toString(id(p)),'all','20190417') YIELD data, errors  RETURN data as a, errors as b");
		//TEST
		//String bloque_optimo = "10000";
		//int veces = new Integer(total) / new Integer(bloque_optimo);
		Long cuenta = 0L;
		Long promos_parcial = 0L;
		Long promos = 0L ;
		for(Long nodeId : inicioPagina){
			//while (veces > 0) {
			long iniLlamada = System.currentTimeMillis();
			boolean prueba = true; // prueba true , ejecuta versión 'Cypher streaming'
			// prueba false, ejecuta versión agregate
			if (total.equals("T"))
				//se ejecuta desde este main
				prueba = true;
			else {
				// se ejecuta desde HelloWorld y se usa el segundo parámetro
				if (streaming_mode.equals("false"))
					prueba = false;
				else
					prueba = true;
			}
			StatementResult resultCa;
			if (prueba) {
				resultCa = session.run("MATCH (c:BARRA)-[:SEGUIDO_DE*0.." + bloque_optimo + "]->(p) where ID(c) = " + nodeId.toString() + " CALL eci.exploitCommoditiesNode(toString(id(p)),'all','20190417','" + payload + "') YIELD data  RETURN data as a");
			} else {
				resultCa = session.run(" CALL eci.exploitCommodities('" + nodeId.toString().trim() + "','" + finPagina.get(cuenta.intValue()).toString().trim() + "','all','20190417','" + payload + "') YIELD data  RETURN data as a");
			}
			String datos;
			int parcial = 0;
			while (resultCa.hasNext()) {
				datos = resultCa.next().get("a").toString();
				promos_parcial = promos_parcial +1;
				if (promos_parcial==0) System.out.println("DATOS: " + datos);
			}
			cuenta = cuenta + 1;
			promos = promos+ promos_parcial;
			long finLlamada = System.currentTimeMillis();
			System.out.println("Tiempo empleado en Llamada de " + String.valueOf(cuenta) +": " + (finLlamada - iniLlamada)  + " milisegundos, promos: " + promos_parcial);
			promos_parcial = 0L;
		}
		long finTraversal = System.currentTimeMillis();
		System.out.println("Tiempo empleado en traversal completo: " + (finTraversal - iniTraversal)  + " milisegundos, promos: " + promos);
		session.close();
		driver.close();
	}

	public static void calc_test(String arg, String total) {
		String mode = arg;
		if (mode.equals("")) mode = "mio";
		System.out.println("Args: " + mode);

		Driver driver = getDriver();
		Session session = driver.session();

		StatementResult resultP = session.run("CALL eci.loadCaches(\"mio\")");
		List<Double> paginas = new ArrayList<Double>();
		//finish = resultP.single().get("atEnd", false);
		while(resultP.hasNext()) {
			Record rec = resultP.next();
			Double datos = rec.get("startId").asDouble();
			paginas.add(datos);
		}

		long iniTraversal = System.currentTimeMillis();
		System.out.println("Llamando al primero");
		//StatementResult resultCa = session.run("MATCH (c:BARRA)-[:SEGUIDO_DE*0..10000]->(p) where ID(c) = 224 CALL eci.exploitCommoditiesNode(toString(id(p)),'all','20190417') YIELD data, errors  RETURN data as a, errors as b");
		//TEST

		int veces = new Integer(total) / new Integer(bloque_optimo);
		int cuenta = 0;
		while(veces >0) {
			StatementResult resultCa = session.run("MATCH (c:BARRA)-[:SEGUIDO_DE*0.." + bloque_optimo + "]->(p) where ID(c) = 224 CALL eci.exploitCommoditiesNode(toString(id(p)),'all','20190417') YIELD data  RETURN data as a");
			String datos;
			int parcial = 0;
			int grupos = 5000;
			int cuenta_grupos = 0;
			if (resultCa.hasNext()) {
				while (resultCa.hasNext()) {
					datos = resultCa.next().get("a").toString();
					cuenta = cuenta + 1;
					parcial = parcial + 1;
					if (parcial == grupos) {
						cuenta_grupos = cuenta_grupos + 1;
						System.out.println("Procesado " + cuenta_grupos * grupos);
						parcial = 0;
					}
				}
			}
			veces = veces -1;
		}

		long finTraversal = System.currentTimeMillis();
		session.close();
		driver.close();
		//long finCacheDiv = System.nanoTime();
		System.out.println("Tiempo empleado en traversal de " + String.valueOf(cuenta) +": " + (finTraversal - iniTraversal)  + " milisegundos");
	}

	public static Driver getDriver(){

		//return GraphDatabase.driver("bolt://10.202.10.64:7688",AuthTokens.basic( "neo4j", "pepito" ));
		//return GraphDatabase.driver("bolt://localhost:7687",AuthTokens.basic( "neo4j", "pepito" ));
		return GraphDatabase.driver("bolt://localhost:7687", Config.build()
				.withConnectionTimeout( 15, TimeUnit.MINUTES )
				.withMaxConnectionLifetime( 30, TimeUnit.MINUTES )
				.withMaxConnectionPoolSize(100)
				.toConfig());

	}
}
