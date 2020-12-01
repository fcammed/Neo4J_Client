package com.vector;

import com.vector.repo.RepoFile;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.neo4j.driver.*;

import org.neo4j.driver.Config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.neo4j.driver.Config.TrustStrategy.trustAllCertificates;

public class LauncherExplo {
	static String bloque_optimo = "1000";
	static int  numero_bloques = 0;
	static int connTimeOut = 45;

	public LauncherExplo(String[] args, String total,Boolean test, String streaming_mode, String payload, int numero_workers , int workerId, String entono, String sobreescribir, String usu, String pass, String rutaArchivo, String disco, String opt) {
		if (test)
			calc_test(args[0], total, entono);
		else {
			calc_completo(args[0], total, streaming_mode, payload, numero_workers, workerId, entono, sobreescribir, usu, pass, rutaArchivo, disco, opt);
		}
	}

	public static void main(String[] args) {
		String mode;
		if (args.length == 0) {
			mode = "mio";
		} else {
			mode = args[0];
		}

/*		String pass = "";
		Options options = new Options();
		options.addOption(new Option("c", true, "Tamaño pagina"));
		DefaultParser clp = new DefaultParser();
		try {
			CommandLine cl = clp.parse(options, args);
			if (cl.getOptionValue("c") != null) pass = cl.getOptionValue("c");
		} catch (Exception e) {

		}*/
		numero_bloques = 0;
		calc_completo(mode,"10000","false" ,"7",1,1, "10.202.10.64","0","neo4j","vector-itcgroup", "", "si","true");
	}

	public static void calc_completo(String arg, String total, String streaming_mode , String payload, int numero_workers , int workerId, String entorno, String sobreescribir, String usu, String pass, String rutaArchivo, String disco, String opt) {
		String mode = arg;
		String l_bloque_optimo = bloque_optimo;
		if (!total.equals("")) l_bloque_optimo = total;
		if (mode.equals("")) mode = "mio";
		if (streaming_mode.equals("")) streaming_mode = "true";
		if (payload.equals("")) payload = "1";
		System.out.println("Explo Completo -> Arg: " + mode + " ;Bloque:" + l_bloque_optimo + " ;Streaming_mode:" + streaming_mode+ " ;payload:" + payload
				+ " ;numero workers:" + numero_workers+ " ;workerId:" + workerId+ " ;entorno:" + entorno+ " ;sobreescritura:" + sobreescribir+ " ;disco:" + disco+ " ;opt:" + opt);

		Driver driver = getDriver(entorno,usu, pass);
		Session session = driver.session();

		//Llama al Paginado
		// y crear Arrays con los Id de las páginas
		Result resultP = session.run("call eci.getBarrasPagination(" + l_bloque_optimo + ")");
		List<Long> inicioPagina = new ArrayList<Long>();
		List<Long> finPagina = new ArrayList<Long>();
		while(resultP.hasNext()) {
			Record rec = resultP.next();
			Double datos = rec.get("startId").asDouble();
			Double pagfin = rec.get("endId").asDouble();
			inicioPagina.add(datos.longValue());
			finPagina.add(pagfin.longValue());
		}
		//System.out.println("List de nodos páginas Inicio:" + inicioPagina);
		//System.out.println("List de nodos páginas Fin:" + finPagina);

		long iniTraversal = System.currentTimeMillis();
		System.out.println("Llamando al primero");


		Long cuenta = 0L;
		Long promos_parcial = 0L;
		Long promos = 0L ;
		int cuenta_niveles0 = 0;
		int cuenta_niveles0y1 = 0;
		int contador_candopt_niveles0 =0;
		int contador_candopt_niveles0y1 =0;
		Long promos_candopt_parcial = 0L;
		Long promos_candopt = 0L;

		RepoFile flog = new RepoFile(0,",", workerId, rutaArchivo);
		flog.abreFicheroW();
		flog.writeLine(new ArrayList<String>(Arrays.asList("Explo Completo -> Arg: " + mode ,
				"Bloque:" + l_bloque_optimo ,
				"Streaming_mode:" + streaming_mode ,
				"payload:" + payload,
				"numero workers:" + numero_workers,
				"workerId:" + workerId,
				"entorno:" + entorno,
				"sobreescritura:" + sobreescribir,
				"disco:" + disco,
				"opt:" + opt)));
		String desarrollo="";
		if (payload.equals("6")) {
			//Se tira contra versión de desarrollo pero equivalente a payload 1 de la PoC PCM
			desarrollo = "1";
			payload = "1";
		} else if (payload.equals("7")) {
			//Se tira contra versión de desarrollo pero equivalente a payload 5 de la PoC PCM
			desarrollo = "2";
			payload = "5";
		} else {
			desarrollo = "";
		}
		int formula = inicioPagina.size()/numero_workers;
		int searchMiBloque =0;
		int miBloque = workerId;
		ArrayList<List<String>> lcdP = new ArrayList<List<String>>();
		for(Long nodeId : inicioPagina){
			//Procesa Página
			long iniLlamada = System.currentTimeMillis();
			cuenta = cuenta + 1;
			searchMiBloque = searchMiBloque +1;
			//if (cuenta > (workerId - 1) * formula && (cuenta <= workerId*formula || workerId == numero_workers)) {

			//Cada Worker recorre todo el Array de página
			//Pero, sólo procesa las páginas que le corresponde
			//(Sin son 3 workers, cada 3 páginas
			if (searchMiBloque == miBloque) { //este Bloque es mio
				miBloque = numero_workers;
				searchMiBloque =0;
				RepoFile fdatos;
				if (sobreescribir.equals("1")) //Sobreescribe siempre el mismo fichero para no ocupar espacio en disco
					fdatos = new RepoFile(workerId, "|", workerId, rutaArchivo);
				else
					fdatos = new RepoFile(cuenta, "|", workerId, rutaArchivo);
				fdatos.abreFicheroW();
				boolean prueba = true; 	// prueba true , ejecuta versión 'Cypher streaming'
										// prueba false, ejecuta versión 'iterar nodos seguido de ...'
				if (total.equals("T"))
					//se ejecuta desde este main
					prueba = true;
				else {
					// se ejecuta desde HelloWorld
					if (streaming_mode.equals("false"))
						prueba = false;
					else
						prueba = true;
				}
				Result resultCa;
				if (prueba) {
					if (!desarrollo.equals(""))
						resultCa = session.run("MATCH (c:BARRA)-[:SEGUIDO_DE*0.." + l_bloque_optimo + "]->(p) where ID(c) = " + nodeId.toString() + " CALL eci.exploitCommoditiesNode(toString(id(p)),'all','20190417','" + desarrollo + "') YIELD data  RETURN data as a");
					else
						resultCa = session.run("MATCH (c:BARRA)-[:SEGUIDO_DE*0.." + l_bloque_optimo + "]->(p) where ID(c) = " + nodeId.toString() + " CALL eci.exploitCommoditiesNode(toString(id(p)),'all','20190417','" + payload + "') YIELD data  RETURN data as a");
				} else {
					if (!desarrollo.equals(""))
						resultCa = session.run(" CALL eci.exploitCommodities('" + nodeId.toString().trim() + "','" + finPagina.get(cuenta.intValue() - 1).toString().trim() + "','all','20190417','" + desarrollo + "') YIELD data  RETURN data as a");
					else
						resultCa = session.run(" CALL eci.exploitCommodities('" + nodeId.toString().trim() + "','" + finPagina.get(cuenta.intValue() - 1).toString().trim() + "','all','20190417','" + payload + "') YIELD data  RETURN data as a");
				}
				String datos = "";
				Map datosM;
				String Barra;
				String Divisio;
				int parcial = 0;
				int cuenta_barras = 0;
				int cuenta_niveles0y1_parcial = 0;
				int cuenta_niveles0_parcial = 0;
				int contador_candopt_niveles0_parcial =0;
				int contador_candopt_niveles0y1_parcial =0;
				boolean ventana = false;
				boolean candidato = false;

				long primerResult = System.currentTimeMillis();
				//Procesa el ResulSet
				while (resultCa.hasNext()) {
					Record result = resultCa.next();
					candidato = false;
					if (true ) { //Me permite activar y desactivar rápidamente toda la lógica de cada registro
						String barra = (String) result.get("a").asMap().get("nodeId");
						cuenta_niveles0_parcial = cuenta_niveles0_parcial + 1;
						if (payload.equals("4") || payload.equals("5")) {
							//Lee la estructura optimizada
							candidato = true;
							List<String> lpromos_directas = ((List<String>) result.get("a").asMap().get("directPromotions"));
							List<String> lpromos_combinadas = ((List<String>) result.get("a").asMap().get("combinedPromotions"));
							List<Map> ltallas = ((List<Map>) result.get("a").asMap().get("tallas"));
							ArrayList<List<String>> lpdg = new ArrayList<List<String>>();
							ArrayList<List<String>> lpcg = new ArrayList<List<String>>();
							//recupero lista de promos combinadas
							for (String promo : lpromos_combinadas
							) {
								List<String> lista = Arrays.asList(promo.split(Pattern.quote("|")));
								lpcg.add(lista);
								candidato = false;
							}
							String cdP = (String)result.get("a").asMap().get("copiedDirectPromotions");
							if (cdP.equals("false")) {
								//recuperamos del ResulSet lista de promos directas
								for (String promo : lpromos_directas
								) {
									List<String> lista = Arrays.asList(promo.split(Pattern.quote("|")));
									lpdg.add(lista);
								}
								lcdP = lpdg;
							} else {
								//ldcP ya tiene el valor de la última válida
							}

							if (ltallas == null || ltallas.size() == 0) {
								//No ha tallas escribimos todas las promos asociadas a Barra
								if (opt.equals("true")) {
									if (!disco.equals("no")&& !(ventana && candidato && cdP.equals("true"))) fdatos.writeLineofList("P", lcdP,lpcg, barra); //escribe directas  y combinadas asociada a Barra
									if (!disco.equals("no") && (ventana && candidato && cdP.equals("true"))) fdatos.writeLine(new ArrayList<>(Arrays.asList("B",barra))); //escribe sólo la Barra
								} else {
									if (!disco.equals("no")) fdatos.writeLineofList("P", lcdP,lpcg, barra); //escribe directas  y combinadas asociada a Barra
								}
								promos_parcial = promos_parcial + lcdP.size() + lpcg.size(); //contabiliza número de promos
								if (ventana && candidato && cdP.equals("true")) promos_candopt_parcial = promos_candopt_parcial + lcdP.size() + lpcg.size();
								cuenta_niveles0y1_parcial = cuenta_niveles0y1_parcial + 1;
								if (ventana && candidato && cdP.equals("true")) contador_candopt_niveles0y1_parcial = contador_candopt_niveles0y1_parcial + 1;
							} else {
								//Hay Tallas, se escribe la lista de todas las promos para cada Talla
								for (Map ltalla : ltallas
								) {
									String talla = (String) ltalla.get("nodeId");
									List<String> lpromosTalla = ((List<String>) ltalla.get("promotions"));

									ArrayList<List<String>> lpt = new ArrayList<List<String>>();
									//Recupera lista de promos de Talla
									for (String promoTalla : lpromosTalla
									) {
										List<String> lista = Arrays.asList(promoTalla.split(Pattern.quote("|")));
										lpt.add(lista);
										candidato = false;
									}
									if (opt.equals("true")) {
										if (!disco.equals("no") && !(ventana && candidato && cdP.equals("true"))) fdatos.writeLineofList("P",lcdP, lpcg, lpt, talla); //escribe directas, combinadas y las de tallas, asociada a Talla
									} else {
										if (!disco.equals("no")) fdatos.writeLineofList("P",lcdP, lpcg, lpt, talla); //escribe directas, combinadas y las de tallas, asociada a Talla
									}

									promos_parcial = promos_parcial + lcdP.size() + lpcg.size() + lpt.size(); //contabiliza número de promos
									cuenta_niveles0y1_parcial = cuenta_niveles0y1_parcial + 1;
									if (ventana && candidato && cdP.equals("true")) promos_candopt_parcial = promos_candopt_parcial + lcdP.size() + lpcg.size() + lpt.size();
									if (ventana && candidato && cdP.equals("true")) contador_candopt_niveles0y1_parcial = contador_candopt_niveles0y1_parcial + 1;
								}
								if (opt.equals("true")) {
									if (!disco.equals("no") && (ventana && candidato && cdP.equals("true"))) {
										ArrayList<String> lista_tallas = new ArrayList<>();
										lista_tallas.add("T");
										for (Map ltalla : ltallas
										) {
											String talla = (String) ltalla.get("nodeId");
											lista_tallas.add(talla);
										}
										fdatos.writeLine(lista_tallas); //escribe una línea sólo con las tallas.
									}
								}
							}

							if (ventana && candidato && cdP.equals("true")) contador_candopt_niveles0_parcial = contador_candopt_niveles0_parcial + 1;
							ventana = candidato;

						} else { //estructura Mapper
							//No escribimos en este caso
							datosM = result.get("a").asMap();
							datos = datosM.get("nodeId").toString();
							promos_parcial = promos_parcial + 1;
						}
						cuenta_barras = cuenta_barras + 1;
						if (cuenta_barras == 1) primerResult = System.currentTimeMillis();

					}
				}
				promos = promos + promos_parcial;
				promos_candopt = promos_candopt + promos_candopt_parcial;
				cuenta_niveles0 = cuenta_niveles0 + cuenta_niveles0_parcial;
				cuenta_niveles0y1 = cuenta_niveles0y1 + cuenta_niveles0y1_parcial;
				contador_candopt_niveles0 = contador_candopt_niveles0 + contador_candopt_niveles0_parcial;
				contador_candopt_niveles0y1 = contador_candopt_niveles0y1 + contador_candopt_niveles0y1_parcial;
				long finLlamada = System.currentTimeMillis();
				long media = 0;
				if (promos_parcial != 0)
					media = promos_parcial / cuenta_niveles0y1_parcial;
				System.out.println("Tiempo empleado en Llamada de " + String.valueOf(cuenta) +
						": primero: " + (primerResult - iniLlamada) +
						" total: " + (finLlamada - iniLlamada) + " milisegundos, promos: " + promos_parcial +
						", media de promos por item: " + media +
						", barras: " + cuenta_niveles0_parcial +
						", items: " + cuenta_niveles0y1_parcial +
						", Opt_tamaño_fragmento(b,i,lineas_p): (" +
						contador_candopt_niveles0_parcial + "," + contador_candopt_niveles0y1_parcial+ "," + promos_candopt_parcial + ")");
				flog.writeLine(new ArrayList<String>(Arrays.asList("Bloque n: " + String.valueOf(cuenta),
						"Tiempo primero: " + (primerResult - iniLlamada) + " milisegundos",
						"Tiempo total: " + (finLlamada - iniLlamada) + " milisegundos",
						"Promos: " + promos_parcial,
						"Media de promos por item: " + media,
						"Barras: " + cuenta_niveles0_parcial,
						"Items: " + cuenta_niveles0y1_parcial,
						", Opt_tamaño_fragmento(b,i,lineas_p): (" +
								contador_candopt_niveles0_parcial + "," + contador_candopt_niveles0y1_parcial+ "," + promos_candopt_parcial + ")"
						)));
				promos_parcial = 0L;
				promos_candopt_parcial = 0L;
				cuenta_niveles0_parcial = 0;
				cuenta_niveles0y1_parcial =0;
				fdatos.cierraFichero();
				if (numero_bloques > 0 && cuenta == numero_bloques) break;
			}
		}
		long finTraversal = System.currentTimeMillis();
		System.out.println("Tiempo empleado en traversal completo: " + (finTraversal - iniTraversal)  + " milisegundos, promos: " + promos + ", barras: " + cuenta_niveles0+ ", items: " + cuenta_niveles0y1 +
				", Opt_tamaño_fragmento(b,i,lineas_p): (" +
				contador_candopt_niveles0 + "," + contador_candopt_niveles0y1+ "," + promos_candopt + ")");
		flog.writeLine(new ArrayList<String>(Arrays.asList(
				"Tiempo empleado en traversal completo: " + (finTraversal - iniTraversal)+ " milisegundos",
				"Promos: " + promos,
				"Barras: " + cuenta_niveles0,
				"Items: " + cuenta_niveles0y1,
				", Opt_tamaño_fragmento(b,i,lineas_p): (" +
						contador_candopt_niveles0 + "," + contador_candopt_niveles0y1+ "," + promos_candopt + ")")));
		flog.cierraFichero();
		session.close();
		driver.close();
	}

	public static void calc_test(String arg, String total, String entorno) {
		String mode = arg;
		if (mode.equals("")) mode = "mio";
		System.out.println("Args: " + mode);

		Driver driver = getDriver(entorno);
		Session session = driver.session();

		Result resultP = session.run("CALL eci.loadCaches(\"mio\")");
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
			Result resultCa = session.run("MATCH (c:BARRA)-[:SEGUIDO_DE*0.." + bloque_optimo + "]->(p) where ID(c) = 224 CALL eci.exploitCommoditiesNode(toString(id(p)),'all','20190417') YIELD data  RETURN data as a");
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

	public static Driver getDriver(String entorno, String usu, String pass){

		if (entorno.equals("1"))
			return GraphDatabase.driver("bolt://10.202.10.64:7687",AuthTokens.basic( "neo4j", "vector-itcgroup" ), Config.builder()
					.withConnectionTimeout( 15, TimeUnit.MINUTES )
					.withMaxConnectionLifetime( connTimeOut, TimeUnit.MINUTES )
					.withMaxConnectionPoolSize(100)
					.build());
		else
			if (entorno.equals("2"))
			//return GraphDatabase.driver("bolt://localhost:7687",AuthTokens.basic( "neo4j", "pepito" ));
			return GraphDatabase.driver("bolt://localhost:7687", Config.builder()
							.withConnectionTimeout( 15, TimeUnit.MINUTES )
							.withMaxConnectionLifetime( connTimeOut, TimeUnit.MINUTES )
							.withMaxConnectionPoolSize(100)
							.build()
			);
			else
				return GraphDatabase.driver("bolt://" + entorno + ":7687",AuthTokens.basic( usu, pass ), Config.builder()
								.withConnectionTimeout( 15, TimeUnit.MINUTES )
								.withMaxConnectionLifetime( connTimeOut, TimeUnit.MINUTES )
								.withMaxConnectionPoolSize(100)
								//.withFetchSize(1)
								.build());
	}
	public static Driver getDriver(String entorno){

		if (entorno.equals("1"))
			return GraphDatabase.driver("bolt://10.202.10.64:7687",AuthTokens.basic( "neo4j", "vector-itcgroup" ));
		else
			//return GraphDatabase.driver("bolt://localhost:7687",AuthTokens.basic( "neo4j", "pepito" ));
			return GraphDatabase.driver("bolt://localhost:7687", Config.builder()
					.withConnectionTimeout( 15, TimeUnit.MINUTES )
					.withMaxConnectionLifetime( connTimeOut, TimeUnit.MINUTES )
					.withMaxConnectionPoolSize(100)
					.build());

	}
}
