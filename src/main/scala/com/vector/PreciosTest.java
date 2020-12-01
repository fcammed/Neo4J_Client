package com.vector;

import org.neo4j.driver.Driver;
import org.neo4j.driver.*;
import org.neo4j.driver.async.AsyncSession;
import org.neo4j.driver.internal.shaded.reactor.core.publisher.Flux;
import org.neo4j.driver.internal.shaded.reactor.core.publisher.Mono;
import org.neo4j.driver.reactive.RxResult;
import org.neo4j.driver.reactive.RxSession;
import org.neo4j.driver.summary.ResultSummary;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.vector.LaunchPromo.getDriver;
import static java.lang.Thread.sleep;

public class PreciosTest {

	public static void main(String[] args) {
		String nodos_por_llamada = "100000", modo="1";
		long numero_de_llamadas = 10;
		if (args.length==3) {
			numero_de_llamadas = new Long(args[0]);
			nodos_por_llamada = args[1];
			modo=args[2];
		}
		//precios_test(args,numero_de_llamadas,nodos_por_llamada, modo);
		test_error_unparseable_date(args);
		return;
		//stream_test(args);
	}

	public PreciosTest(String[] args, long numero_de_llamadas, String nodos_por_llamada, String modo) {
		precios_test(args, numero_de_llamadas,nodos_por_llamada, modo);
	}

	public static void stream_test(String[] args) {

		String mode;
		if (args.length == 0) {
			mode = "mio";
		} else {
			mode = args[0];
		}
		System.out.println("Args: " + mode);

		Driver driver = LauncherExplo.getDriver("localhost", "neo4j", "vector-itcgroup");
		int cuenta=0;
		long iniTraversal = System.currentTimeMillis();
		//prueba_driver_async(driver,iniTraversal);
		prueba_driver(driver,iniTraversal);
		driver.close();
	}

	private static void prueba_driver (Driver driver, long iniTraversal) {
		Session session = driver.session();
		long finTotal1 = System.currentTimeMillis();
		System.out.println("Tiempo Total: " + (finTotal1 - iniTraversal) + " milisegundos ");
		Result result = session.run("CALL eci2.querystream2(\"00B18\",\"50000000\",\"0\")");
		if (result.hasNext()) {
			long finTotal2 = System.currentTimeMillis();
			System.out.println("Tiempo Total: " + (finTotal2 - iniTraversal) + " milisegundos ");
		}
		Stream<Record> st = result.stream();
		st.forEach(record -> imprime_record( record, iniTraversal));//map(record -> imprime_record( record, iniTraversal));
		System.out.println("Summary, tiempo inicial: " + result.consume().resultAvailableAfter(TimeUnit.MILLISECONDS) + " ,tiempo final: " + result.consume().resultConsumedAfter(TimeUnit.MILLISECONDS) );;
		//try{sleep(30000);} catch (InterruptedException e){};
		long finTotal = System.currentTimeMillis();
		System.out.println("---------------------------------------------------------------------------------------");
		System.out.println("Tiempo Total: " + (finTotal - iniTraversal) + " milisegundos ");
	}

	private static void prueba_driver_async (Driver driver, long iniTraversal) {
		AsyncSession session = driver.asyncSession();
		CompletionStage resultCa = session.runAsync("CALL eci2.querystream2(\"00B18\",\"10\",\"100\")")
				//.thenApplyAsync( record -> imprime_record( record, iniTraversal));
				//.records().subscribe((record -> imprime_record( record, iniTraversal)));
				.thenApplyAsync( cursor -> cursor.forEachAsync(( record -> imprime_record( record, iniTraversal))));

		//StatementResult resultCa = session.run("CALL example.readStream(\"00B18\")");
		try{sleep(30000);} catch (InterruptedException e){};
		long finTotal = System.currentTimeMillis();
		System.out.println("---------------------------------------------------------------------------------------");
		System.out.println("Tiempo Total: " + (finTotal - iniTraversal) + " milisegundos ");
	}

	private static Record imprime_record (Record resultRe, long iniTraversal) {
		String name = resultRe.get("name").toString();
		String tipo = resultRe.get("tipo").toString();
		String desc = resultRe.get("desc").toString();
		String rel = resultRe.get("rel").toString();
		String cen = resultRe.get("cen").toString();
		//cuenta = cuenta + 1;
		long finTraversal = System.currentTimeMillis();
		System.out.println("Registro time:" + (finTraversal - iniTraversal)  + " milisegundos -> name:" + name + " tipo:" + tipo + " desc:" + desc + " rel:" + rel + " cen:" + cen);
		return resultRe;
	}

	public static void precios_test(String[] args, long numero_de_llamadas, String nodos_por_llamada, String modo) {

		Driver driver = LauncherExplo.getDriver("2");
		Session session = driver.session();

		//StatementResult resultP = session.run("CALL eci.loadCaches(\"mio\")");

		long veces = 0;
		long iniTotal = System.currentTimeMillis();
		while(veces <numero_de_llamadas) {
			long iniParcial = System.currentTimeMillis();
			//StatementResult resultCa = session.run("call eci.precios.testmemorysize("+nodos_por_llamada+","+modo+")").consume();
			ResultSummary resultSummary = session.run("call eci.precios.testmemorysize("+nodos_por_llamada+","+modo+")").consume();
			long finParcial = System.currentTimeMillis();
			System.out.println("Tiempo en llamada " + String.valueOf(veces+1) +	": " + (finParcial - iniParcial) + " milisegundos ; numero nodos procesadas: " + nodos_por_llamada);
			veces = veces +1;
		}
		long finTotal = System.currentTimeMillis();
		System.out.println("---------------------------------------------------------------------------------------");
		System.out.println("Tiempo Total: " + (finTotal - iniTotal) + " milisegundos ");
		driver.close();
	}

	public static void test_error_unparseable_date(String[] args) {
		//Driver driver = LauncherExplo.getDriver("2");
		Driver driver = LauncherExplo.getDriver("localhost", "neo4j", "vector-itcgroup");
		Session session = driver.session();
		String EXPLOIT_CENTERS = "CALL eci.exploitCenters('%s')";
		String DATE_TIME_FORMAT = "yyyyMMdd HHmm";

		long iniTotal = System.currentTimeMillis();
		try {
			Date hoy = new Date();
			Date exploitDate = (Date) new SimpleDateFormat(DATE_TIME_FORMAT).parse("20190424 0900");

			String fechas = new SimpleDateFormat(DATE_TIME_FORMAT).format(exploitDate);
			Result resultP = session.run(String.format(EXPLOIT_CENTERS, fechas));
			while (resultP.hasNext()) {
				Record result = resultP.next();
				System.out.println(result.get("data").asMap());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		long finTotal = System.currentTimeMillis();
		System.out.println("Tiempo Total: " + (finTotal - iniTotal) + " milisegundos ");
		driver.close();

	}

}