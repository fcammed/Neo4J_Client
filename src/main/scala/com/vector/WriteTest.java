package com.vector;

import com.vector.repo.RepoFile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.*;

import java.util.regex.Pattern;

public class WriteTest {

	public static void main(String[] args) {
		int n_files = 10, tamano_files = 1043359, numero_workers = 3, workerId = 1;
		String sobreesccribir = "0", rutaArchivo = "", zipped = "true", modo_parser = "3", modo_modif = "1", cola_logic="1000",cola_file="1000000";
		boolean leer = true;
		boolean monolitico = false;

		//write_test(args,264,(200*1024*1024/201) ,"0",1,1, "","true",false);
		if (monolitico)
			write_test(args, n_files, tamano_files, sobreesccribir, numero_workers, workerId, rutaArchivo, zipped, leer, modo_parser, modo_modif); //(200*1024*1024/201)
		// un tercio - 50 Gb ; con diferentes tamaños y número de ficheros
		//write_test(args,264,  1043359,"0",1,1, "","true",false); //(200*1024*1024/201)
		//write_test(args,1,  1043359,"1",1,1, "","true",false); //(200*1024*1024/201)
		//write_test(args,26,  10594106,"0",1,1, "","false",true); //(200*1024*1024/201)
		//write_test(args,3,  91815592,"0",1,1, "","true",false); //(200*1024*1024/201)
		//write_test(args,1,  275446776,"0",1,1, "","true",false); //51,5GB (200*1024*1024/201)
		//write_test(args,1,  826340328,"0",1,1, "","true",false); //154,5GB (200*1024*1024/201)

		else {
			BlockingQueueProcessFragment bqpf = new BlockingQueueProcessFragment(n_files, tamano_files, sobreesccribir, numero_workers, workerId, rutaArchivo, zipped, leer, modo_parser, modo_modif,cola_logic,cola_file);
			try {
				bqpf.leer();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	public WriteTest(String[] args, int n_files, int tamano_files, String sobreescribir, int numero_workers, int workerId, String rutaArchivo, String zipped, String leer, String hilos, String modo_parser, String modo_modif,String cola_logic,String cola_file ) {
		boolean leerb;
		if (leer.equals("false"))
			leerb = false;
		else
			leerb = true;
		if (hilos.equals("true")) {
			BlockingQueueProcessFragment bqpf = new BlockingQueueProcessFragment(n_files, tamano_files, sobreescribir, numero_workers, workerId, rutaArchivo, zipped, leerb, modo_parser, modo_modif, cola_logic,cola_file);
			try {
				bqpf.leer();
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else
			write_test(args, n_files, tamano_files, sobreescribir, numero_workers, workerId, rutaArchivo, zipped, leerb, modo_parser, modo_modif);
	}

	public static void write_test(String[] args, int n_files, int tamano_files, String sobreescribir, int numero_workers, int workerId, String rutaArchivo, String zipped, boolean leer, String modo_parser, String modo_modif) {
		int cuenta = 0;

		RepoFile flog = new RepoFile(0, ",", workerId, rutaArchivo);
		flog.abreFicheroW();
		flog.writeLine(new ArrayList<String>(Arrays.asList("WriteTest -> ",
				"Numero ficheros:" + n_files,
				"Lineas por fichero:" + tamano_files,
				"numero workers:" + numero_workers,
				"workerId:" + workerId,
				"sobreescritura:" + sobreescribir,
				"isZipped:" + zipped
		)));

		long primerResultT = System.currentTimeMillis();

		while (cuenta < n_files) {
			cuenta = cuenta + 1;

			long primerResultP = System.currentTimeMillis();
			RepoFile fdatos;
			RepoFile fleer = new RepoFile(workerId, "|", workerId, rutaArchivo, false, true);
			if (sobreescribir.equals("1")) //Sobreescribe siempre el mismo fichero para no ocupar espacio en disco
				fdatos = new RepoFile(workerId, "|", workerId, rutaArchivo, false);
			else
				fdatos = new RepoFile(cuenta, "|", workerId, rutaArchivo, true);
			if (!zipped.equals("true"))
				fdatos.abreFicheroW();
			else
				//fdatos.abreFicheroW_zip();
				fdatos.abreFicheroW_zipSplit();

			int nlineas = 0;
			int cada = 0;
			boolean seguir = false;
			String buf_linea = "";
			String concatena_linea = "";
			List<String> linea = new ArrayList<String>();
			if (leer) {
				fleer.abreFicheroR();
				buf_linea = fleer.readLine();
				seguir = buf_linea != null;
				//ls = Arrays.stream(linea.split("|"));
				if (seguir)
					linea = Arrays.asList(buf_linea.split(Pattern.quote("|")));

			} else {
				buf_linea = "P|B001000811538362||7|0|null|null|00|P18354008|20190101|20190630|0000|2359|B|null|null|P - PARCEIROS HOTEIS|0040401063771|P - PARCEIROS HOTEIS GAIA                         |1  |                        ";
				seguir = nlineas < tamano_files;
			}

			//while (nlineas<tamano_files) {
			while (seguir) {
				nlineas = nlineas + 1;
				cada = cada + 1;
				//fdatos.writeLineString(linea);
				//fdatos.writeLine(linea);
				if (false && cada > 25000) {
					fdatos.flush();
					cada = 0;
				}

				//Parsear--------------------------------------------------------------------
				switch (modo_parser) {
					case "0":
						//no hacemos nada
						break;
					case "1":
						linea = Arrays.asList(buf_linea.split(Pattern.quote("|")));
						break;
					case "2":
						linea = new ArrayList<>();
						String trozo = "";
						for (int i = 0; i < buf_linea.length(); i++) {
							if (buf_linea.getBytes()[i] == '|') {
								linea.add(trozo);
								trozo = "";
							} else
								trozo = trozo + (char) buf_linea.getBytes()[i];
							//concatena_linea = concatena_linea + i.next();
						}
						break;
					case "3":
						linea = Arrays.asList(buf_linea.substring(0, 1),
								buf_linea.substring(2, 18),
								buf_linea.substring(20, 21),
								buf_linea.substring(21, 21),
								buf_linea.substring(22, 23),
								buf_linea.substring(24, 28),
								buf_linea.substring(29, 33),
								buf_linea.substring(34, 36),
								buf_linea.substring(37, 46),
								buf_linea.substring(47, 55),
								buf_linea.substring(56, 64),
								buf_linea.substring(65, 69),
								buf_linea.substring(70, 74),
								buf_linea.substring(75, 76),
								buf_linea.substring(77, 81),
								buf_linea.substring(82, 86),
								buf_linea.substring(87, 107),
								buf_linea.substring(108, 121),
								buf_linea.substring(122, 172),
								buf_linea.substring(173, 176),
								buf_linea.substring(177, 201)
						);
						break;
					case "4":
						linea = Arrays.asList(buf_linea.substring(0, 121),
								buf_linea.substring(122, 172),
								buf_linea.substring(173, 176),
								buf_linea.substring(177, 201)
						);
						break;
				}

				//Modificar--------------------------------------------------------------------------------

				switch (modo_modif) {
					case "0":
						//no hacemos nada
						break;
					case "1":
						for (ListIterator i = linea.listIterator(); i.hasNext(); ) {
							i.set("x" + i.next());
						}
						break;
				}

				//Escribir----------------------------------------------------------------------------------
				if (modo_parser.equals("0"))
					fdatos.writeLine(buf_linea);
				else
					fdatos.writeLine(linea);

				//Next---------------------------------------------------------------------------------------
				if (leer) {
					buf_linea = fleer.readLine();
					seguir = buf_linea != null;
				} else {
					//linea = "P|B001000811538362||7|0|null|null|00|P18354008|20190101|20190630|0000|2359|B|null|null|P - PARCEIROS HOTEIS|0040401063771|P - PARCEIROS HOTEIS GAIA                         |1  |                        ";
					//fdatos.writeLine(buf_linea);
					seguir = nlineas < tamano_files;
				}
			}
			if (leer) {
				fleer.cierraFicheroR();
			} else {

			}
			fdatos.cierraFichero();
			long finResultP = System.currentTimeMillis();
			System.out.println("Tiempo empleado en escribir fichero " + String.valueOf(cuenta) +
					": " + (finResultP - primerResultP) + " milisegundos ; numero lineas procesadas: " + nlineas);
			flog.writeLine(new ArrayList<String>(Arrays.asList("Fichero : " + String.valueOf(cuenta),
					"Tiempo: " + (finResultP - primerResultP) + " milisegundos"
			)));
		}
		long finResultT = System.currentTimeMillis();
		System.out.println("Tiempo total en escribir ficheros" +
				": " + (finResultT - primerResultT) + " milisegundos");
		flog.writeLine(new ArrayList<String>(Arrays.asList(
				"Tiempo Total: " + (finResultT - primerResultT) + " milisegundos"
		)));
		flog.cierraFichero();
	}
}

class BlockingQueueProcessFragment {

	private  int tamano_files;
	private  String sobreescribir;
	private  int numero_workers;
	private  int workerId;
	private  String rutaArchivo;
	private  String zipped;
	private  boolean leer;
	private  int n_files;
	private String modo_parser;
	private String modo_modif;
	private int long_cola_logic=1000;
	private int long_cola_file = 1000000;

	public BlockingQueueProcessFragment( int n_files, int tamano_files, String sobreescribir, int numero_workers, int workerId, String rutaArchivo, String zipped, boolean leer, String modo_parser, String modo_modif, String cola_logic,String cola_file  ) {
		this.tamano_files = tamano_files;
		this.sobreescribir = sobreescribir;
		this.numero_workers = numero_workers;
		this.workerId = workerId;
		this.rutaArchivo = rutaArchivo;
		this.zipped = zipped;
		this.leer = leer;
		this.n_files = n_files;
		this.modo_parser = modo_parser;
		this.modo_modif = modo_modif;
		if (!cola_logic.equals(""))
			this.long_cola_logic=new Integer(cola_logic).intValue();
		if (!cola_file.equals(""))
			this.long_cola_file=new Integer(cola_file).intValue();
	}

	public void leer() throws Exception {

		boolean bleer=false;
		RepoFile flog_r;
		RepoFile flog_w;
		int cuenta=0;

		if (!leer)
			long_cola_logic=10000;
		/*else
			long_cola_logic=1000;*/

		BlockingQueue queueLogic = new ArrayBlockingQueue(long_cola_logic);

		BlockingQueue queueFile = new ArrayBlockingQueue(long_cola_file);

		flog_r = new RepoFile(0, ",", workerId, rutaArchivo, true, false);
		flog_r.abreFicheroW();
		flog_w = new RepoFile(0, ",", workerId, rutaArchivo, false, false);
		flog_w.abreFicheroW();

		ExecutorService pool_rf = Executors.newFixedThreadPool(1);
		ExecutorService pool_t = Executors.newFixedThreadPool(numero_workers);
		ExecutorService pool_wf = Executors.newFixedThreadPool(1);

		//Unico fichero
		Runnable rwf = new WriterFile(queueFile, tamano_files, sobreescribir, numero_workers, workerId, rutaArchivo, zipped, 1, flog_w);
		for (int i = 0; i < numero_workers; i++) {
			pool_t.execute(new Transformer(queueLogic, queueFile, numero_workers,cuenta, modo_parser, modo_modif));
		}
		Future fwf = pool_wf.submit(rwf);

		long iniWriteT = System.currentTimeMillis();
		while (cuenta<n_files) {
			cuenta = cuenta + 1;

			Runnable rrf = new ReaderFile(queueLogic, tamano_files, numero_workers, workerId, rutaArchivo, leer, cuenta, flog_r, modo_modif,queueFile);
			Future frrf = pool_rf.submit(rrf);


			//Multiples ficheros
			/*Runnable rwf = new WriterFile(queueFile, sobreescribir, numero_workers, workerId, rutaArchivo, zipped, 1, flog_w);
			for (int i = 0; i < numero_workers; i++) {
				pool_t.execute(new Transformer(queueLogic, queueFile, cuenta));
			}
			Future fwf = pool_wf.submit(rwf);*/

			//Unico fichero
			frrf.get();
			//Multiples ficheros
			//fwf.get();
		}

		//Unico Fichero
		try {
			for (int j = 0; j < numero_workers; j++) {
				queueLogic.put("STOP");
				//System.out.println("READ: Enviado STOP");
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		fwf.get();


		long finWriteT = System.currentTimeMillis();
		System.out.println(" ");
		System.out.println("Tiempo empleado en total " +
				": " + (finWriteT - iniWriteT) + " milisegundos");
		flog_w.writeLine(new ArrayList<String>(Arrays.asList(" ")));
		flog_w.writeLine(new ArrayList<String>(Arrays.asList("Tiempo total proceso: " + (finWriteT - iniWriteT) + " milisegundos")));
		flog_r.cierraFichero();
		flog_w.cierraFichero();

		pool_rf.shutdown();
		pool_t.shutdown();
		pool_wf.shutdown();

	}
}

class ReaderFile implements Runnable{

	protected BlockingQueue queue = null;
	//private int n_files;
	private int tamano_files;
	private int numero_workers;
	private int workerId;
	private String rutaArchivo;
	private boolean leer;
	private int cuenta;
	private RepoFile flog;
	private String modo_modif;
	protected BlockingQueue queuefile = null;

	public ReaderFile(BlockingQueue queue, int tamano_files, int numero_workers, int workerId, String rutaArchivo, boolean leer, int cuenta, RepoFile flog, String modo_modif, BlockingQueue queuefile) {
		this.queue = queue;
		//this.n_files = n_files;
		this.tamano_files = tamano_files;
		this.numero_workers = numero_workers;
		this.workerId = workerId;
		this.rutaArchivo = rutaArchivo;
		this.leer = leer;
		this.cuenta = cuenta;
		this.flog = flog;
		this.modo_modif = modo_modif;
		this.queuefile = queuefile;
	}

	public void run() {
		//try {
			/*queue.put("1");
			Thread.sleep(1000);
			queue.put("2");
			Thread.sleep(1000);
			queue.put("3");*/
		Thread.currentThread().setPriority(10);
		read_file(queue ,tamano_files, numero_workers, workerId, rutaArchivo, leer, cuenta, modo_modif, queuefile);
		//} catch (InterruptedException e) {
		//	e.printStackTrace();
		//}
	}


	public void read_file(BlockingQueue queue,int tamano_files, int numero_workers, int workerId, String rutaArchivo, boolean leer, int cuenta, String modo_modif, BlockingQueue queuefile) {

		long primerResultP = System.currentTimeMillis();
		long ini_leerfichero = System.nanoTime();
		RepoFile fleer = new RepoFile(workerId, "|", workerId, rutaArchivo,false, true);

		int nlineas=0;
		int cada=0;
		long duration =0;

		boolean seguir = false;
		String buf_linea= "";
		String concatena_linea = "";
		//List<String> linea = new ArrayList<String>();
		if (leer) {
			fleer.abreFicheroR();
			buf_linea = fleer.readLine();
			seguir = buf_linea != null;
			//ls = Arrays.stream(linea.split("|"));
				/*if (seguir)
					linea = Arrays.asList(buf_linea.split(Pattern.quote("|")));*/

		} else {
			buf_linea = "P|B001000811538362||7|0|null|null|00|P18354008|20190101|20190630|0000|2359|B|null|null|P - PARCEIROS HOTEIS|0040401063771|P - PARCEIROS HOTEIS GAIA                         |1  |                        ";
			seguir = nlineas<tamano_files;
		}
		//while (nlineas<tamano_files) {
		while (seguir) {
			nlineas = nlineas + 1;
			//Put en la Blocking Queue
			long fin_leerfichero = System.nanoTime();
			duration = duration + (fin_leerfichero-ini_leerfichero);
			try {
				if (modo_modif.equals("0"))
					queuefile.put(buf_linea);
				else
					queue.put(buf_linea);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			ini_leerfichero = System.nanoTime();
			//leer siguiente
			if (leer) {
				buf_linea = fleer.readLine();
				seguir = buf_linea != null;
			} else {
				seguir = nlineas<tamano_files;
			}
		}
		if (leer) {
			fleer.cierraFicheroR();
		} else {

		}
		try {
			for (int j = 1; j <= numero_workers; j++) {
				queue.put("FICHERO"+j);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		long finResultP = System.currentTimeMillis();
		System.out.println(" ");
		System.out.println("Tiempo empleado en procesar fichero " + String.valueOf(cuenta) +
				": " + (finResultP - primerResultP) + " milisegundos ; numero lineas procesadas: " + nlineas);
		System.out.println("     SOLO Leer fichero " + String.valueOf(cuenta) + 	": " + duration / 1e6  + " milisegundos");
		flog.writeLine(new ArrayList<String>(Arrays.asList(" ")));
		flog.writeLine(new ArrayList<String>(Arrays.asList("Fichero : " + String.valueOf(cuenta),
				"Tiempo leer: " + (finResultP - primerResultP) + " milisegundos"
		)));
		flog.writeLine(new ArrayList<String>(Arrays.asList(
				"     SOLO Leer fichero " + String.valueOf(cuenta) + 	": " + duration / 1e6  + " milisegundos"
		)));
	}
}

class Transformer implements Runnable{

	protected BlockingQueue queue = null;
	protected BlockingQueue queuefile = null;
	private boolean leer;
	private int cuenta, numero_workers;
	private static final String separador_field = "|";
	private static final String separador_registro = "\n\r";
	private String modo_parser;
	private String modo_modif;
	private long duracion=0;


	public Transformer(BlockingQueue queue, BlockingQueue queuefile,  int numero_workers,int cuenta, String modo_parser, String modo_modif) {
		this.queue = queue;
		this.queuefile = queuefile;
		this.numero_workers = numero_workers;
		this.leer = leer;
		this.cuenta = cuenta;
		this.modo_parser = modo_parser;
		this.modo_modif = modo_modif;
	}

	public void run() {
		try {
			while (true) {
				String buf_linea = (String) queue.take();
				//long iniDuracion = System.nanoTime();
				if (buf_linea.equals("STOP")) {
					transforma_linea( "STOP", queuefile, cuenta, modo_parser, modo_modif);
					return;
				} else {
					if (buf_linea.substring(0,7).equals("FICHERO")) {
						if (buf_linea.equals("FICHERO"+numero_workers)) {
							//long finDuracion = System.nanoTime();
							//duracion = duracion + (finDuracion - iniDuracion);
							System.out.println("     SOLO Procesar fichero: " + duracion / 1e6 + " milisegundos");
							duracion = 0;
						}
						duracion = duracion + transforma_linea( "FICHERO", queuefile, cuenta, modo_parser, modo_modif);
					} else
						duracion = duracion + transforma_linea( buf_linea, queuefile, cuenta, modo_parser, modo_modif);
					//long finDuracion = System.nanoTime();
					//duracion = duracion + (finDuracion - iniDuracion);
				}
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	public long transforma_linea(String buf_linea, BlockingQueue queuefile, int cuenta, String modo_parser, String modo_modif) {
		boolean seguir = false;
		String buffer = "";
		long iniDuracion = System.nanoTime();
		long finDuracion = iniDuracion;
		List<String> linea = new ArrayList<>();
		if (!buf_linea.equals("STOP") && !buf_linea.equals("FICHERO")) {
			switch (modo_parser)
			{
				case "0":
					//no hacemos nada
					break;
				case "1":
					linea = Arrays.asList(buf_linea.split(Pattern.quote("|")));
					break;
				case "2":
					linea = new ArrayList<>();
					String trozo ="";
					for ( int i = 0; i<buf_linea.length(); i++ ) {
						if (buf_linea.getBytes()[i] == '|')
						{
							linea.add(trozo);
							trozo="";
						}
						else
							trozo = trozo + (char)buf_linea.getBytes()[i];
						//concatena_linea = concatena_linea + i.next();
					}
					break;
				case "3":
					linea = Arrays.asList(buf_linea.substring(0,1),
							buf_linea.substring(2,18),
							buf_linea.substring(20,21),
							buf_linea.substring(21,21),
							buf_linea.substring(22,23),
							buf_linea.substring(24,28),
							buf_linea.substring(29,33),
							buf_linea.substring(34,36),
							buf_linea.substring(37,46),
							buf_linea.substring(47,55),
							buf_linea.substring(56,64),
							buf_linea.substring(65,69),
							buf_linea.substring(70,74),
							buf_linea.substring(75,76),
							buf_linea.substring(77,81),
							buf_linea.substring(82,86),
							buf_linea.substring(87,107),
							buf_linea.substring(108,121),
							buf_linea.substring(122,172),
							buf_linea.substring(173,176),
							buf_linea.substring(177,201)
					);
					break;
				case "4":
					linea = Arrays.asList(buf_linea.substring(0, 121),
							buf_linea.substring(122, 172),
							buf_linea.substring(173, 176),
							buf_linea.substring(177, 201)
					);
					break;
			}
			switch (modo_modif)
			{
				case "0":
					//no hacemos nada
					break;
				case "1":
					int count = 0;
					for (ListIterator i = linea.listIterator(); i.hasNext(); ) {
						//i.set("x" + i.next());
						if (count != 0)
							buffer = buffer + separador_field;
						//else
						buffer = buffer + "x" + i.next();
						count = count + 1;
					}
					break;

			}
			finDuracion = System.nanoTime();
			if (modo_modif.equals("0"))
				try {
					queuefile.put(buf_linea);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			else
				try {
					queuefile.put(buffer);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		} else {
			try {
				queuefile.put(buf_linea);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return (finDuracion - iniDuracion);
	}
}

class WriterFile implements Runnable{

	protected BlockingQueue queue = null;
	private int numero_workers;
	private int workerId;
	private String rutaArchivo;
	//private boolean leer;
	private int cuenta;
	private String sobreescribir;
	private String zipped;
	private RepoFile fdatos;
	private RepoFile flog;
	private int cada=0;
	private long primerResultP;
	private int cuenta_stop =0, cuenta_fichero=0;
	private long ini_temporal;
	private int tamano_file;
	private long t_proceso=0, t_duracion=0;

	public WriterFile(BlockingQueue queue, int tamano_files,  String sobreescribir, int numero_workers, int workerId, String rutaArchivo, String zipped, int cuenta, RepoFile flog) {
		this.queue = queue;
		this.tamano_file = tamano_files;
		this.numero_workers = numero_workers;
		this.workerId = workerId;
		this.rutaArchivo = rutaArchivo;
		//this.leer = leer;
		this.cuenta = cuenta;
		this.sobreescribir = sobreescribir;
		this.zipped = zipped;
		this.flog = flog;
		primerResultP = System.currentTimeMillis();


		if (sobreescribir.equals("1")) //Sobreescribe siempre el mismo fichero para no ocupar espacio en disco
			fdatos = new RepoFile(workerId, "|", workerId, rutaArchivo,false);
		else
			fdatos = new RepoFile(cuenta, "|", workerId, rutaArchivo, true);
		if(!zipped.equals("true"))
			fdatos.abreFicheroW();
		else
			//fdatos.abreFicheroW_zip();
			fdatos.abreFicheroW_zipSplit();
	}

	public void run() {
		Thread.currentThread().setPriority(10);
		int cont_bloques=0;
		int cont_temp_lineas_estadistica=0;
		long iniDuracion = System.currentTimeMillis();
		try {
			while (true) {
				String buf_linea = (String) queue.take();
				long iniProceso = System.nanoTime();
				cont_bloques = cont_bloques+1;
				cont_temp_lineas_estadistica = cont_temp_lineas_estadistica + 1;
				//if (buf_linea.equals("STOP") || buf_linea.equals("FICHERO")) {
				switch (buf_linea) {
					case "STOP":
						write_file("STOP");
						if (cuenta_stop == numero_workers) {
							fdatos.cierraFichero();
							long finResultP = System.currentTimeMillis();

							System.out.println(" ");
							System.out.println("Tiempo TOTAL en escribir fichero " + String.valueOf(cuenta) +
									": " + (finResultP - primerResultP) + " milisegundos");

							flog.writeLine(new ArrayList<String>(Arrays.asList(" ")));
							flog.writeLine(new ArrayList<String>(Arrays.asList("Fichero : " + String.valueOf(cuenta),
									"Tiempo escribir: " + (finResultP - primerResultP) + " milisegundos"
							)));
							return;
						}
						break;
					case "FICHERO":
						write_file("FICHERO");
						if (cuenta_fichero == numero_workers) {
							System.out.println("     Lineas escritas por transformer: " + cont_temp_lineas_estadistica);
							flog.writeLine(new ArrayList<String>(Arrays.asList("     Lineas escritas por bloque transformer: " + cont_temp_lineas_estadistica)));
							cont_temp_lineas_estadistica = 0;
							cuenta_fichero = 0;
						}
						break;
					default:
						write_file( buf_linea);
						break;
				}
				//	}
				long finProceso = System.nanoTime();
				t_proceso = t_proceso + (finProceso-iniProceso);
				if (cont_bloques==tamano_file) {
					long finDuracion = System.currentTimeMillis();
					//t_duracion = t_duracion + (finDuracion-iniDuracion);
					cont_bloques=0;
					System.out.println("     SOLO Escribir Fichero: " +	t_proceso / 1e6+ " milisegundos");
					System.out.println("     Duracion escribir Fichero: " +	(finDuracion-iniDuracion) + " milisegundos");
					flog.writeLine(new ArrayList<String>(Arrays.asList("     SOLO Escribir Fichero: " + t_proceso / 1e6 + " milisegundos")));
					flog.writeLine(new ArrayList<String>(Arrays.asList("     Duracion escribir Fichero: " + (finDuracion-iniDuracion) + " milisegundos")));
					t_proceso =0;
					iniDuracion = System.currentTimeMillis();
				}
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	public void write_file( String linea) {
		switch (linea) {
			case "STOP":
				cuenta_stop = cuenta_stop + 1;
				break;
			case "FICHERO":
				cuenta_fichero = cuenta_fichero + 1;
				break;
			default:
				fdatos.writeLine(linea);
		}
	}
}