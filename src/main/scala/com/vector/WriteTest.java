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
		int n_files=2, tamano_files=1043359, numero_workers=1, workerId=1;
		String sobreesccribir="0", rutaArchivo="", zipped="false";
		boolean leer=false;
		boolean prueba = false;

		//write_test(args,264,(200*1024*1024/201) ,"0",1,1, "","true",false);
		if (prueba) write_test(args,n_files, tamano_files ,sobreesccribir,numero_workers,workerId, rutaArchivo,zipped,leer); //(200*1024*1024/201)
		// un tercio - 50 Gb ; con diferentes tamaños y número de ficheros
		//write_test(args,264,  1043359,"0",1,1, "","true",false); //(200*1024*1024/201)
		//write_test(args,1,  1043359,"1",1,1, "","true",false); //(200*1024*1024/201)
		//write_test(args,26,  10594106,"0",1,1, "","false",true); //(200*1024*1024/201)
		//write_test(args,3,  91815592,"0",1,1, "","true",false); //(200*1024*1024/201)
		//write_test(args,1,  275446776,"0",1,1, "","true",false); //51,5GB (200*1024*1024/201)
		//write_test(args,1,  826340328,"0",1,1, "","true",false); //154,5GB (200*1024*1024/201)

		if (!prueba) {
			BlockingQueueProcessFragment bqpf = new BlockingQueueProcessFragment(n_files, tamano_files ,sobreesccribir,3,workerId, rutaArchivo,zipped,leer);
			try {
				bqpf.leer();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	public WriteTest(String[] args,int n_files,int tamano_files,String sobreescribir, int numero_workers,int workerId, String rutaArchivo, String zipped, String leer, String hilos) {
		boolean leerb;
		if (leer.equals("false"))
			leerb = false;
		else
			leerb = true;
		if (hilos.equals("true")) {
			BlockingQueueProcessFragment bqpf = new BlockingQueueProcessFragment(n_files, tamano_files, sobreescribir, numero_workers, workerId, rutaArchivo, zipped, leerb);
			try {
				bqpf.leer();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		else
			write_test(args,n_files,tamano_files,sobreescribir, numero_workers,workerId,rutaArchivo,zipped, leerb);
	}

	public static void write_test(String[] args,int n_files,int tamano_files,String sobreescribir, int numero_workers,int workerId, String rutaArchivo, String zipped, boolean leer) {
		int cuenta=0;

		RepoFile flog = new RepoFile(0,",", workerId, rutaArchivo);
		flog.abreFicheroW();
		flog.writeLine(new ArrayList<String>(Arrays.asList("WriteTest -> ",
				"Numero ficheros:" + n_files ,
				"Lineas por fichero:" + tamano_files ,
				"numero workers:" + numero_workers,
				"workerId:" + workerId,
				"sobreescritura:" + sobreescribir,
				"isZipped:" + zipped
		)));

		long primerResultT = System.currentTimeMillis();

		while (cuenta<n_files) {
			cuenta = cuenta +1;

			long primerResultP = System.currentTimeMillis();
			RepoFile fdatos;
			RepoFile fleer = new RepoFile(workerId, "|", workerId, rutaArchivo,false, true);
			if (sobreescribir.equals("1")) //Sobreescribe siempre el mismo fichero para no ocupar espacio en disco
				fdatos = new RepoFile(workerId, "|", workerId, rutaArchivo,false);
			else
				fdatos = new RepoFile(cuenta, "|", workerId, rutaArchivo, true);
			if(!zipped.equals("true"))
				fdatos.abreFicheroW();
			else
				//fdatos.abreFicheroW_zip();
				fdatos.abreFicheroW_zipSplit();

			int nlineas=0;
			int cada=0;
			boolean seguir = false;
			String buf_linea= "";
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
				seguir = nlineas<tamano_files;
			}

			//while (nlineas<tamano_files) {
			while (seguir) {
				nlineas = nlineas + 1;
				cada=cada+1;
				//fdatos.writeLineString(linea);
				//fdatos.writeLine(linea);
				if (false && cada>25000)  {
					fdatos.flush();
					cada =0;
				}

				//Parsear--------------------------------------------------------------------

				//1
				//linea = Arrays.asList(buf_linea.split(Pattern.quote("|")));

				//2
						/*linea = new ArrayList<>();
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
						}*/

				//3
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

				//4
						/*linea = Arrays.asList(buf_linea.substring(0,121),
								buf_linea.substring(122,172),
								buf_linea.substring(173,176),
								buf_linea.substring(177,201)
						);*/

				//Modificar--------------------------------------------------------------------------------
				if (true) {//if ((cada % 2) == 0 )
					//concatena_linea = "";
					for (ListIterator i = linea.listIterator(); i.hasNext(); ) {
						i.set("x" + i.next());
						//concatena_linea = concatena_linea + "x" + i.next();
					}
				}

				//Escribir----------------------------------------------------------------------------------
				fdatos.writeLine(linea);

				//Next---------------------------------------------------------------------------------------
				if (leer) {
					buf_linea = fleer.readLine();
					seguir = buf_linea != null;
				} else {
					//linea = "P|B001000811538362||7|0|null|null|00|P18354008|20190101|20190630|0000|2359|B|null|null|P - PARCEIROS HOTEIS|0040401063771|P - PARCEIROS HOTEIS GAIA                         |1  |                        ";
					//fdatos.writeLine(buf_linea);
					seguir = nlineas<tamano_files;
				}
			}
			if (leer) {
				fleer.cierraFicheroR();
			} else {

			}
			fdatos.cierraFichero();
			long finResultP = System.currentTimeMillis();
			System.out.println("Tiempo empleado en escribir fichero " + String.valueOf(cuenta) +
					": " + (finResultP - primerResultP)+ " milisegundos ; numero lineas procesadas: " + nlineas);
			flog.writeLine(new ArrayList<String>(Arrays.asList("Fichero : " + String.valueOf(cuenta),
					"Tiempo: " + (finResultP - primerResultP) + " milisegundos"
			)));
		}
		long finResultT = System.currentTimeMillis();
		System.out.println("Tiempo total en escribir ficheros" +
				": " + (finResultT - primerResultT)+ " milisegundos");
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

	public BlockingQueueProcessFragment( int n_files, int tamano_files, String sobreescribir, int numero_workers, int workerId, String rutaArchivo, String zipped, boolean leer) {
		this.tamano_files = tamano_files;
		this.sobreescribir = sobreescribir;
		this.numero_workers = numero_workers;
		this.workerId = workerId;
		this.rutaArchivo = rutaArchivo;
		this.zipped = zipped;
		this.leer = leer;
		this.n_files = n_files;
	}

	public void leer() throws Exception {

		boolean bleer=false;
		RepoFile flog_r;
		RepoFile flog_w;
		int cuenta=0;

		BlockingQueue queueLogic = new ArrayBlockingQueue(1000);
		BlockingQueue queueFile = new ArrayBlockingQueue(1000000);

		flog_r = new RepoFile(0, ",", workerId, rutaArchivo, true, false);
		flog_r.abreFicheroW();
		flog_w = new RepoFile(0, ",", workerId, rutaArchivo, false, false);
		flog_w.abreFicheroW();

		ExecutorService pool_rf = Executors.newFixedThreadPool(1);
		ExecutorService pool_t = Executors.newFixedThreadPool(numero_workers);
		ExecutorService pool_wf = Executors.newFixedThreadPool(1);

		//Unico fichero
		Runnable rwf = new WriterFile(queueFile, sobreescribir, numero_workers, workerId, rutaArchivo, zipped, 1, flog_w);
		for (int i = 0; i < numero_workers; i++) {
			pool_t.execute(new Transformer(queueLogic, queueFile, cuenta));
		}
		Future fwf = pool_wf.submit(rwf);

		long iniWriteT = System.currentTimeMillis();
		while (cuenta<n_files) {
			cuenta = cuenta + 1;

			Runnable rrf = new ReaderFile(queueLogic, tamano_files, numero_workers, workerId, rutaArchivo, leer, cuenta, flog_r);
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
		System.out.println("Tiempo empleado en total " +
				": " + (finWriteT - iniWriteT) + " milisegundos");
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

	public ReaderFile(BlockingQueue queue, int tamano_files, int numero_workers, int workerId, String rutaArchivo, boolean leer, int cuenta, RepoFile flog) {
		this.queue = queue;
		//this.n_files = n_files;
		this.tamano_files = tamano_files;
		this.numero_workers = numero_workers;
		this.workerId = workerId;
		this.rutaArchivo = rutaArchivo;
		this.leer = leer;
		this.cuenta = cuenta;
		this.flog = flog;
	}

	public void run() {
		//try {
			/*queue.put("1");
			Thread.sleep(1000);
			queue.put("2");
			Thread.sleep(1000);
			queue.put("3");*/
			read_file(queue ,tamano_files, numero_workers, workerId, rutaArchivo, leer, cuenta);
		//} catch (InterruptedException e) {
		//	e.printStackTrace();
		//}
	}


	public void read_file(BlockingQueue queue,int tamano_files, int numero_workers, int workerId, String rutaArchivo, boolean leer, int cuenta) {

			long primerResultP = System.currentTimeMillis();
			RepoFile fleer = new RepoFile(workerId, "|", workerId, rutaArchivo,false, true);

			int nlineas=0;
			int cada=0;
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
				try {
					queue.put(buf_linea);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
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
		/*try {
			for (int j = 0; j < numero_workers; j++) {
				queue.put("STOP");
				//System.out.println("READ: Enviado STOP");
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}*/
			long finResultP = System.currentTimeMillis();
			System.out.println("Tiempo empleado en leer fichero " + String.valueOf(cuenta) +
				": " + (finResultP - primerResultP) + " milisegundos ; numero lineas procesadas: " + nlineas);
			flog.writeLine(new ArrayList<String>(Arrays.asList("Fichero : " + String.valueOf(cuenta),
					"Tiempo leer: " + (finResultP - primerResultP) + " milisegundos"
			)));
	}
}

class Transformer implements Runnable{

	protected BlockingQueue queue = null;
	protected BlockingQueue queuefile = null;
	private boolean leer;
	private int cuenta;
	private static String separador_field = "|";
	private static final String separador_registro = "\n\r";

	public Transformer(BlockingQueue queue, BlockingQueue queuefile, int cuenta) {
		this.queue = queue;
		this.queuefile = queuefile;
		this.leer = leer;
		this.cuenta = cuenta;
	}

	public void run() {
		try {
			while (true) {
				String buf_linea = (String) queue.take();
				if (buf_linea.equals("STOP")) {
					transforma_linea( "STOP", queuefile, cuenta);
					return;
				} else {
					transforma_linea( buf_linea, queuefile, cuenta);
				}
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	public void transforma_linea(String buf_linea, BlockingQueue queuefile, int cuenta) {


		long primerResultP = System.currentTimeMillis();

		boolean seguir = false;
		List<String> linea;
		if (!buf_linea.equals("STOP")) {
			//1
			//linea = Arrays.asList(buf_linea.split(Pattern.quote("|")));

			//2
			/*linea = new ArrayList<>();
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
			}*/

			//3
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

			//4
			/*linea = Arrays.asList(buf_linea.substring(0, 121),
					buf_linea.substring(122, 172),
					buf_linea.substring(173, 176),
					buf_linea.substring(177, 201)
			);*/
			if (true) {
				int count = 0;
				String buffer = "";
				for (ListIterator i = linea.listIterator(); i.hasNext(); ) {
					//i.set("x" + i.next());
					if (count != 0)
						buffer = buffer + separador_field;
					//else
					buffer = buffer + "x" + i.next();
					count = count + 1;
				}
				/*int count = 0;
				String buffer = "";
				for (String field : linea
				) {
					if (count != 0)
						buffer = buffer + separador_field;
					//out.newLine();
					if (field == null) {
						buffer = buffer + "";
					} else if (field == "null") {
						buffer = buffer + "";
					} else
						buffer = buffer + field;
					count = count + 1;
				}*/
				//buffer = buffer + "\r\n"; //newLine();

				try {
					queuefile.put(buffer);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}
		} else {
			try {
				//System.out.println("TRANS: Recibido STOP");
				queuefile.put(buf_linea);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
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
	private int sr =0;



	public WriterFile(BlockingQueue queue,  String sobreescribir, int numero_workers, int workerId, String rutaArchivo, String zipped, int cuenta, RepoFile flog) {
		this.queue = queue;
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
		try {
			while (true) {
				String buf_linea = (String) queue.take();
				if (buf_linea.equals("STOP")) {
					write_file( "STOP");
					if (sr == numero_workers) {
						fdatos.cierraFichero();
						long finResultP = System.currentTimeMillis();
						System.out.println("Tiempo empleado en escribir fichero " + String.valueOf(cuenta) +
								": " + (finResultP - primerResultP) + " milisegundos");
						flog.writeLine(new ArrayList<String>(Arrays.asList("Fichero : " + String.valueOf(cuenta),
								"Tiempo escribir: " + (finResultP - primerResultP) + " milisegundos"
						)));
						return;
					}
				} else {
					write_file( buf_linea);
				}
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	public void write_file( String linea) {
		if (linea.equals("STOP")) {
			sr =sr +1 ;
			//System.out.println("WRITER: Recibido STOP");
		} else {
			/*cada=cada+1;

			if (false && cada>25000)  {
				fdatos.flush();
				cada =0;
			}*/
			fdatos.writeLine(linea);
		}
	}
}