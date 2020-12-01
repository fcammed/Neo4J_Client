package com.vector;

import com.vector.repo.RepoFile;
import com.vector.service.zip.original.*;
//import com.vector.service.zip.crc.CRC32;

//import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.*;

import java.util.regex.Pattern;
//import java.util.zip.CRC32;
import com.vector.service.zip.ParallelCRC32;

import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
//import java.util.zip.ZipEntry;
import com.vector.service.zip.original.ZipEntry;
//import java.util.zip.ZipException;

//import java.util.zip.*;
// import para prueba deflate con JZlib
import com.jcraft.jzlib.*;

/*import static com.vector.service.zip.original.ZipConstants.CENSIG;
import static com.vector.service.zip.original.ZipConstants.ENDSIG;
import static com.vector.service.zip.zipConstants64.*;
import static com.vector.service.zip.zipUtils.fileTimeToUnixTime;
import static com.vector.service.zip.zipUtils.get16;
import static java.util.zip.ZipOutputStream.DEFLATED;
import static java.util.zip.ZipOutputStream.STORED;*/


public class WriteTest {

	//Datos referidos al OutputStream
	private static ParallelCRC32 crcD1 = null;
	private static ParallelCRC32 crcD2 = null;
	private static boolean nowrap = true;
	//private static long countD = 0;
	//private static long written =0;
	//private static long locoff = 0;
	//private static XEntry current = null;
	//private static Vector<XEntry> xentries = new Vector<>();
	//private static zipCoder zc = new zipCoder(StandardCharsets.UTF_8);
	//Fin Datos referidos al OutputStream

	public static void main(String[] args) throws DataFormatException, UnsupportedEncodingException {
		//Prueba estandar
		int n_files = 10,  tamano_files = 1043359, numero_workers = 3, workerId = 1;
		String sobreesccribir = "0", rutaArchivo = "", zipped = "true", modo_parser = "4", modo_modif = "1", cola_logic="100",cola_file="100";

		//Prueba 1 fuchero con 3 líneas
		//int n_files = 1,  tamano_files = 5, numero_workers = 3, workerId = 1;
		//String sobreesccribir = "0", rutaArchivo = "", zipped = "true", modo_parser = "0", modo_modif = "0", cola_logic="100",cola_file="100";

		//Escenario ECI, lectura muy rápida (por fragmentos optimizados) -> se simula con leer = false
		// Con proceso menor que escritura, hay que poner pocos workers(1) y cola de proceso 10% de la de file. Buffer 5
		// Con proceso más pesado o similar que escritura, aumentar los workers y cola de proceso 100% de la de file. Buffer 5
		boolean leer = false;
		boolean monohilo = false;
		boolean poc_paralelizacion_createzip = false;
		boolean una_linea_poc_paralelizacion = false;
		if (!poc_paralelizacion_createzip) {

			//write_test(args,264,(200*1024*1024/201) ,"0",1,1, "","true",false);
			if (monohilo)
				write_test(args, n_files, tamano_files, sobreesccribir, numero_workers, workerId, rutaArchivo, zipped, leer, modo_parser, modo_modif); //(200*1024*1024/201)
				// un tercio - 50 Gb ; con diferentes tamaños y número de ficheros
				//write_test(args,264,  1043359,"0",1,1, "","true",false); //(200*1024*1024/201)
				//write_test(args,1,  1043359,"1",1,1, "","true",false); //(200*1024*1024/201)
				//write_test(args,26,  10594106,"0",1,1, "","false",true); //(200*1024*1024/201)
				//write_test(args,3,  91815592,"0",1,1, "","true",false); //(200*1024*1024/201)
				//write_test(args,1,  275446776,"0",1,1, "","true",false); //51,5GB (200*1024*1024/201)
				//write_test(args,1,  826340328,"0",1,1, "","true",false); //154,5GB (200*1024*1024/201)

			else {
				BlockingQueueProcessFragment bqpf = new BlockingQueueProcessFragment(n_files, tamano_files, sobreesccribir, numero_workers, workerId, rutaArchivo, zipped, leer, modo_parser, modo_modif, cola_logic, cola_file);
				try {
					bqpf.leer();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		} else {
			//ExternalSort es = new ExternalSort("R6Z7B0A6.csv", 100, "salida.txt", "");
			RepoFile fzip = new RepoFile("zipManual.zip", rutaArchivo,"");
			fzip.abreFicheroW();
			OutputStream out = fzip.getFOS();
			try
			{
				if (una_linea_poc_paralelizacion)
					writeZIPManual_1Compresor(out);
				else
					writeZIPManual_2Compresores(out);
			}
				catch (IOException e) {
					System.err.println("Error: " + e.getMessage());
				}
			fzip.cierraFichero();
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
					case "5":
						linea = Arrays.asList(buf_linea.substring(0, 60),
								buf_linea.substring(61, 121),
								buf_linea.substring(122, 152),
								buf_linea.substring(153, 172),
								buf_linea.substring(173, 174),
								buf_linea.substring(175, 176),
								buf_linea.substring(177, 190),
								buf_linea.substring(191, 201)
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
					fdatos.writeLine(buf_linea,false);
				//TODO hacer compatible con las dos formas de comprimir (Deflater y JZlib)
				//No funcionaría con JZlib , porque nunca habría último
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
	static int CRCUpdate(String linea, ParallelCRC32 crc) throws IOException {
		byte[] b = linea.getBytes();
		if(b.length > -1) {
			crc.update(b, 0, b.length);
		}
		return b.length;
	}
	static void writeZIPManual_1Compresor(OutputStream out) throws IOException {
		String linea="";
		ZipOutputStream zipStream = new ZipOutputStream(out);
		zipStream.setMethod(ZipOutputStream.DEFLATED);
		zipStream.setLevel(1);
		//Header LOC
		ZipEntry entry = new ZipEntry("salida.txt");
		entry.setComment("Fichero de salida zip");
		zipStream.putNextEntry(entry);
		//Data
		linea="1234567890123456789012345678901234567890P1234567890123456789012345678901234567890";
		crcD1 = new ParallelCRC32();
		CRCUpdate(linea, crcD1);
		// Compress the bytes
		byte[] input1 = linea.getBytes("UTF-8");
		byte[] output1 = new byte[100];
		Deflater compresser1 = new Deflater(Deflater.BEST_SPEED, nowrap);
		//Deflater compresser1 = new Deflater();
		compresser1.setInput(input1);
		compresser1.finish();
		int compressedDataLength1 = compresser1.deflate(output1);
		compresser1.end();
		//DEBUG
		print_Hex(output1,compressedDataLength1);
		//DEBUG
		out.write(output1,0,compressedDataLength1);
		long size = input1.length;
		long csize= compressedDataLength1;
		long crcv=0;

		crcv = crcD1.getValue();
		//Footer LOC
		/*long size = deflater.getBytesRead()+ deflater2.getBytesRead();
		long csize= deflater.getBytesWritten()+ deflater2.getBytesWritten();//cache porque al usarlo se pierde el valor de getBytesWritten
		long crcv = crcD.getValue(); //crcD.combine(crcD.getValue(),crcD2.getValue(),deflater2.getBytesRead());// crcD.getValue() ;//cache porque al usarlo se pierde el valor del CRC*/
		//ParallelZipOutputStream -> para hacer que funcione el closeEntry
		//1- extender ZipOutputStream para tener un setCRC32, que estableceríamos con el crcD
		//zipStream.setCrc(crcD);
		//2-usar un DeflaterOutputStream extendido, que a su vez use un Deflater extendido, al que se le pueda establecer BytesRead y BytesWritten
		// De esta forma sería -> zipStream.closeEntry();
		//En vez de:
		zipStream.closeEntry(size, csize,crcv);

		/*ParallelDeflater d_closeentry = new ParallelDeflater(Deflater.DEFAULT_COMPRESSION, true);
		ParallelDeflaterOutputStream dos_closeentry = new ParallelDeflaterOutputStream(out,deflater);

		zipStream.setCrc(crcD);
		d_closeentry.setBytesRead(deflater.getBytesRead());
		d_closeentry.setBytesWritten(csize);
		zipStream.closeEntry();*/
		print_Inflate(output1,csize);

		//Central Directory
		entry.setSize(size);
		entry.setCompressedSize(csize);
		entry.setCrc(crcv);
		zipStream.close();
	}
	static void writeZIPManual_2Compresores(OutputStream out) throws IOException {
		String linea="";
		//ByteArrayOutputStream ab1 = new ByteArrayOutputStream();
		//ByteArrayOutputStream ab2 = new ByteArrayOutputStream();
		//ParallelZipOutputStream zipStream = new ParallelZipOutputStream(out);
		ZipOutputStream zipStream = new ZipOutputStream(out);
		zipStream.setMethod(ZipOutputStream.DEFLATED);
		zipStream.setLevel(1);

		//Header LOC
		ZipEntry entry = new ZipEntry("salida.txt");
		entry.setComment("Fichero de salida zip");
		zipStream.putNextEntry(entry);

		//Data

		//Usando Método ParallelDeflater
		//Descartado
		/*Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
		ParallelDeflaterOutputStream deflateOut = new ParallelDeflaterOutputStream(ab1,deflater);
		deflater.setLevel(1);

		Deflater deflater2 = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
		ParallelDeflaterOutputStream deflateOut2 = new ParallelDeflaterOutputStream(ab2,deflater2);
		deflater2.setLevel(1);*/

		boolean uno = false;
		if(uno)
			linea="1234567890123456789012345678901234567890P1234567890123456789012345678901234567890";
		else
			linea="1234567890123456789012345678901234567890P";

		crcD1 = new ParallelCRC32();
		//String lineaf="1234567890123456789012345678901234567890V";
		CRCUpdate(linea, crcD1);

		//otros métodos
		/*deflateOut.write(linea.getBytes(), 0, linea.length());
		deflateOut.flush();
		if (!deflater.finished()) {
			deflater.finish();
			while (!deflater.finished()) {
				// razón de la existencia de ParallelDeflaterOutputStream
				deflateOut.deflate();
			}
		}
		out.write(ab1.toByteArray());*/

		// Compress the bytes
		byte[] input1 = linea.getBytes("UTF-8");
		byte[] output1 = new byte[100];
		int compressedDataLength1 =0;
		int modo_deflater=1;
		int err=0;
		switch (modo_deflater) {
			case 0:
				//Deflater
				Deflater compresser1 = new Deflater(Deflater.BEST_SPEED, nowrap);
				compresser1.setInput(input1);
				compresser1.finish();
				compressedDataLength1 = compresser1.deflate(output1, 0, output1.length, Deflater.FULL_FLUSH);
				//int compressedDataLength1 = compresser1.deflate(output1);
				compresser1.end();
				break;
			case 1:
				//JZlip
				com.jcraft.jzlib.Deflater deflaterJZlib1 = null;
				try {
					deflaterJZlib1 = new com.jcraft.jzlib.Deflater(JZlib.Z_DEFAULT_COMPRESSION, nowrap);
				} catch (GZIPException e) {
					// never happen, because argument is valid.
				}
				deflaterJZlib1.setInput(input1);
				deflaterJZlib1.setOutput(output1);
				//deflaterJZlib.total_in=input1.length;
				//deflaterJZlib.avail_out=output1.length;

				while(deflaterJZlib1.total_in!=input1.length &&
						deflaterJZlib1.total_out<output1.length){
					//deflaterJZlib.avail_in=deflaterJZlib.avail_out=1; // force small buffers
					err=deflaterJZlib1.deflate(JZlib.Z_SYNC_FLUSH);
					CHECK_ERR(deflaterJZlib1, err, "deflate");
				}

				/*while(true){
					deflaterJZlib1.avail_out=1;
					err=deflaterJZlib1.deflate(JZlib.Z_FINISH);
					if(err==JZlib.Z_STREAM_END)break;
					CHECK_ERR(deflaterJZlib1, err, "deflate");
				}

				err=deflaterJZlib1.end();
				CHECK_ERR(deflaterJZlib1, err, "deflateEnd");*/
				compressedDataLength1 = (int) deflaterJZlib1.total_out;
				break;
		}

		compressedDataLength1 = compressedDataLength1 ;
		print_Hex(output1,compressedDataLength1);
		out.write(output1,0,compressedDataLength1);

		long size = input1.length;
		long csize= compressedDataLength1;
		long crcv=0;

		byte[] input2 ; //= linea.getBytes("UTF-8");
		byte[] output2 = new byte[100];
		int compressedDataLength2 = 0;

		if(!uno) {
			linea = "1234567890123456789012345678901234567890P";
			//linea = "XXXXXXXXXXXXXXXXXXXXXXXXXX";
			input2 = linea.getBytes("UTF-8");
			crcD2 = new ParallelCRC32();
			//Segundo chunck
			CRCUpdate(linea, crcD2);
			switch (modo_deflater) {
				case 0:
					//Deflater
					Deflater compresser2 = new Deflater(Deflater.BEST_SPEED, nowrap);
					compresser2.setInput(input2);
					compresser2.finish();
					compressedDataLength2 = compresser2.deflate(output2);
					compresser2.end();
					break;
				case 1:
					//JZlip
					com.jcraft.jzlib.Deflater deflaterJZlib2 = null;
					try {
						deflaterJZlib2 = new com.jcraft.jzlib.Deflater(JZlib.Z_DEFAULT_COMPRESSION, nowrap);
					} catch (GZIPException e) {
						// never happen, because argument is valid.
					}
					deflaterJZlib2.setInput(input2);
					deflaterJZlib2.setOutput(output2);
					//deflaterJZlib2.total_in=input1.length;
					//deflaterJZlib2.avail_out=output1.length;

					while(deflaterJZlib2.total_in!=input2.length &&
							deflaterJZlib2.total_out<output2.length){
						//deflaterJZlib.avail_in=deflaterJZlib.avail_out=1; // force small buffers
						err=deflaterJZlib2.deflate(JZlib.Z_SYNC_FLUSH);
						CHECK_ERR(deflaterJZlib2, err, "deflate");
					}

				while(true){
					deflaterJZlib2.avail_out=1;
					err=deflaterJZlib2.deflate(JZlib.Z_FINISH);
					if(err==JZlib.Z_STREAM_END)break;
					CHECK_ERR(deflaterJZlib2, err, "deflate");
				}

					err=deflaterJZlib2.end();
					CHECK_ERR(deflaterJZlib2, err, "deflateEnd");
					compressedDataLength2 = (int) deflaterJZlib2.total_out;
					break;
			}
			print_Hex(output2,compressedDataLength2);
			out.write(output2, 0, compressedDataLength2);

/*		if(false) { //false - sólo una linea; true - las dos
			CRCUpdate(linea,crcD2);
			deflateOut2.write(linea.getBytes(), 0, linea.length());
			if (!deflater2.finished()) {
				deflater2.finish();
				while (!deflater2.finished()) {
					// razón de la existencia de ParallelDeflaterOutputStream
					deflateOut2.deflate();
				}
			}
			out.write(ab2.toByteArray());
		}*/

			size = size + input2.length;
			csize = csize + compressedDataLength2;
			crcv = crcD1.combine(crcD1.getValue(), crcD2.getValue(), input2.length);// crcD.getValue() ;//cache porque al usarlo se pierde el valor del CRC
		} else {
			crcv = crcD1.getValue();
		}

		//Footer LOC
		/*long size = deflater.getBytesRead()+ deflater2.getBytesRead();
		long csize= deflater.getBytesWritten()+ deflater2.getBytesWritten();//cache porque al usarlo se pierde el valor de getBytesWritten
		long crcv = crcD.getValue(); //crcD.combine(crcD.getValue(),crcD2.getValue(),deflater2.getBytesRead());// crcD.getValue() ;//cache porque al usarlo se pierde el valor del CRC*/
		//ParallelZipOutputStream -> para hacer que funcione el closeEntry
		//1- extender ZipOutputStream para tener un setCRC32, que estableceríamos con el crcD
		//zipStream.setCrc(crcD);
		//2-usar un DeflaterOutputStream extendido, que a su vez use un Deflater extendido, al que se le pueda establecer BytesRead y BytesWritten
		// De esta forma sería -> zipStream.closeEntry();
		//En vez de:
		zipStream.closeEntry(size, csize,crcv);
		// Decompress the bytes
		byte[] c = new byte[output1.length + output2.length];
		System.arraycopy(output1, 0, c, 0, compressedDataLength1);
		System.arraycopy(output2, 0, c, compressedDataLength1, compressedDataLength2);
		print_Hex(c,(int) csize);
		print_Inflate(c,csize);
		/*ParallelDeflater d_closeentry = new ParallelDeflater(Deflater.DEFAULT_COMPRESSION, true);
		ParallelDeflaterOutputStream dos_closeentry = new ParallelDeflaterOutputStream(out,deflater);

		zipStream.setCrc(crcD);
		d_closeentry.setBytesRead(deflater.getBytesRead());
		d_closeentry.setBytesWritten(csize);
		zipStream.closeEntry();*/

		//Central Directory
		entry.setSize(size);
		entry.setCompressedSize(csize);
		entry.setCrc(crcv);
		zipStream.close();
	}
	static void CHECK_ERR(com.jcraft.jzlib.Deflater z, int err, String msg) {
		if(err!=JZlib.Z_OK){
			if(z.msg!=null) System.out.print(z.msg+" ");
			System.out.println(msg+" error: "+err);
			System.exit(1);
		}
	}
	static void print_Hex(byte[] output, int compressedDataLength) {
		System.out.print("[");
		String espacio="";
		for (int i = 0; i < compressedDataLength; i++) {
			System.out.print(espacio + String.format("%02X", output[i]));
			espacio=" ";
		}
		System.out.println("]");
	}
	static void print_Inflate(byte[] output, long compressedDataLength) {
		Inflater inflater = new Inflater(nowrap);
		inflater.setInput(output, 0, (int) compressedDataLength);
		byte[] result = new byte[1024];
		int resultLength =0;
		try {
			resultLength = inflater.inflate(result);

		} catch (DataFormatException e)
		{}
		inflater.end();
		// Decode the bytes into a String
		String message ="";
		try {
			message = new String(result, 0, resultLength, "UTF-8");
		}
		catch (UnsupportedEncodingException e)
			{}
		System.out.println("UnCompressed Message : " + message);
		System.out.println("UnCompressed Message length : " + message.length());

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

		BlockingQueue queueLast = new ArrayBlockingQueue(1);

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
		Thread.currentThread().setPriority(Thread.MAX_PRIORITY); //.setPriority(10);
		read_file(queue ,tamano_files, numero_workers, workerId, rutaArchivo, leer, cuenta, modo_modif, queuefile);

		/*try {
			while (true) {
				String buf_linea = (String) queueread.take();
				switch (buf_linea) {
					case "STOP":
						read_file(queue ,1, 1, workerId, rutaArchivo, leer, cuenta, modo_modif, queuefile);
						return;
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}*/
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
			//buf_linea = "1234567890123456789012345678901234567890P";
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
				case "5":
					linea = Arrays.asList(buf_linea.substring(0, 60),
							buf_linea.substring(61, 121),
							buf_linea.substring(122, 152),
							buf_linea.substring(153, 172),
							buf_linea.substring(173, 174),
							buf_linea.substring(175, 176),
							buf_linea.substring(177, 190),
							buf_linea.substring(191, 201)
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
	private String buf_linea_anterior;

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
		else {
			//true -> usa ZipOutputStream, requiere un único threath
			//false -> usa JZlib, aunque ahora se implementa en un único threath, aceptaría varios treaths en paralelo para comprimir.
				//Las pruebas preliminales (1 threath) han dado un pobre rendimiento
			if (true){
				fdatos.abreFicheroW_zipSplit();
			} else {
				fdatos.abreFicheroW_zipManual();
			}
		}
	}

	public void run() {
		Thread.currentThread().setPriority(Thread.MAX_PRIORITY); //.setPriority(10);
		int cont_bloques=0;
		int cont_temp_lineas_estadistica=0;
		long iniDuracion = System.currentTimeMillis();
		try {
			while (true) {
				String buf_linea = (String) queue.take();


					long iniProceso = System.nanoTime();
					cont_bloques = cont_bloques + 1;
					cont_temp_lineas_estadistica = cont_temp_lineas_estadistica + 1;
					//if (buf_linea.equals("STOP") || buf_linea.equals("FICHERO")) {
					switch (buf_linea) {
						case "STOP":
							write_file("STOP",false);
							if (cuenta_stop == numero_workers) {
								write_file(buf_linea_anterior,true);
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
							write_file("FICHERO",false);
							if (cuenta_fichero == numero_workers) {
								System.out.println("     Lineas escritas por transformer: " + cont_temp_lineas_estadistica);
								flog.writeLine(new ArrayList<String>(Arrays.asList("     Lineas escritas por bloque transformer: " + cont_temp_lineas_estadistica)));
								cont_temp_lineas_estadistica = 0;
								cuenta_fichero = 0;
							}
							break;
						default:
							if (buf_linea_anterior==null) {
								buf_linea_anterior = buf_linea;
							} else {
								String buf_tmp = buf_linea;
								buf_linea = buf_linea_anterior;
								buf_linea_anterior = buf_tmp;
								write_file(buf_linea,false);
							}
							break;
					}
					//	}
					long finProceso = System.nanoTime();
					t_proceso = t_proceso + (finProceso - iniProceso);
					if (cont_bloques == tamano_file) {
						long finDuracion = System.currentTimeMillis();
						//t_duracion = t_duracion + (finDuracion-iniDuracion);
						cont_bloques = 0;
						System.out.println("     SOLO Escribir Fichero: " + t_proceso / 1e6 + " milisegundos");
						System.out.println("     Duracion escribir Fichero: " + (finDuracion - iniDuracion) + " milisegundos");
						flog.writeLine(new ArrayList<String>(Arrays.asList("     SOLO Escribir Fichero: " + t_proceso / 1e6 + " milisegundos")));
						flog.writeLine(new ArrayList<String>(Arrays.asList("     Duracion escribir Fichero: " + (finDuracion - iniDuracion) + " milisegundos")));
						t_proceso = 0;
						iniDuracion = System.currentTimeMillis();
					}
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	public void write_file( String linea, boolean ultimo) {
		if (ultimo) {
			fdatos.writeLine(linea,true);
		} else
		switch (linea) {
			case "STOP":
				cuenta_stop = cuenta_stop + 1;
				break;
			case "FICHERO":
				cuenta_fichero = cuenta_fichero + 1;
				break;
			default:
				fdatos.writeLine(linea,false);
		}
	}
}