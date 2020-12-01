package com.vector.repo;

import com.jcraft.jzlib.GZIPException;
import com.jcraft.jzlib.JZlib;
import com.vector.model.AppliedPromotion;
import com.vector.service.CompressionUtil;
import com.vector.service.zip.CRCInputStream;
import com.vector.service.zip.GZIPFooter;
import com.vector.service.zip.GZIPHeader;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.attribute.FileTime;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import com.vector.service.zip.original.ZipEntry;
import com.vector.service.zip.original.ZipOutputStream;
//java.util.zip
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

//Version de prueba con
import com.vector.service.zip.ParallelCRC32;
import org.apache.commons.compress.archivers.zip.*;
import org.jooq.SelectSeekLimitStep;

public class RepoFile {
	private String nombreArchivo;
	private String rutaArchivo;
	private BufferedWriter out = null;
	private BufferedReader br = null;
	private FileOutputStream fos = null;
	private ZipOutputStream zipStream;
	private ZipEntry zipEntry;
	private DeflaterOutputStream deflaterStream;
	private Deflater zipDeflater;
	private ByteArrayOutputStream outputStream;
	private boolean isZipped = false, isZippedSplit = false, isZippedManual=false, isZippedParallel=false;
	private CompressionUtil cutil = null;
	private CRCInputStream crc;
	private GZIPHeader gzHeader;
	private final int nbuffer=5 ;
	private String buffer="";
	private int clineas=0;
	//= new ZipOutputStream(
	//		new FileOutputStream(zipFileName))
	long size = 0;
	long csize= 0;
	long crcv=0;
	ParallelCRC32 crcD = null;
	private boolean primera_linea_CRC = true;
	private static final int long_output_buffer = 1024;

	ParallelCRC32 crcD_part = null;
	byte[] output = null;
	int compressedDataLength =0;
	int err=0;
	com.jcraft.jzlib.Deflater deflaterJZlib = null;

	private String separador_field = "|";
	private static final String separador_registro = "\n\r";

	public RepoFile(String nombreArchivo, String rutaArchivo, String separador_field  ) {
		if (rutaArchivo.equals("")) rutaArchivo = "C:/DATOSP~1/Vector/PoC/PLAYPR~1/poc-bigfiles-upload/Uploadfiles/explosionado/";
		this.nombreArchivo = nombreArchivo;
		this.rutaArchivo = rutaArchivo + "/"+nombreArchivo ;
		this.separador_field = separador_field;

	}

	public RepoFile( long input_sufijo, String separador_field , int workerId, String rutaArchivo ) {
		if (rutaArchivo.equals("")) rutaArchivo = "C:/DATOSP~1/Vector/PoC/PLAYPR~1/poc-bigfiles-upload/Uploadfiles/explosionado/";
		SimpleDateFormat formatf = new java.text.SimpleDateFormat("YYYMMdd_hhmmss");
		String sufijo = "";
		String nombreArchivolog = "";
		if (input_sufijo==0) {
			sufijo = formatf.format(System.currentTimeMillis());
			nombreArchivolog =  "explosionadoECI_log" + sufijo + workerId + ".csv";
		} else {
			sufijo	= new Long(input_sufijo).toString();
			nombreArchivolog =  "explosionadoECI" + sufijo + ".csv";
		}
		this.nombreArchivo = nombreArchivolog;
		this.rutaArchivo = rutaArchivo + "/"+nombreArchivolog;
		this.separador_field = separador_field;
	}

	public RepoFile( long input_sufijo, String separador_field , int workerId, String rutaArchivo, boolean separar_por_workerId ) {

		cutil = new CompressionUtil();
		crcD_part = new ParallelCRC32();
		output = new byte[long_output_buffer];
		try {
			deflaterJZlib = new com.jcraft.jzlib.Deflater(JZlib.Z_DEFAULT_COMPRESSION, true);
		} catch (GZIPException e) {
			// never happen, because argument is valid.
		}
		deflaterJZlib.setOutput(output);

		if (rutaArchivo.equals("")) rutaArchivo = "C:/DATOSP~1/Vector/PoC/PLAYPR~1/poc-bigfiles-upload/Uploadfiles/explosionado/";
		SimpleDateFormat formatf = new java.text.SimpleDateFormat("YYYMMdd_hhmmss");
		String sufijo = "";
		String nombreArchivolog = "";
		if (input_sufijo==0) {
			sufijo = formatf.format(System.currentTimeMillis());
			nombreArchivolog =  "explosionadoECI_log" + sufijo + workerId + ".csv";
		} else {
			sufijo	= new Long(input_sufijo).toString();
			if (separar_por_workerId)
				nombreArchivolog =  "explosionadoECI" + sufijo + "_" + workerId + ".csv";
			else
				nombreArchivolog =  "explosionadoECI" + sufijo + ".csv";
		}
		this.nombreArchivo = nombreArchivolog;
		this.rutaArchivo = rutaArchivo + "/"+nombreArchivolog;
		this.separador_field = separador_field;
	}

	public RepoFile( long input_sufijo, String separador_field , int workerId, String rutaArchivo, boolean separar_por_workerId, boolean leer ) {
		if (rutaArchivo.equals("")) rutaArchivo = "C:/DATOSP~1/Vector/PoC/PLAYPR~1/poc-bigfiles-upload/Uploadfiles/explosionado/";
		SimpleDateFormat formatf = new java.text.SimpleDateFormat("YYYMMdd_hhmmss");
		String sufijo = formatf.format(System.currentTimeMillis());
		String nombreArchivolog = "explosionadoECI_data.csv";
		if (leer)
			nombreArchivolog = "explosionadoECI_data.csv";
		else
			if (separar_por_workerId)
				nombreArchivolog = "explosionadoECI_log"+ sufijo+"_R.csv";
			else
				nombreArchivolog = "explosionadoECI_log"+sufijo+"_W.csv";
		this.nombreArchivo = nombreArchivolog;
		this.rutaArchivo = rutaArchivo + "/"+nombreArchivolog;
		this.separador_field = separador_field;
	}

	public String getRutaArchivo() {
		return rutaArchivo;
	}

	public String getSeparador_field() {
		return separador_field;
	}

	public void setSeparador_field(String separador_field) {
		this.separador_field = separador_field;
	}

	public void abreFicheroW() {
		try {
			//Writer fstream = new OutputStreamWriter(new FileOutputStream(rutaArchivo), "windows-1252");
			//File file;
			fos = new FileOutputStream(rutaArchivo);
			Writer fstream = new OutputStreamWriter(fos, "UTF8");
			out = new BufferedWriter(fstream);
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

	public void abreFicheroW_zip() {
		try {
			zipStream = new ZipOutputStream(new FileOutputStream(rutaArchivo+".zip"));
			isZipped = true;
			zipStream.setMethod(ZipOutputStream.DEFLATED);

			ZipEntry entry = new ZipEntry(nombreArchivo);
			//entry.setCreationTime(FileTime.fromMillis(file.toFile().lastModified()));
			entry.setComment("Fichero de salida zip");
			zipStream.putNextEntry(entry);

			Writer fstream = new OutputStreamWriter(new FileOutputStream(rutaArchivo), "UTF8");
			out = new BufferedWriter(fstream);
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

	public void abreFicheroW_zipSplit() {
		try {
			clineas=0;
			//1
			zipStream = new ZipOutputStream(new FileOutputStream(rutaArchivo+".zip"));
			isZippedSplit = true;
			zipStream.setMethod(ZipOutputStream.DEFLATED);
			zipStream.setLevel(1);
			ZipEntry entry = new ZipEntry(nombreArchivo);
			//entry.setCreationTime(FileTime.fromMillis(file.toFile().lastModified()));
			entry.setComment("Fichero de salida zip");
			zipStream.putNextEntry(entry);
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

	public void abreFicheroW_zipManual() {
		try {
			//0
			outputStream = new ByteArrayOutputStream();
			fos = new FileOutputStream(rutaArchivo + ".zip");
			Writer fstream = new OutputStreamWriter(fos);
			out = new BufferedWriter(fstream);
			if (true) {
				zipStream = new ZipOutputStream(fos);
				zipStream.setMethod(ZipOutputStream.DEFLATED);
				zipStream.setLevel(1);

				//Header LOC
				this.zipEntry = new ZipEntry(nombreArchivo);
				zipEntry.setComment("Fichero de salida zip");
				zipStream.putNextEntry(zipEntry);
				crcD = new ParallelCRC32();
			}
			else {
				/*crc = new CRCInputStream();
				gzHeader = new GZIPHeader();
				gzHeader.writeBytes(outputStream);
				out.write(outputStream.toString());
				outputStream.reset();

				zipDeflater = new Deflater(Deflater.BEST_SPEED, true);
				deflaterStream = new DeflaterOutputStream(fos, zipDeflater);*/
			}
			clineas = 0;
			isZippedManual = true;
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

	public void flush() {
		try {
			this.out.flush();
			/*if(!isZipped) {
				if (out != null) {
					out.close();
				}
			}
			else {
				if(this.zipStream != null) {
					zipStream.closeEntry();
					zipStream.close();
				}
			}*/
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

	public boolean abreFicheroR() {
		boolean result = true;
		try {
			Reader fr = new InputStreamReader(new FileInputStream(rutaArchivo), "UTF8");;
			br = new BufferedReader(fr);
		} catch (IOException e) {
			result = false;
			System.err.println("Error: " + e.getMessage());
		}
		return result;
	}

	public String readLine () {
		String result = "";
		try {
			result = br.readLine();
		//br.lines().collect(Collectors.joining());
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
		return result;
	}

	public void writeLine(AppliedPromotion mbr) {
		try {
			out.write(mbr.getCanalVta() + separador_field +
					mbr.getcCarpeta() + separador_field +
					mbr.getcCenVta()+ separador_field +
					mbr.getcEmprVta() + separador_field +
					mbr.getcFabrica()+ separador_field +
					mbr.getcHoraFin() ); //+ separador_registro);
			out.newLine();
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

	public void writeLine(String linea,boolean ultimo) {
		try {
			if (isZippedManual) {
				clineas = clineas + 1;
				buffer = buffer + linea+ "\r\n";
				if (clineas>=nbuffer)
				{

					{
						//ParallelCRC32 crcD_part = null;
						//crcD_part = new ParallelCRC32();
						CRCUpdate(buffer, crcD_part);
						// Compress the bytes
						byte[] input = buffer.getBytes("UTF-8");
						//byte[] output = new byte[long_output_buffer];
						//int compressedDataLength =0;
						//int err=0;
						//JZlip
				/*com.jcraft.jzlib.Deflater deflaterJZlib = null;
				try {
					deflaterJZlib = new com.jcraft.jzlib.Deflater(JZlib.Z_DEFAULT_COMPRESSION, true);
				} catch (GZIPException e) {
					// never happen, because argument is valid.
				}*/
						deflaterJZlib.setInput(input);
						//deflaterJZlib.setOutput(output);
						while(deflaterJZlib.total_in!=input.length &&
								deflaterJZlib.total_out<output.length){
							//deflaterJZlib.avail_in=deflaterJZlib.avail_out=1; // force small buffers
							err=deflaterJZlib.deflate(JZlib.Z_SYNC_FLUSH);
							//CHECK_ERR(deflaterJZlib, err, "deflate");
						}

						if (ultimo) {
							while (true) {
								deflaterJZlib.avail_out = 1;
								err = deflaterJZlib.deflate(JZlib.Z_FINISH);
								if (err == JZlib.Z_STREAM_END) break;
								//CHECK_ERR(deflaterJZlib, err, "deflate");
							}
							err = deflaterJZlib.end();
							//CHECK_ERR(deflaterJZlib, err, "deflateEnd");
						}

						compressedDataLength = (int) deflaterJZlib.total_out;

						//combinar
						size = size + input.length;
						csize = csize + compressedDataLength;

						if(primera_linea_CRC) {
							crcv = crcD_part.getValue();
							primera_linea_CRC = false;
						} else
							crcv = crcD.combine(crcv, crcD_part.getValue(), input.length);
						//escribir salida
						fos.write(output, 0, compressedDataLength);
					}

					buffer="";
					clineas = 0;

				}

			} else
			if (!isZipped)
				if (!isZippedSplit)
				{
					out.write(linea);
					out.newLine();
				}
				else {

					clineas = clineas + 1;
					//0
					/*deflaterStream.write(linea.getBytes(), 0, linea.length());
					crc.read(linea.getBytes());*/

					//1
					buffer = buffer + linea+ "\r\n";
					if (clineas>=nbuffer)
					{
						zipStream.write(buffer.getBytes(), 0, buffer.length());
						buffer="";
						clineas = 0;

					}

					//2
					/*zipStream.write(linea.getBytes(), 0, linea.length());
					out.write(outputStream.toString());
					outputStream.reset();*/

					//3
					//byte[] zipeada = zip(linea);
					//out.write(zipeada.toString());


					//4
					//byte[] zipeada =cutil.zipCompression(linea);
					//out.write(zipeada.toString());

					//5
				}

			else {
				//while ((amountRead = inputStream.read(readBuffer)) > 0) {
				zipStream.write(linea.getBytes(), 0, linea.length());
				//	written += amountRead;
				//}
			}
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
	}
	/*public void writeLine(String linea, boolean ultimo) {
		try {
			ParallelCRC32 crcD_part = null;
			crcD_part = new ParallelCRC32();
			CRCUpdate(linea, crcD_part);
			// Compress the bytes
			byte[] input = linea.getBytes("UTF-8");
			byte[] output = new byte[long_output_buffer];
			int compressedDataLength =0;
			int err=0;
			//JZlip
			com.jcraft.jzlib.Deflater deflaterJZlib = null;
			try {
				deflaterJZlib = new com.jcraft.jzlib.Deflater(JZlib.Z_DEFAULT_COMPRESSION, true);
			} catch (GZIPException e) {
				// never happen, because argument is valid.
			}
			deflaterJZlib.setInput(input);
			deflaterJZlib.setOutput(output);
			while(deflaterJZlib.total_in!=input.length &&
					deflaterJZlib.total_out<output.length){
				//deflaterJZlib.avail_in=deflaterJZlib.avail_out=1; // force small buffers
				err=deflaterJZlib.deflate(JZlib.Z_SYNC_FLUSH);
				CHECK_ERR(deflaterJZlib, err, "deflate");
			}
			while (true) {
				deflaterJZlib.avail_out = 1;
				err = deflaterJZlib.deflate(JZlib.Z_FINISH);
				if (err == JZlib.Z_STREAM_END) break;
				CHECK_ERR(deflaterJZlib, err, "deflate");
			}
			err = deflaterJZlib.end();
			CHECK_ERR(deflaterJZlib, err, "deflateEnd");

			compressedDataLength = (int) deflaterJZlib.total_out;

			//combinar
			size = size + input.length;
			csize = csize + compressedDataLength;

			crcv = crcD.combine(crcv, crcD_part.getValue(), input.length);
			//escribir salida
			fos.write(output, 0, compressedDataLength);

			//deflaterStream.write(linea.getBytes(), 0, linea.length());
			//crc.read(linea.getBytes());
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
	}*/

	public void writeLine(List<String> mbl) {
		int count =0;
		try {
			if (!isZipped)
				if (!isZippedSplit)
				{
					count = 0;
					for (String field:mbl
					) {
						if (count != 0)
							out.write(separador_field);
						//out.newLine();
						if (field==null)
							out.write("");
						else
						if (field=="null")
							out.write("");
						else
							out.write(field);
						count = count +1;
					}
					//out.write(separador_registro);
					out.newLine();
				}
				else {
					clineas = clineas + 1;
					count = 0;
					for (String field:mbl
					) {
						if (count != 0)
							buffer = buffer + separador_field;
						//out.newLine();
						if (field==null) {
							buffer = buffer +"";
						}
						else
						if (field=="null") {
							buffer = buffer +"";
						}
						else
							buffer = buffer + field;
						count = count +1;
					}
					//out.write(separador_registro);
					buffer = buffer + "\r\n"; //newLine();

					//buffer = buffer + linea;
					if (clineas>=nbuffer)
					{
						zipStream.write(buffer.getBytes(), 0, buffer.length());
						buffer="";
						clineas = 0;
					}
				}
			else {

			}
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

	public void writeLineofList(String letra, ArrayList<List<String>> mbl, String barra) {
		int count =0;
		try {
			for (List<String> mbr:mbl
			) {
				count = 0;
				out.write(letra);
				out.newLine();
				//out.write(separador_field);
				out.write(barra);
				out.newLine();
				//out.write(separador_field);
				count = count +1;
				for (String field:mbr
				) {
					if (count != 0)
						//out.write(separador_field);
						out.newLine();
					if (field==null)
						out.write("");
					else
						out.write(field);
					count = count +1;
				}
				//out.write(separador_registro);
				out.newLine();
			}
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

	public void writeLineofList(String letra, ArrayList<List<String>> lpd, ArrayList<List<String>> lpc, String barra) {
		writeLineofList(letra, lpd,barra);
		writeLineofList(letra, lpc,barra);
	}

	public void writeLineofList(String letra, ArrayList<List<String>> lpd, ArrayList<List<String>> lpc, ArrayList<List<String>> lpt, String talla) {
		writeLineofList(letra, lpd,talla);
		writeLineofList(letra, lpc,talla);
		writeLineofList(letra, lpt,talla);
	}

	public void writeLineofList(ArrayList<ArrayList<String>> mbl) {
		int count =0;
		try {
			for (ArrayList<String> mbr:mbl
			) {
				count = 0;
				for (String field:mbr
				) {
					if (count != 0)
						out.write(separador_field);
					if (field==null)
						out.write("");
					else
						out.write(field);
					count = count +1;
				}
				//out.write(separador_registro);
				out.newLine();
			}
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

	public void cierraFichero() {

		try {
			if (isZippedManual) {
				//close Zip manual by chunks
				zipStream.closeEntry(size, csize,crcv);

				//creo que redundante
				zipEntry.setSize(size);
				zipEntry.setCompressedSize(csize);
				zipEntry.setCrc(crcv);
				//fin redundante

				zipStream.close();
			} else
			if(!isZipped) {
				if(!isZippedSplit) {
					if (out != null) {
						out.close();
					}
				} else {
					//0
					//deflaterStream.close();

					/*deflaterStream.finish();
					GZIPFooter gzFooter = new GZIPFooter(crc.getCRCValue(), crc.getByteCount());

					gzFooter.writeBytes(outputStream);
					out.write(outputStream.toString());
					outputStream.reset();

					out.flush();
					out.close();*/

					//1
					if (clineas>0) zipStream.write(buffer.getBytes(), 0, buffer.length());
					zipStream.closeEntry();
					zipStream.close();

					//2
					/*zipStream.closeEntry();
					out.write(outputStream.toString());
					zipStream.close();
					out.close();*/

					//3
				}
			}
			else {
				if(this.zipStream != null) {
					//0
					zipStream.close();

					//1
					//zipStream.closeEntry();
					//zipStream.close();
				}
			}
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

	public void cierraFicheroR() {
		try {
			if (br != null) {
				br.close();
			}
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

	public static byte[] zip(final String str) {
		if ((str == null) || (str.length() == 0)) {
			throw new IllegalArgumentException("Cannot zip null or empty string");
		}

		try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {
			try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
				gzipOutputStream.write(str.getBytes(StandardCharsets.UTF_8));
			}
			return byteArrayOutputStream.toByteArray();
		} catch(IOException e) {
			throw new RuntimeException("Failed to zip content", e);
		}
	}

	public BufferedWriter getBW() {
		return out;
	}

	public FileOutputStream getFOS() {
		return fos;
	}

	int CRCUpdate(String linea, ParallelCRC32 crc) throws IOException {
		byte[] b = linea.getBytes();
		if(b.length > -1) {
			crc.update(b, 0, b.length);
		}
		return b.length;
	}

	void CHECK_ERR(com.jcraft.jzlib.Deflater z, int err, String msg) {
		if(err!=JZlib.Z_OK){
			if(z.msg!=null) System.out.print(z.msg+" ");
			System.out.println(msg+" error: "+err);
			System.exit(1);
		}
	}
}
