package com.vector.repo;

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
import java.util.zip.*;

public class RepoFile {
	private String nombreArchivo;
	private String rutaArchivo;
	private BufferedWriter out = null;
	private BufferedReader br = null;
	private ZipOutputStream zipStream;
	private DeflaterOutputStream deflaterStream;
	private Deflater zipDeflater;
	private ByteArrayOutputStream outputStream;
	private boolean isZipped = false;
	private boolean isZippedSplit = false;
	private CompressionUtil cutil = null;
	private CRCInputStream crc;
	private GZIPHeader gzHeader;
	private final int nbuffer=5 ;
	private String buffer="";
	private int clineas=0;
	//= new ZipOutputStream(
	//		new FileOutputStream(zipFileName))

	private String separador_field = "|";
	private static final String separador_registro = "\n\r";

	public RepoFile(String nombreArchivo, String rutaArchivo, String separador_field  ) {
		this.nombreArchivo = nombreArchivo;
		this.rutaArchivo = rutaArchivo + "\\"+nombreArchivo ;
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
			Writer fstream = new OutputStreamWriter(new FileOutputStream(rutaArchivo), "UTF8");
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
			//0
			//CRCInputStream crc = new CRCInputStream(is);
			/*outputStream = new ByteArrayOutputStream();
			FileOutputStream fos = new FileOutputStream(rutaArchivo+".zip");
			Writer fstream = new OutputStreamWriter(fos);
			out = new BufferedWriter(fstream);

			crc = new CRCInputStream();
			gzHeader = new GZIPHeader();
			gzHeader.writeBytes(outputStream);
			out.write(outputStream.toString());
			outputStream.reset();

			zipDeflater = new Deflater(Deflater.BEST_SPEED,true);
			deflaterStream = new DeflaterOutputStream(fos, zipDeflater);
			isZippedSplit = true;*/

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

			//2
			/*outputStream = new ByteArrayOutputStream();
			Writer fstream = new OutputStreamWriter(new FileOutputStream(rutaArchivo+".zip"));
			out = new BufferedWriter(fstream);
			zipStream = new ZipOutputStream(outputStream);
			isZippedSplit = true;
			zipStream.setMethod(ZipOutputStream.DEFLATED);
			ZipEntry entry = new ZipEntry(nombreArchivo);
			//entry.setCreationTime(FileTime.fromMillis(file.toFile().lastModified()));
			entry.setComment("Fichero de salida zip");
			zipStream.putNextEntry(entry);
			out.write(outputStream.toString());*/


			//outputStream.reset();
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

	public void writeLine(String linea) {
		try {
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

}
