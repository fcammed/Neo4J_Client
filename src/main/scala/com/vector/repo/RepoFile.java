package com.vector.repo;

import com.vector.model.AppliedPromotion;

import java.io.*;
import java.nio.file.attribute.FileTime;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class RepoFile {
	private String nombreArchivo;
	private String rutaArchivo;
	private BufferedWriter out = null;
	private BufferedReader br = null;
	private ZipOutputStream zipStream;
	private boolean isZipped = false;
	//= new ZipOutputStream(
	//		new FileOutputStream(zipFileName))

	private String separador_field = "|";
	private static final String separador_registro = "\n";

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

			/*Writer fstream = new OutputStreamWriter(new FileOutputStream(rutaArchivo), "UTF8");
			out = new BufferedWriter(fstream);*/
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

	public void writeLine(AppliedPromotion mbr) {
		try {
			out.write(mbr.getCanalVta() + separador_field +
					mbr.getcCarpeta() + separador_field +
					mbr.getcCenVta()+ separador_field +
					mbr.getcEmprVta() + separador_field +
					mbr.getcFabrica()+ separador_field +
					mbr.getcHoraFin() + separador_registro);
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

	public void writeLineString(String linea) {
		try {
			if (!isZipped)
				out.write(linea);
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
			count = 0;
			for (String field:mbl
			) {
				if (count != 0)
					out.write(separador_field);
				if (field==null)
					out.write("");
				else
					if (field=="null")
						out.write("");
					else
						out.write(field);
				count = count +1;
			}
			out.write(separador_registro);
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
				out.write(separador_field);
				out.write(barra);
				out.write(separador_field);
				count = count +1;
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
				out.write(separador_registro);
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
				out.write(separador_registro);
			}
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
	}

	public void cierraFichero() {

		try {
			if(!isZipped) {
				if (out != null) {
					out.close();
				}
			}
			else {
				if(this.zipStream != null) {
					zipStream.closeEntry();
					zipStream.close();
				}
			}
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
	}
/*
	public boolean readLine(modelLocalDccRegistry linea, boolean leer) {
		boolean result = true;
		try {
			String line = br.readLine();
			if (line != null) {
				if (leer) {
					int count = 0;
					String[] fields = line.split(separador_field, -1);
					for (String contenido : fields) {
						switch (count) {
							case 0:
								linea.setLocal_electoral(contenido);
								break;
							case 1:
								linea.setNombre_local(contenido);
								break;
							case 2:
								linea.setDcc_local(contenido);
								break;
						}
						count = count + 1;
					}
				}
			}  else {
				result = false; }
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
		return result;
	}

	public boolean readLine(modelCandidaturaMuniRegistry linea, boolean leer) {
		boolean result = true;
		try {
			String line = br.readLine();
			if (line != null) {
				if (leer) {
					int count = 0;
					String[] fields = line.split(separador_field, -1);
					for (String contenido : fields) {
						switch (count) {
							case 0:
								linea.setCodigo_municipio(contenido);
								break;
							case 1:
								linea.setCandidatura(contenido);
								break;
							case 2:
								linea.setSiglas(contenido);
								break;
							case 3:
								linea.setPeso(contenido);
								break;
						}
						count = count + 1;
					}
				}
			}  else {
				result = false; }
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
		return result;
	}

	public boolean readLine(modelMesasRepRegistry linea, boolean leer) {
		boolean result = true;
		try {
			String line = br.readLine();
			if (line != null) {
				if (leer) {
					int count = 0;
					String[] fields = line.split(separador_field, -1);
					for (String contenido : fields) {
						switch (count) {
							case 0:
								linea.setCodigo_local(contenido);
								break;
							case 1:
								linea.setNombre_local(contenido);
								break;
							case 2:
								linea.setMunicipio(contenido);
								break;
							case 3:
								linea.setCodigo_mesa(contenido);
								break;
							case 4:
								linea.setNif(contenido);
								break;
							case 5:
								linea.setPrimer_apellido(contenido);
								break;
							case 6:
								linea.setSegundo_apellido(contenido);
								break;
							case 7:
								linea.setNombre(contenido);
								break;
							case 8:
								linea.setTelefono_movil(contenido);
								break;
							case 9:
								linea.setTelefono_alternativo(contenido);
								break;
							case 10:
								linea.setCodigo_representante(contenido);
								break;
							case 11:
								linea.setCoordinador(contenido);
								break;
							case 12:
								linea.setN_dispositivo(contenido);
								break;
							case 13:
								linea.setRef(contenido);
								break;
						}
						count = count + 1;
					}
				}
			}  else {
				result = false; }
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
		return result;
	}

	public boolean readLine(modelMesasLocalRegistry linea, boolean leer) {
		boolean result = true;
		try {
			String line = br.readLine();
			if (line != null) {
				if (leer) {
					int count = 0;
					String[] fields = line.split(separador_field, -1);
					for (String contenido : fields) {
						switch (count) {
							case 0:
								linea.setLocal_electoral(contenido);
								break;
							case 1:
								linea.setNombre_local(contenido);
								break;
							case 2:
								linea.setMunicipio(contenido);
								break;
							case 3:
								linea.setCodigo_municipio(contenido);
								break;
							case 4:
								linea.setCodigo_mesa(contenido);
								break;
							case 5:
								linea.setCer(contenido);
								break;
						}
						count = count + 1;
					}
				}
			}  else {
				result = false; }
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
		return result;
	}

	public boolean readLine(modelResultadoMunRegistry linea, boolean leer) {
		boolean result = true;
		try {
			String line = br.readLine();
			if (line != null) {
				if (leer) {
					int count = 0;
					String[] fields = line.split(separador_field, -1);
					for (String contenido : fields) {
						switch (count) {
							case 0:
								linea.setConvocatoria(contenido);
								break;
							case 1:
								linea.setCcaa(contenido);
								break;
							case 2:
								linea.setCodProv(contenido);
								break;
							case 3:
								//linea.setCodigo_municipio(contenido);
								break;
							case 4:
								linea.setMesa(contenido);
								break;
							case 5:
								linea.setVotosAvance1(contenido);
								break;
							case 6:
								linea.setVotosAvance2(contenido);
								break;
							case 7:
								linea.setVotantes(contenido);
								break;
							case 8:
								linea.setVotosBlanco(contenido);
								break;
							case 9:
								linea.setVotosNulos(contenido);
								break;
							case 10:
								linea.setVotosCandidaturas(contenido);
								break;
							case 11:
								linea.setCandidaturas(contenido);
								break;
						}
						count = count + 1;
					}
				}
			}  else {
				result = false; }
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		}
		return result;
	}
*/
}
