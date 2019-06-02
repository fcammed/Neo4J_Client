package com.vector;

import com.vector.repo.RepoFile;

import java.util.ArrayList;
import java.util.Arrays;

public class WriteTest {

	public static void main(String[] args) {
		write_test(args,264,(200*1024*1024/201) ,"0",1,1, "","true");
	}

	public WriteTest(String[] args,int n_files,int tamano_files,String sobreescribir, int numero_workers,int workerId, String rutaArchivo, String zipped) {
		write_test(args,n_files,tamano_files,sobreescribir, numero_workers,workerId,rutaArchivo,zipped);
	}

	public static void write_test(String[] args,int n_files,int tamano_files,String sobreescribir, int numero_workers,int workerId, String rutaArchivo, String zipped) {
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
			if (sobreescribir.equals("1")) //Sobreescribe siempre el mismo fichero para no ocupar espacio en disco
				fdatos = new RepoFile(workerId, "|", workerId, rutaArchivo,false);
			else
				fdatos = new RepoFile(cuenta, "|", workerId, rutaArchivo, true);
			if(!zipped.equals("true"))
				fdatos.abreFicheroW();
			else
				fdatos.abreFicheroW_zip();

			String linea ="P|B001000811538362||7|0|null|null|00|P18354008|20190101|20190630|0000|2359|B|null|null|P - PARCEIROS HOTEIS|0040401063771|P - PARCEIROS HOTEIS GAIA                         |1  |                        ";
			int nlineas=0;
			while (nlineas<tamano_files) {
				nlineas = nlineas + 1;
				fdatos.writeLineString(linea);
			}
			fdatos.cierraFichero();
			long finResultP = System.currentTimeMillis();
			System.out.println("Tiempo empleado en escribir fichero " + String.valueOf(cuenta) +
					": " + (finResultP - primerResultP)+ " milisegundos");
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
