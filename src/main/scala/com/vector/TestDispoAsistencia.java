package com.vector;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.summary.ResultSummary;

import java.text.DecimalFormat;
import java.util.concurrent.ThreadLocalRandom;

public class TestDispoAsistencia {

    public static void main(String[] args) {
        String mode;
        if (args.length == 0) {
            mode = "basico";
        } else {
            mode = args[0];
        }
        getDispo_test(args, 10000, "", "STREAM");
    }

    public static void getDispo_test(String[] args, long numero_de_llamadas, String nodos_por_llamada, String modo) {

        Driver driver = LauncherExplo.getDriver("localhost", "neo4j", "vector-itcgroup");
        Session session = driver.session();

       // Random Zona Código Postal;
        int randomNum = ThreadLocalRandom.current().nextInt(1, 13999 + 1);
        String cdp="CDP_"+randomNum;
        long veces = 0;
        Result resultSeti = session.run("call multiA.getDispoOrder('"+cdp+"','caracteristica1','Gremio1','Aseguradora_1','"+modo+"')");
        long iniTotal = System.currentTimeMillis();
        int count =0;
        while(veces <numero_de_llamadas) {
            //long iniParcial = System.currentTimeMillis();
            Result resultSet = session.run("call multiA.getDispoOrder('"+cdp+"','caracteristica1','Gremio1','Aseguradora_1','"+modo+"')");
            //int count =0;
            while (resultSet.hasNext()) {
                Record result = resultSet.next();
                count=count+1;
                //System.out.println(result.get("proveedor").asString() +","+result.get("anomes").asString()+","+result.get("workday").asString()+","+result.get("workslot").asString());
            }
            //System.out.println("Proveedores: "+count);
            //long finParcial = System.currentTimeMillis();
            //System.out.println("Tiempo en llamada " + String.valueOf(veces+1) +	": " + (finParcial - iniParcial) + " milisegundos ; tiempo acumulado: " + (finParcial - iniTotal));
            veces = veces +1;
        }
        long finTotal = System.currentTimeMillis();
        System.out.println("---------------------------------------------------------------------------------------");
        DecimalFormat df = new DecimalFormat("#,###");
        String formatted = df.format(numero_de_llamadas);

        DecimalFormat ds = new DecimalFormat("#.##");
        float d = ((float)(finTotal - iniTotal)/1000);
        String tiempo = ds.format(d);
        System.out.println("Tiempo Total de una ejecución de " +formatted+" peticiones de Disponibilidad ->" + tiempo + " segundos " );

        float pps = (float) numero_de_llamadas/( (float) (finTotal - iniTotal)/1000);
        formatted = df.format(pps);
        System.out.println("Cálculo de Peticiones x Segundo: " +formatted);

        formatted = df.format(count);
        System.out.println("En total se han devuelto: "+formatted+ " huecos de proveedores");
        driver.close();
    }

}
