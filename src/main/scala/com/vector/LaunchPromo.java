package com.vector;

import org.neo4j.driver.v1.*;

import java.util.concurrent.TimeUnit;

public class LaunchPromo {

    public LaunchPromo(String[] args, String total) {
        // TODO Auto-generated constructor stub
        calc(args[0], total);
    }

    public static void main(String[] args) {
        String mode;
        if (args.length == 0) {
            mode = "mio";
        } else {
            mode = args[0];
        }
        calc(mode,"T");
    }

    public static void calc(String arg, String total) {
        String mode = arg;
        if (mode.equals("")) mode = "mio";
        System.out.println("Args: " + mode);

        Driver driver = getDriver();
        Session session = driver.session();
        long iniCacheDiv = System.nanoTime();
        System.out.println("Recrear caches");
        StatementResult resultCa = session.run("CALL eci.loadCaches(\"mio\")");
        boolean finish = false;
        finish = resultCa.single().get("atEnd", false);
        if (finish) System.out.println("Recreada caches OK");
        else System.out.println("ERROR - Recreada caches KO");
        //session.run("CALL eci.initPostgresPromotionsImport(\"" + mode + "\",\"vector\",\"hrkerQu2yNMmBz9H\",\"jdbc:postgresql://10.202.10.64:5432/catalogo?currentSchema=cdc\",10000)").consume();
        session.run("CALL eci.initPostgresPromotionsImport(\"mio\",\"vector\",\"hrkerQu2yNMmBz9H\",\"jdbc:postgresql://10.202.10.64:5432/catalogo?currentSchema=cdc\",10000)").consume();
        System.out.println("Creada el rs");
        finish = !(finish); //Si la recarga de cachés fué bien, ponemos finish a false. Si no, es true para abortar proceso.
        int cuenta =0;
        while (!finish){
            long iniImportComm = System.currentTimeMillis();
            //StatementResult result2 = session.run("CALL eci.importPromotionsRegistries(\"" + mode + "\",100000)");
            StatementResult result2 = session.run("CALL eci.importPromotionsRegistries(\"mio\",10000)");
            finish = result2.single().get("atEnd", false);
            cuenta = cuenta +1 ;
            long finImportComm = System.currentTimeMillis();
            System.out.println("Tiempo empleado en llamada: " + String.valueOf(cuenta) +" , " + (finImportComm - iniImportComm)  + " milisegundos");
        }
        StatementResult result2 = session.run("CALL eci.importPromotionsClose(\"" + mode + "\")");
        session.close();
        driver.close();
        long finCacheDiv = System.nanoTime();


        System.out.println("Tiempo empleado en generacion de caché: " + String.valueOf((finCacheDiv - iniCacheDiv) / 1000000000));

    }

    /*public static Driver getDriver(){

        return GraphDatabase.driver("bolt://10.202.10.64:7688",AuthTokens.basic( "neo4j", "vector-itcgroup" ));

    }*/

    public static Driver getDriver(){

        //return GraphDatabase.driver("bolt://10.202.10.64:7688",AuthTokens.basic( "neo4j", "pepito" ));
        //return GraphDatabase.driver("bolt://localhost:7687",AuthTokens.basic( "neo4j", "pepito" ));
        return GraphDatabase.driver("bolt://localhost:7687", Config.build()
                .withConnectionTimeout( 15, TimeUnit.MINUTES )
                .withMaxConnectionLifetime( 30, TimeUnit.MINUTES )
                .withMaxConnectionPoolSize(100)
                .toConfig());

    }

}