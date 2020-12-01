package com.vector;
import io.reactiverse.pgclient.*;
import io.vertx.core.Vertx;
import org.neo4j.driver.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class launchercomo {

    public launchercomo(String[] args, String total) {
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
        if (true) {
            Driver driver = getDriver();
            Session session = driver.session();
            long iniCacheDiv = System.nanoTime();
            System.out.println("Creando Indice de Tallas");
            Result resultiT = session.run("CREATE INDEX ON :TALLA(id)");
            System.out.println("Creando Indice de Barras");
            Result resultiB = session.run("CREATE INDEX ON :BARRA(id)");
            System.out.println("Creando Indice de Familia");
            Result resultiF = session.run("CREATE INDEX ON :FAMILIA(id)");
            System.out.println("Creando Indice de Uneco");
            Result resultiU = session.run("CREATE INDEX ON :UNECO(id)");
            System.out.println("Creando Indice de Empresa");
            Result resultiE = session.run("CREATE INDEX ON :EMPRESA(id)");
            System.out.println("Creando Indice de Division");
            Result resultiD = session.run("CREATE INDEX ON :DIVISION(id)");
            //session.run("CALL eci.initPostgresCommoditiesImport(\"mio\",\"vector\",\"hrkerQu2yNMmBz9H\",\"jdbc:postgresql://10.202.10.64:5432/catalogo?currentSchema=cdc\",10000)").consume();
            session.run("CALL eci.initPostgresCommoditiesImport(\"" + mode+ "\",\"vector\",\"hrkerQu2yNMmBz9H\",\"jdbc:postgresql://10.202.10.64:5432/catalogo?currentSchema=cdc\",1)").consume(); //0000)").consume();
            boolean finish = false;
            int cuenta =0;
            while (!finish) { //|| (cuenta<((22000000/100000)/2))&&(total.equals("P")) ){
                //Result result2 = session.run("CALL eci.importCommodityRegistries(\"mio\",100000)");
				long iniImportComm = System.currentTimeMillis();
                Result result2 = session.run("CALL eci.importCommodityRegistries(\"" + mode + "\",50000)");
                finish = result2.single().get("atEnd", false);
                cuenta = cuenta +1 ;
				long finImportComm = System.currentTimeMillis();
				System.out.println("Tiempo empleado en llamada: " + String.valueOf(cuenta) +" , " + (finImportComm - iniImportComm)  + " milisegundos");
            }
            //Result result2 = session.run("CALL eci.importCommodityClose(\"mio\")");
            Result result2 = session.run("CALL eci.importCommodityClose(\"" + mode + "\")");
            session.close();
            driver.close();
            // TODO Auto-generated method stub
            long finCacheDiv = System.nanoTime();

            System.out.println("Tiempo empleado en generacion de grafo: " + String.valueOf((finCacheDiv - iniCacheDiv) / 1000000000));
        } else {
            iteraTable_reactive_pg_client();
        }
    }

    static void iteraTable_reactive_pg_client() {
        long iniIteraTable = System.nanoTime();
        //dbRepository = new PostgreDBRepository2("vector", "hrkerQu2yNMmBz9H", "jdbc:postgresql://10.202.10.64:5432/catalogo?currentSchema=cdc");
        final String GET_COMMODITIES_QUERY = "SELECT c.CEMPRESA as empresa, c.CUNECOOO, c.CFAMILIA, c.CBARRAAA, c.CTALLAEC,  c.CFABRICA,c.CMARMUMA FROM cdc.t3766000 c "
                + "where c.CEMPRESA = $1 ORDER BY c.CEMPRESA , c.CUNECOOO, c.CFAMILIA, c.CBARRAAA, c.CTALLAEC, c.CFABRICA, c.CMARMUMA";
        final String GET_COMMODITIES_QUERY2 = "SELECT COUNT(c.CEMPRESA) FROM cdc.t3766000 c order by $1";
        final String GET_COMMODITIES_QUERY3 = "SELECT * FROM users WHERE first_name LIKE $1";


        PgPoolOptions options = new PgPoolOptions()
                //.fromUri("jdbc:postgresql://10.202.10.64:5432/catalogo?currentSchema=cdc")
                .fromUri("postgresql://vector:hrkerQu2yNMmBz9H@10.202.10.64:5432/catalogo?currentSchema=cdc")
                /*.setPort(5432)
                .setHost("10.202.10.64")
                .setDatabase("catalogo")
                .setUser("vector")
                .setPassword("hrkerQu2yNMmBz9H")*/
                .setMaxSize(5);

        // Create the client pool
        //PgPool client = PgClient.pool(options);
        // Create the pool from the connection URI
        PgPool client = PgClient.pool(Vertx.vertx(),"postgresql://vector:hrkerQu2yNMmBz9H@10.202.10.64:5432/catalogo?currentSchema=cdc");
        // Get a connection from the pool

        if (true ) {
        client.getConnection(ar1 -> {
            if (ar1.succeeded()) {
                System.out.println("Connected");
                // Obtain our connection
                PgConnection conn = ar1.result();
                conn.prepare(GET_COMMODITIES_QUERY, ar2 -> {
                    if (ar2.succeeded()) {
                        System.out.println("Prepared Statement OK");
                        PgPreparedQuery pq = ar2.result();
                        // Streams require to run within a transaction
                        PgTransaction tx = conn.begin();
                        // Fetch 50 rows at a time
                        PgStream<Row> stream = pq.createStream(50,Tuple.of("001"));
                        // Use the stream
                        stream.exceptionHandler(err -> {
                            System.out.println("Error: " + err.getMessage());
                        });
                        stream.endHandler(v -> {
                            tx.commit();
                            System.out.println("End of stream");
                        });
                        stream.handler(row -> {
                            System.out.println("Empresa: " + row.getString(0) + " ");
                        });

                        /*PgRowSet rows = ar2.result();
                        for (Row row : rows) {
                            System.out.println("Filas: " + row.getInteger(0) + " ");
                        }*/
                        //conn.close();
                    } else {
                        // Release the connection to the pool
                        System.out.println("Prepared Statement KO");
                        conn.close();
                    }
                });
                //client.close();
            } else {
                System.out.println("Could not connect: " + ar1.cause().getMessage());
            }
        });
        }

        if (false) {
            client.query(GET_COMMODITIES_QUERY2, ar -> {
                if (ar.succeeded()) {
                    PgRowSet rows = ar.result();
                    for (Row row : rows) {
                        System.out.println("Filas2: " + row.getInteger(0) + " ");
                    }
                    client.close();
                } else {
                    System.out.println("Failure: " + ar.cause().getMessage());
                }
            });
        }

        long finIteraTable = System.nanoTime();
        System.out.println("Tiempo empleado en recorrer PostGres: " + String.valueOf((finIteraTable - iniIteraTable) / 1000000000));
    }

    public static Driver getDriver(){

        //return GraphDatabase.driver("bolt://10.202.10.64:7688",AuthTokens.basic( "neo4j", "pepito" ));
        //return GraphDatabase.driver("bolt://localhost:7687",AuthTokens.basic( "neo4j", "pepito" ));
        return GraphDatabase.driver("bolt://localhost:7687", Config.builder()
                .withConnectionTimeout( 15, TimeUnit.MINUTES )
                .withMaxConnectionLifetime( 30, TimeUnit.MINUTES )
                .withMaxConnectionPoolSize(100)
                .build());

    }
}