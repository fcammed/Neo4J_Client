package com.vector;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;

public class futuresTest {
	static int instancia=2;
	static int tamaño_lote=1000;
	static int tamaño_buffer=1000;

	public static void main(String args[]) throws InterruptedException {
		Future<?>[] futures = new Future<?>[tamaño_buffer];

		ExecutorService executor = Executors.newFixedThreadPool(1000);
		ExecutorService pool_c = Executors.newFixedThreadPool(4);

		Controler control1 = new Controler(tamaño_buffer/2);
		control1.setName("Instancia 1");
		Future fcontrol1 = pool_c.submit(control1);
		Controler control2 = new Controler(tamaño_buffer/2);
		control2.setName("Instancia 2");
		Future fcontrol2 = pool_c.submit(control2);
		Controler control3 = new Controler(tamaño_buffer);
		control3.setName("Instancia 3");
		Future fcontrol3 = pool_c.submit(control3);
		Controler_huecos controlh = new Controler_huecos(tamaño_buffer);
		controlh.setName("Instancia 1");
		Future fcontrolh = pool_c.submit(controlh);

		int cont=0;
		int veces=10;
		int mode=4; //Control del caso de Uso
		if (mode==2) veces=veces*2;
		int ins = instancia;
		long inicio= System.currentTimeMillis();
		while (cont<veces) {
			switch (mode){
				case 1: // Array Futures
					porLotes(futures,executor,cont);
					break;
				case 2: //Runnable resolviendo Futures asíncronamente, con 2 instancias. Cada una de tamaño la mitad para ser equivalentes
					instancia = (instancia==1) ? 2 :1;
					porRunnable(executor,control1,control2,cont,tamaño_lote/2 );
					break;
				case 3: //Runnable resolviendo Futures asíncronamente, con 1 sólo instancia.
					instancia = 1;
					porRunnable(executor,control3,control2,cont, tamaño_lote);
					break;
				case 4: //Runnable asíncrono que gestiona su propio array de Futures, recibiendo el Future y la posicion del Array a insertarlo
					int cont2=0;
					while (cont2<tamaño_lote) {
						porRunnable_alt2(executor,controlh,cont, tamaño_lote, cont2);
						cont2+=1;
					}
					break;
			}
			cont = cont+1;
		}
		System.out.println("Tiempo en mandar todas ms: " + (System.currentTimeMillis()-inicio));
		while (!controlh.isAllDone()) {
			//esperar
		}
		while (control1.getStatus()) {
			control1.getStatus();
		}
		while (control2.getStatus()) {
			control2.getStatus();
		}
		System.out.println("Tiempo todas acabadas ms: " + (System.currentTimeMillis()-inicio));
		pool_c.shutdownNow();
		executor.shutdown();
	}

	public static void porRunnable_alt2( ExecutorService executor, Controler_huecos controlh, int cont, int buffer,int i) throws InterruptedException {
		int elemento = cont*buffer+i;
		/*
		// Se busca hueco en el Array en orden secuencial
		int hueco=0;
		if (elemento>=tamaño_lote) {
			while (hueco==0) {
				hueco=controlh.getHueco(); //devuelve posición Array +1
			}
			hueco=hueco-1;
		} else {
			hueco=elemento;
		}*/
		/*
		// Se busca hueco desde el principio cada vez
		int hueco=0;
		int index_hueco=0;
		while (hueco==0) {
			//Thread.sleep(10);
			hueco=controlh.getHueco(index_hueco); //devuelve posición Array +1
			index_hueco+=1;
		}
		hueco=hueco-1;
		*/

		//El runnable publica los huecos en una BlockingQueue, para que no tenga ni que buscarlo
		int hueco = controlh.getHuecobq();

		controlh.setFutures(new SquareCalculator(executor,i).calculate(elemento), hueco);
	}

	public static void porRunnable( ExecutorService executor, Controler control1, Controler control2, int cont, int buffer) {
		boolean ocupado=true;
		while (ocupado) {
			ocupado = (instancia == 1) ? control1.getStatus() : control2.getStatus();
		}
		for (int i = 0; i < buffer; i++) {
			if (instancia==1) {
				control1.setFutures(new SquareCalculator(executor,i).calculate(cont*buffer+i), i);
			} else {
				control2.setFutures(new SquareCalculator(executor,i).calculate(cont*buffer+i), i);
			}
		}
		if (instancia==1) {
			control1.setCuantos(buffer);
			control1.setStatus(true);
		} else {
			control2.setCuantos(buffer);
			control2.setStatus(true);
		}
	}

	public static void  porLotes(Future<?>[] futures, ExecutorService executor, int cont) {
		for (int i = 0; i < tamaño_lote; i++) {
			futures[i] = new SquareCalculator(executor,i).calculate(cont*tamaño_lote+i);
		}

		for (int i = 0; i < tamaño_lote; i++) {
			try {
				System.out.println("Raiz Cuadrada " + futures[i].get());
			} catch (InterruptedException | ExecutionException e) {
				// Handle appropriately, results.add(null) or just leave it out
			}
		}
	}
}

class Controler_huecos implements Runnable {
	private int buffer =100;
	//private Pair<?,?>[] futuresold = new Pair<?,?>[buffer];
	private AtomicReferenceArray<Pair<?,?>> futures; // = new AtomicReferenceArray<Pair<?,?>>(new Pair<?,?>[buffer]);
	private BlockingQueue<Integer> huecos = new LinkedBlockingQueue<Integer>();

	private AtomicBoolean status = new AtomicBoolean(false);
	private int cuantos =0;
	private String name="";
	private int index_hueco=0;

	public Controler_huecos(int buffer) throws InterruptedException {
		this.buffer = buffer;
		futures = new AtomicReferenceArray<Pair<?,?>>(new Pair<?,?>[buffer]);

		for (int i = 0; i < this.buffer-1; i++) {
			huecos.put(new Integer(i));
		}
	}

	public void setFutures(Future<?> future,int i) {
		this.futures.set(i,new Pair<String,Future<?>>("Task",future));
		//this.futures[i] = future;
	}

	public void setCuantos(int cuantos) {
		this.cuantos = cuantos;
	}

	public boolean getStatus() {
		return status.get();
	}

	public void setStatus(boolean status) {
		this.status.set(status);
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getHuecobq() throws InterruptedException {
		Integer h = this.huecos.take();
		return h.intValue();
	}

	public int getHueco(int index_hueco) {
		if (index_hueco>this.buffer-1)
			index_hueco = 0;

		if(this.futures.get(index_hueco)!=null)
			if (this.futures.get(index_hueco).getLeft()==null) {
				index_hueco = index_hueco + 1;
				//System.out.println("Hueco: "+this.index_hueco);
				return index_hueco;
			} else {
				index_hueco = index_hueco + 1;
				return 0;
			}

		index_hueco = index_hueco + 1;
		//System.out.println("Hueco: "+this.index_hueco);
		return index_hueco;
	}

	public int getHueco() {
		if (this.index_hueco>this.buffer-1)
			this.index_hueco = 0;

		if(this.futures.get(this.index_hueco)!=null)
			if (this.futures.get(this.index_hueco).getLeft()==null) {
				this.index_hueco = this.index_hueco + 1;
				//System.out.println("Hueco: "+this.index_hueco);
				return this.index_hueco;
			} else {
				this.index_hueco = this.index_hueco + 1;
				return 0;
			}

		this.index_hueco = this.index_hueco + 1;
		//System.out.println("Hueco: "+this.index_hueco);
		return this.index_hueco;
	}

	public boolean isAllDone() {
			for (int i = 0; i < this.buffer-1; i++) {
				if(this.futures.get(i)!=null)
					if (this.futures.get(i).getLeft() != null) {
						return false;
					}
			}
		return true;
	}

	public void run() {
		System.out.println("Arrancando");
		try{
			while (!Thread.currentThread().isInterrupted()) {
				for (int i = 0; i < this.buffer-1; i++) {
					if (this.futures.get(i)!=null)
						if (this.futures.get(i).getLeft()!=null)
							try {
								Future<?> fi = (Future)this.futures.get(i).getRight();
								//if (fi.isDone()) {
									System.out.println(this.name + " - Raiz Cuadrada " + fi.get());
									this.futures.get(i).setLeft(null);
									this.huecos.put(new Integer(i));
								//} else {

								//}
							} catch ( ExecutionException e) {
								// Handle appropriately, results.add(null) or just leave it out
								e.printStackTrace();
							}
				}
			}
		} catch (InterruptedException e) {
			System.out.println("Finalizando Runnable: " + this.name);
			Thread.currentThread().interrupt();
		}
	}
}

class Controler implements Runnable {
	private Future<?>[] futures; // = new Future<?>[100];
	private AtomicBoolean status = new AtomicBoolean(false);
	private int cuantos =0;
	private String name="";
	private int buffer;

	public Controler(int buffer) {
		this.buffer = buffer;
		futures = new Future<?>[buffer];
	}

	public void setFutures(Future<?> future, int i) {
		this.futures[i] = future;
	}

	public void setCuantos(int cuantos) {
		this.cuantos = cuantos;
	}

	public boolean getStatus() {
		return status.get();
	}

	public void setStatus(boolean status) {
		this.status.set(status);
	}

	public void setName(String name) {
		this.name = name;
	}

	public void run() {
		try{
			while (!Thread.currentThread().isInterrupted()) {
				if (status.get()==true ) {
					if (this.cuantos>0)
						for (int i = 0; i < this.cuantos-1; i++) {
							try {
								System.out.println(this.name + " - Raiz Cuadrada " + futures[i].get());
							} catch ( ExecutionException e) {
								// Handle appropriately, results.add(null) or just leave it out
								e.printStackTrace();
							}
						}
					status.set(false);
				} else {
				}
			}
		} catch (InterruptedException e) {
			System.out.println("Finalizando Runnable: " + this.name);
			Thread.currentThread().interrupt();
		}
	}
}

class SquareCalculator {

	private ExecutorService executor;
	private int ratio;

	public SquareCalculator(ExecutorService executor, int ratio) {
		this.executor = executor;
		this.ratio = ratio;
	}

	public Future<Integer> calculate(Integer input) {
		return executor.submit(() -> {
			//Para simular carga variable
			// sólo las peticiones pares de cada lote se incrementa su tiempo de respuesta.
			// Proporcional a su número. Cuanto más se lancen, más tarda.
			if (ratio%2==0)
				ratio=0;
			Thread.sleep(1000 + ratio);
			//return input * input;
			return input;
		});
	}
}
