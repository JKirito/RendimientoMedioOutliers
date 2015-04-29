package funciones;

import org.apache.hadoop.io.IntWritable;

import entities.CeldaWritable;
import entities.CoordenadaWritable;

public class BuscadorCasilla {
	double anchoCelda;
	// double latitudMin;
	// double longitudMin;
	double latitud;
	double longitud;

	public BuscadorCasilla(CoordenadaWritable coordenada, double anchoCelda) {
		this.latitud = coordenada.getLatitud().get();
		this.longitud = coordenada.getLongitud().get();
		this.anchoCelda = anchoCelda;
	}

	public CeldaWritable celda() {
		CeldaWritable celda = new CeldaWritable();
		int columna = (int) (this.longitud / this.anchoCelda);
		int fila = (int) (this.latitud / this.anchoCelda);
		// System.out.println("Fila: " + this.latitud + " | " + fila);
		// System.out.println("Columna: " + this.longitud + " | " + columna);
		celda.setFila(new IntWritable(fila));
		celda.setColumna(new IntWritable(columna));
		return celda;
	}

}
