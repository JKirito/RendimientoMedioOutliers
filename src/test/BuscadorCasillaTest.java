package test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.junit.Before;
import org.junit.Test;

import entities.CeldaWritable;
import entities.CoordenadaWritable;
import funciones.BuscadorCasilla;

public class BuscadorCasillaTest {
	CoordenadaWritable c;

	@Before
	public void init() throws IOException, InterruptedException {
		c = new CoordenadaWritable(new DoubleWritable(-37.5108465428739), new DoubleWritable(-61.784015));
	}

	@Test
	public void testColumnaConAncho1() throws IOException, InterruptedException {
		BuscadorCasilla buscadorCasilla = new BuscadorCasilla(c, 0.001);
		CeldaWritable celda = buscadorCasilla.celda();
		assertEquals(celda.getColumna(),new IntWritable(-37510));
	}

	@Test
	public void testColumnaConAncho2() throws IOException, InterruptedException {
		BuscadorCasilla buscadorCasilla = new BuscadorCasilla(c, 0.1);
		CeldaWritable celda = buscadorCasilla.celda();
		assertEquals(celda.getColumna(),new IntWritable(-375));
	}

	@Test
	public void testFila1() throws IOException, InterruptedException {
		BuscadorCasilla buscadorCasilla = new BuscadorCasilla(c, 0.001);
		CeldaWritable celda = buscadorCasilla.celda();
		assertEquals(celda.getFila(), new IntWritable(-61784));
	}

	@Test
	public void testFila2() throws IOException, InterruptedException {
		BuscadorCasilla buscadorCasilla = new BuscadorCasilla(c, 0.1);
		CeldaWritable celda = buscadorCasilla.celda();
		assertEquals(celda.getFila(), new IntWritable(-617));
	}

}
