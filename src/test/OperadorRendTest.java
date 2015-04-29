package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.junit.Test;

import funciones.OperadorRend;

public class OperadorRendTest {

	@Test
	public void testPromedioOk() throws IOException, InterruptedException {
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(0.0));
		values.add(new DoubleWritable(0.1));
		values.add(new DoubleWritable(0.2));
		values.add(new DoubleWritable(0.3));
		values.add(new DoubleWritable(0.4));
		values.add(new DoubleWritable(0.5));
		values.add(new DoubleWritable(0.6));
		values.add(new DoubleWritable(0.7));
		values.add(new DoubleWritable(0.8));
		values.add(new DoubleWritable(0.9));
		OperadorRend op = new OperadorRend(values);
		assertEquals(op.getPromedio(), new Double(0.45));
	}

	@Test
	public void testPromedioFalla() throws IOException, InterruptedException {
		List<DoubleWritable> values = new ArrayList<DoubleWritable>();
		values.add(new DoubleWritable(0.0));
		values.add(new DoubleWritable(0.1));
		values.add(new DoubleWritable(0.2));
		values.add(new DoubleWritable(0.3));
		values.add(new DoubleWritable(0.4));
		values.add(new DoubleWritable(0.5));
		values.add(new DoubleWritable(0.6));
		values.add(new DoubleWritable(0.7));
		values.add(new DoubleWritable(0.8));
		values.add(new DoubleWritable(0.9));
		OperadorRend op = new OperadorRend(values);
		assertNotSame(op.getPromedio(), new Double(0.45012));
	}


}
