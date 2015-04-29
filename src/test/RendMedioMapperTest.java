package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import MapRed.RendMedioMapper;

public class RendMedioMapperTest {
	private RendMedioMapper mapper;
	private Context context;

	@Before
	public void init() throws IOException, InterruptedException {
		mapper = new RendMedioMapper();
		context = Mockito.mock(Context.class);
		System.setProperty("anchoCelda", "0.001");
	}

	@Test
	public void testLatitud() throws IOException, InterruptedException {
		RendMedioMapper rendMedioMapper = new RendMedioMapper();
		Text line = new Text(
				"0g5sDpQTe1,-61.784015,-37.5108465428739,25969032,238.2000000000,8.8400000000,0.0000000000,3.0000000000,0.0000000000,0.0000000000,1357148079,TRIGO,F52: RAUL,L 1:,0.0000000000,0.0000000000,1");
		rendMedioMapper.map(new LongWritable(1L), line, context);
		assertTrue (rendMedioMapper.latitudCampo.equals("-37.5108465428739"));
	}

	@Test
	public void testLongitud() throws IOException, InterruptedException {
		RendMedioMapper rendMedioMapper = new RendMedioMapper();
		Text line = new Text(
				"0g5sDpQTe1,-61.784015,-37.5108465428739,25969032,238.2000000000,8.8400000000,0.0000000000,3.0000000000,0.0000000000,0.0000000000,1357148079,TRIGO,F52: RAUL,L 1:,0.0000000000,0.0000000000,1");
		rendMedioMapper.map(new LongWritable(1L), line, context);
		assertTrue (rendMedioMapper.longitudCampo.equals("-61.784015"));
	}

	@Test
	public void testLongitudFail() throws IOException, InterruptedException {
		RendMedioMapper rendMedioMapper = new RendMedioMapper();
		Text line = new Text("FNAME,LONGITUD,LATITUD,ID,ELEVACION,ANCHO,DISTANCIA,DURACION,HUMEDAD,FLUJO,UTC,PRODUCTO,FIELD,CARGA,MASA,REND,CLUSTER");
		rendMedioMapper.map(new LongWritable(1L), line, context);
		assertTrue(rendMedioMapper.longitudCampo.equals("LONGITUD"));
	}

	@Test
	public void testAnchoCelda() throws IOException, InterruptedException {
		RendMedioMapper rendMedioMapper = new RendMedioMapper();
		Text line = new Text("FNAME,LONGITUD,LATITUD,ID,ELEVACION,ANCHO,DISTANCIA,DURACION,HUMEDAD,FLUJO,UTC,PRODUCTO,FIELD,CARGA,MASA,REND,CLUSTER");
		rendMedioMapper.map(new LongWritable(1L), line, context);
		assertEquals(System.getProperty("anchoCelda"), "0.001");
	}



}
