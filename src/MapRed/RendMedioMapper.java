package MapRed;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import entities.CeldaWritable;
import entities.CoordenadaWritable;
import entities.Monitor.MonitorCampo;
import funciones.BuscadorCasilla;

public class RendMedioMapper extends Mapper<LongWritable, Text, CeldaWritable, DoubleWritable> {

	private static final String SEPARATOR_SYMBOL = ",";
	public String latitudCampo;
	public String longitudCampo;

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {

		final String[] values = ivalue.toString().split(SEPARATOR_SYMBOL);

		latitudCampo = values[MonitorCampo.LATITUD.value()];
		longitudCampo = values[MonitorCampo.LONGITUD.value()];

		if (latitudCampo.equals(MonitorCampo.LATITUD.name()) || longitudCampo.equals(MonitorCampo.LONGITUD.name())) {
			return;
		}

		// Solo me quedo con los de un monitor
		if (!values[MonitorCampo.FNAME.value()].toString().equals("0JpiX3keB7")) {
			System.out.println("saltenado linea xq es del monitor... " + values[MonitorCampo.FNAME.value()].toString());
			return;
		}

		DoubleWritable latitud = new DoubleWritable(Double.parseDouble(latitudCampo));
		DoubleWritable longitud = new DoubleWritable(Double.parseDouble(longitudCampo));
		CoordenadaWritable coordenada = new CoordenadaWritable(latitud, longitud);

		double anchoCelda = Double.valueOf(System.getProperty("anchoCelda"));
		// double latitudMin = Double.valueOf(System.getProperty("latitudMin"));
		// double longitudMin =
		// Double.valueOf(System.getProperty("longitudMin"));

		BuscadorCasilla buscador = new BuscadorCasilla(coordenada, anchoCelda);
		CeldaWritable celda = buscador.celda();

		DoubleWritable rend = new DoubleWritable(Double.parseDouble(values[MonitorCampo.REND.value()]));
		context.write(celda, rend);
	}

}
