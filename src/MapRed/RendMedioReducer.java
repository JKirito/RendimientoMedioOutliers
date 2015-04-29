package MapRed;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import entities.CeldaWritable;
import funciones.OperadorRend;

public class RendMedioReducer extends Reducer<CeldaWritable, DoubleWritable, CeldaWritable, DoubleWritable> {

	public void reduce(CeldaWritable _key, Iterable<DoubleWritable> values, Context context) throws IOException,
			InterruptedException {

//		System.out.println(_key.getColumna().toString()+ ", "+_key.getFila());
//		if(_key.getColumna().toString().equals("-622443") && _key.getFila().toString().equals("-366211")){
//			System.out.println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
//			for(DoubleWritable d : values){
//				System.out.println(d.get()+" ---------------------------------");
//			}
//		}

		OperadorRend oper = new OperadorRend(values);

		context.write(_key, new DoubleWritable(oper.getPromedio()));
	}
}
