package entities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class CoordenadaWritable implements WritableComparable<CoordenadaWritable> {
	// TODO: Y si fuera sólo 'double', que ventajas/desventajas tendría??
	private DoubleWritable latitud;
	private DoubleWritable longitud;

	public CoordenadaWritable() {
	}

	public CoordenadaWritable(DoubleWritable latitud, DoubleWritable longitud) {
		this.latitud = latitud;
		this.longitud = longitud;
	}

	public DoubleWritable getLatitud() {
		return latitud;
	}

	public DoubleWritable getLongitud() {
		return longitud;
	}

	public void setLatitud(DoubleWritable latitud) {
		this.latitud = latitud;
	}

	public void setLongitud(DoubleWritable longitud) {
		this.longitud = longitud;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		latitud.readFields(in);
		longitud.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		latitud.write(out);
		longitud.write(out);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((latitud == null) ? 0 : latitud.hashCode());
		result = prime * result + ((longitud == null) ? 0 : longitud.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CoordenadaWritable other = (CoordenadaWritable) obj;
		if (latitud == null) {
			if (other.latitud != null)
				return false;
		} else if (!latitud.equals(other.latitud))
			return false;
		if (longitud == null) {
			if (other.longitud != null)
				return false;
		} else if (!longitud.equals(other.longitud))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "<" + latitud + "," + longitud + ">";
	}

	@Override
	public int compareTo(CoordenadaWritable cw) {
		int cmp = latitud.compareTo(cw.latitud);
		if (cmp != 0) {
			return cmp;
		}
		return longitud.compareTo(cw.longitud);
	}

	public static class Comparator extends WritableComparator {

		private static final DoubleWritable.Comparator DOUBLE_COMPARATOR = new DoubleWritable.Comparator();

		public Comparator() {
			super(CoordenadaWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				return DOUBLE_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}

		static {
			WritableComparator.define(CoordenadaWritable.class, new Comparator());
		}
	}
}
