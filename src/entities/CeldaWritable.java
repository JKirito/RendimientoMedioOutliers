package entities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class CeldaWritable implements WritableComparable<CeldaWritable> {
	private IntWritable fila;
	private IntWritable columna;

	public CeldaWritable() {
		fila = new IntWritable();
		columna = new IntWritable();
	}

	public CeldaWritable(IntWritable fila, IntWritable columna) {
		this.fila = fila;
		this.columna = columna;
	}

	public IntWritable getFila() {
		return fila;
	}

	public IntWritable getColumna() {
		return columna;
	}

	public void setFila(IntWritable fila) {
		this.fila = fila;
	}

	public void setColumna(IntWritable columna) {
		this.columna = columna;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		fila.readFields(in);
		columna.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		fila.write(out);
		columna.write(out);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((columna == null) ? 0 : columna.hashCode());
		result = prime * result + ((fila == null) ? 0 : fila.hashCode());
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
		CeldaWritable other = (CeldaWritable) obj;
		if (columna == null) {
			if (other.columna != null)
				return false;
		} else if (!columna.equals(other.columna))
			return false;
		if (fila == null) {
			if (other.fila != null)
				return false;
		} else if (!fila.equals(other.fila))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "<" + columna + "," + fila + ">";
	}

	@Override
	public int compareTo(CeldaWritable cw) {
		int cmp = fila.compareTo(cw.fila);
		if (cmp != 0) {
			return cmp;
		}
		return columna.compareTo(cw.columna);
	}

	public static class Comparator extends WritableComparator {

		private static final IntWritable.Comparator INT_COMPARATOR = new IntWritable.Comparator();

		public Comparator() {
			super(CeldaWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				return INT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}

		static {
			WritableComparator.define(CeldaWritable.class, new Comparator());
		}
	}
}
