package entities;

public class Monitor {

	/**
	 * El valor numérico representa el número de columna correspondiente a su
	 * campo en la tabla de monitores
	 *
	 * @author pruebahadoop
	 *
	 */
	public enum MonitorCampo {
		FNAME(1), LONGITUD(2), LATITUD(3), ID(4), ELEVACION(5), ANCHO(6), DISTANCIA(7), DURACION(8), HUMEDAD(9), FLUJO(
				10), UTC(11), PRODUCTO(12), FIELD(13), CARGA(14), MASA(15), REND(16), CLUSTER(17);

		private int value;

		private MonitorCampo(int n) {
			this.value = n;
		}

		/**
		 *
		 * @return su posicion de columna en la grilla. Le resto 1 para los
		 *         arrays q empiezan en cero.
		 */
		public int value() {
			return this.value - 1;
		}
	}
}
