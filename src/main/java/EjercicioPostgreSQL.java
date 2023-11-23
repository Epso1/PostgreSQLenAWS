import java.sql.*;

public class EjercicioPostgreSQL {
    public static void main(String[] args) {
        // Configurar de la conexión a la base de datos PostgreSQL
        String urlConexion = "jdbc:postgresql://formula1dbinstancia.c4mcvtpnls1u.us-east-1.rds.amazonaws.com/f12006";
        String usuario = "postgres";
        String password = "12345678";
        long key = 0;// Variable para almacenar la clave generada por la inserción en la tabla "constructors"

        // Cargar el driver de la base de datos
        try (Connection conexion = DriverManager.getConnection(urlConexion, usuario, password)) {

            try {
                // Desactivar auto-commit para gestionar transacciones
                conexion.setAutoCommit(false);

                // Llamar a los procedimientos almacenados a través de métodos java

                getDriversStandings(conexion);

                getResultsByDriver(conexion, "DOO");


                // Insertar a Seat en la tabla "constructors"
                String insertSeat = "INSERT INTO constructors (constructorref, name, nationality, url) VALUES (?,?,?,?) ";

                // Preparar la sentencia
                PreparedStatement sentencia = conexion.prepareStatement(insertSeat, PreparedStatement.RETURN_GENERATED_KEYS);// Obtener la clave generada por la inserción
                sentencia.setString(1, "seat");
                sentencia.setString(2, "Seat");
                sentencia.setString(3, "Spanish");
                sentencia.setString(4, "https://en.wikipedia.org/wiki/SEAT");

                // Ejecutar la sentencia
                sentencia.executeUpdate();

                // Obtener la clave generada
                ResultSet rs = sentencia.getGeneratedKeys();
                if (rs.next()) {
                    key = rs.getLong(1);
                }

                // Insertar a Carlos Sainz en la tabla "drivers"

                // Preparar la sentencia
                String insertCSainz = "INSERT INTO drivers (code, forename, surname, dob, nationality, constructorid, url) VALUES (?,?,?,?,?,?,?) ";
                sentencia = conexion.prepareStatement(insertCSainz);
                sentencia.setString(1, "SAI");
                sentencia.setString(2, "Carlos");
                sentencia.setString(3, "Sainz");
                sentencia.setDate(4, Date.valueOf("1994-09-01"));
                sentencia.setString(5, "Spanish");
                sentencia.setInt(6, (int) key);
                sentencia.setString(7, "https://en.wikipedia.org/wiki/Carlos_Sainz_Jr.");

                // Ejecutar la sentencia
                sentencia.executeUpdate();

                // Insertar a Manuel Alomá en la tabla "drivers"

                // Preparar la sentencia
                String insertMAloma = "INSERT INTO drivers (code, forename, surname, dob, nationality, constructorid, url) VALUES (?,?,?,?,?,?,?) ";
                sentencia = conexion.prepareStatement(insertMAloma);
                sentencia.setString(1, "ALM");
                sentencia.setString(2, "Manuel");
                sentencia.setString(3, "Alomá");
                sentencia.setDate(4, Date.valueOf("1998-04-04"));
                sentencia.setString(5, "Spanish");
                sentencia.setInt(6, (int) key);
                sentencia.setString(7, "https://en.wikipedia.org/wiki/Manuel_Aloma");

                // Ejecutar la sentencia
                sentencia.executeUpdate();

                // Cerrar la conexión
                sentencia.close();


                // Confirmar la transacción
                conexion.commit();

                // Volver a activar el auto-commit
                conexion.setAutoCommit(true);

            } catch (SQLException e) {
                // Si se produce un error, se revierten los cambios y se imprime el error producido
                System.err.println(e.getClass().getName() + ": " + e.getMessage());
                conexion.rollback();
                System.err.println("ROLLBACK ejecutado");
            } finally {
                // Mostrar los constructores y pilotos
                mostrarConstructores(conexion);
                mostrarPilotos(conexion);
            }

        } catch (SQLException e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    // Método para mostrar los constructores
    public static void mostrarConstructores(Connection conexion) throws SQLException {
        String mostrarConstructores = "SELECT * FROM constructors";
        PreparedStatement sentencia = conexion.prepareStatement(mostrarConstructores);
        ResultSet results = sentencia.executeQuery();

        while (results.next()) {
            System.out.println(results.getInt("constructorid") + " " +
                    results.getString("constructorref") + " " + results.getString("name") + " " +
                    results.getString("url"));
        }
        System.out.println();

        sentencia.close();
    }

    // Método para mostrar los pilotos
    public static void mostrarPilotos(Connection conexion) throws SQLException {
        String mostrarPilotos = "SELECT * FROM drivers";
        PreparedStatement sentencia = conexion.prepareStatement(mostrarPilotos);
        ResultSet results = sentencia.executeQuery();

        while (results.next()) {
            System.out.println(
                    results.getInt("driverid") + " " +
                            results.getString("code") + " " +
                            results.getString("forename") + " " +
                            results.getString("surname") + " " +
                            results.getString("dob") + " " +
                            results.getString("nationality") + " " +
                            results.getInt("constructorid") + " " +
                            results.getString("url")
            );
        }
        System.out.println();

        sentencia.close();
    }

    // Método para mostrar los resultados de un piloto por su código
    public static void getResultsByDriver(Connection conexion, String cod) throws SQLException {
        // Llamar al procedimiento almacenado
        CallableStatement resultadosPiloto = conexion.prepareCall("{call get_results_by_driver(?)}");

        // Pasar el parámetro al procedimiento almacenado
        resultadosPiloto.setString(1, cod);

        // Ejecutar el procedimiento almacenado
        resultadosPiloto.execute();

        // Obtener el resultado del procedimiento almacenado
        ResultSet rs = resultadosPiloto.getResultSet();

        // Mostrar el resultado
        System.out.format("\n%4s%10s%10s%10s%10s%10s%5s%10s\n", "Round", " ", "Circuit", " ", "Result", "Points", " ", "Date");
        while (rs.next()) {
            System.out.format("%4d%30s%10d%10d%8s%10s\n",
                    rs.getInt("round"),
                    rs.getString("circuit"),
                    rs.getInt("result"),
                    rs.getInt("points"),
                    " ",
                    rs.getDate(String.valueOf("date"))
            );
        }
        System.out.println();
    }

    // Método para mostrar la clasificación de constructores
    public static void getDriversStandings(Connection conexion) throws SQLException {
        // Llamar al procedimiento almacenado
        CallableStatement driversStandings = conexion.prepareCall("{call get_drivers_standings()}");

        // Ejecutar el procedimiento almacenado
        driversStandings.execute();

        // Obtener el resultado del procedimiento almacenado
        ResultSet rs = driversStandings.getResultSet();

        // Mostrar el resultado
        System.out.format("\n%20s%5s%6s\n", "Driver", " ", "Points");
        while (rs.next()) {
            System.out.format("%20s%5s%6d\n",
                    rs.getString("driver"),
                    " ",
                    rs.getInt("points")

            );
        }
        System.out.println();
    }


}
