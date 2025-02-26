package com.datacuaimas.avro;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import classes.avro.User;

public class GenerateData {
  // Colores de los usuarios (incluyendo nulo que representa sin color)
  public static final String[] COLORS = {"red", "orange", "yellow", "green", "blue", "purple", null};
  // Número de usuarios a generar
  public static final int USERS = 20;
  // Ruta al archivo de datos
  public static final String PATH = "./input/users.avro";

  /**
   * Genera datos de usuarios y los escribe en un archivo Avro.
   * @param args Argumentos de la línea de comandos.
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    // Open data file
    File file = new File(PATH);

    // Si el directorio no existe, crearlo
    if (file.getParentFile() != null) {
      file.getParentFile().mkdirs();
    }

    // Crear escritor de datos Avro
    DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
    DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
    // Escribir datos del esquema de usuario en el archivo
    dataFileWriter.create(User.SCHEMA$, file);

    // Crear y escribir usuarios en el archivo
    User user;
    Random random = new Random();
    for (int i = 0; i < USERS; i++) {
      // Crear un usuario con un color aleatorio y llamarlo "user"
      user = new User("user", null, COLORS[random.nextInt(COLORS.length)]);
      // Escribir el usuario en el archivo
      dataFileWriter.append(user);
      // Imprimir el usuario en la consola
      System.out.println(user);
    }

    // Cerrar el escritor de datos
    dataFileWriter.close();
  }
}
