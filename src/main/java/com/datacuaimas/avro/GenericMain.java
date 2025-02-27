package com.datacuaimas.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class GenericMain {
        /**
         * Método principal que serializa y deserializa datos de usuarios en un archivo Avro.
         * @param args
         * @throws IOException
         */
        public static void main(String[] args) throws IOException{
                if (args.length != 1) {
                        System.err.println("Uso: GenericMain <archivo-avro>");
                        System.exit(1);
                }
                // Cargar el esquema de usuario desde un archivo
                Schema schema = new Schema.Parser().parse(new File(args[0]));          
                // Crea un registro de usuario y lo llena con datos
                GenericRecord user1 = new GenericData.Record(schema);
                user1.put("name", "Alyssa");
                user1.put("favorite_number", 256);
                // Deja el color favorito vacío

                // Crea otro registro de usuario y lo llena con datos
                GenericRecord user2 = new GenericData.Record(schema);
                 user2.put("name", "Ben");
                user2.put("favorite_number", 7);
                user2.put("favorite_color", "red");

                // Serializa los usuarios en un archivo
                File file = new File("users.avro");
                DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
                DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
                dataFileWriter.create(schema, file);
                dataFileWriter.append(user1);
                dataFileWriter.append(user2);
                dataFileWriter.close();

                // Deserializa los usuarios de un archivo
                DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
                DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
                GenericRecord user = null;
                while (dataFileReader.hasNext()) {
                        // Reutiliza el objeto user, cargando los datos del siguiente registro
                        user = dataFileReader.next(user);
                        // Imprime el usuario en la consola
                        System.out.println(user);
                }

                // Cerrar el lector de archivos
                dataFileReader.close();
        }
}
