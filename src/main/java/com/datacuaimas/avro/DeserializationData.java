package com.datacuaimas.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.Pair;

import java.io.File;
import java.io.IOException;

public class DeserializationData {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: DeserializationData <avro-file>");
            System.exit(1);
        }

        String avroFilePath = args[0];

        try {
            // Definir el esquema clave-valor
            String schemaString = "{\"type\":\"record\",\"name\":\"Pair\",\"fields\":[{\"name\":\"key\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"int\"}]}";
            Schema schema = new Schema.Parser().parse(schemaString);

            // Crear un lector de datos genéricos
            GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);

            // Leer el archivo Avro
            File avroFile = new File(avroFilePath);
            FileReader<GenericRecord> fileReader = DataFileReader.openReader(avroFile, datumReader);

            // Iterar sobre los registros y deserializarlos
            while (fileReader.hasNext()) {
                GenericRecord record = fileReader.next();
                Pair<CharSequence, Integer> pair = new Pair<>(record.get("key"), record.get("value"));
                System.out.println(pair);
            }

            fileReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}