package com.datacuaimas.avro;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

public class GenerateSpotify {
    private static final String CSV_FILE_PATH = "data/tracks-small.csv";
    private static final String AVRO_SCHEMA_PATH = "src/main/avro/spotify.avsc";
    private static final String PATH = "./outputSerializado/spotify.avro";

    public static void main(String[] args) throws IOException, CsvValidationException {
        // Load Avro schema
        Schema schema = new Schema.Parser().parse(new File(AVRO_SCHEMA_PATH));

        // Open data file
        File file = new File(PATH);
        if (file.getParentFile() != null) {
            file.getParentFile().mkdirs();
        }
        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, file);

        // Read CSV file
        try (CSVReader csvReader = new CSVReader(new FileReader(CSV_FILE_PATH))) {
            String[] nextRecord = csvReader.readNext();
            while ((nextRecord = csvReader.readNext()) != null) {
                
                // Create a GenericRecord using the schema
                GenericRecord record = new GenericData.Record(schema);
                record.put("id", nextRecord[0]);
                record.put("track_name", nextRecord[1].isEmpty() ? null :  nextRecord[1]);
                record.put("disc_number", nextRecord[2].isEmpty() ? null : Integer.parseInt(nextRecord[2]));
                record.put("duration", nextRecord[3].isEmpty() ? null :  Integer.parseInt(nextRecord[3]));
                record.put("explicit", nextRecord[4].isEmpty() ? null :  Integer.parseInt(nextRecord[4]));
                record.put("audio_feature_id", nextRecord[5].isEmpty() ? null :  nextRecord[5]);
                record.put("preview_url", nextRecord[6].isEmpty() ? null :  nextRecord[6]);
                record.put("track_number", nextRecord[7].isEmpty() ? null : Integer.parseInt(nextRecord[7]));
                record.put("popularity", nextRecord[8].isEmpty() ? null : Integer.parseInt(nextRecord[8]));
                record.put("is_playable", nextRecord[9].isEmpty() ? null : Integer.parseInt(nextRecord[9]));
                record.put("acousticness", nextRecord[10].isEmpty() ? null :  Float.parseFloat(nextRecord[10]));
                record.put("danceability", nextRecord[11].isEmpty() ? null :  Float.parseFloat(nextRecord[11]));
                record.put("energy", nextRecord[12].isEmpty() ? null :  Float.parseFloat(nextRecord[12]));
                record.put("instrumentalness", nextRecord[13].isEmpty() ? null :  Float.parseFloat(nextRecord[13]));
                record.put("key", nextRecord[14].isEmpty() ? null :  Integer.parseInt(nextRecord[14]));
                record.put("liveness", nextRecord[15].isEmpty() ? null :  Float.parseFloat(nextRecord[15]));
                record.put("loudness", nextRecord[16].isEmpty() ? null :  Float.parseFloat(nextRecord[16]));
                record.put("mode", nextRecord[17].isEmpty() ? null :  Integer.parseInt(nextRecord[17]));
                record.put("speechiness", nextRecord[18].isEmpty() ? null :  Float.parseFloat(nextRecord[18]));
                record.put("tempo", nextRecord[19].isEmpty() ? null :  Float.parseFloat(nextRecord[19]));
                record.put("time_signature", nextRecord[20].isEmpty() ? null :  Integer.parseInt(nextRecord[20]));
                record.put("valence", nextRecord[21].isEmpty() ? null :  Float.parseFloat(nextRecord[21]));
                record.put("album_name", nextRecord[22].isEmpty() ? null :  nextRecord[22]);
                record.put("album_group", nextRecord[23].isEmpty() ? null :  nextRecord[23]);
                record.put("album_type", nextRecord[24].isEmpty() ? null :  nextRecord[24]);
                record.put("release_date", nextRecord[25].isEmpty() ? null :  nextRecord[25]);
                record.put("album_popularity", nextRecord[26].isEmpty() ? null :  Integer.parseInt(nextRecord[26]));
                record.put("artist_name", nextRecord[27].isEmpty() ? null :  nextRecord[27]);
                record.put("artist_popularity", nextRecord[28].isEmpty() ? null :  Integer.parseInt(nextRecord[28]));
                record.put("followers", nextRecord[29].isEmpty() ? null :  Integer.parseInt(nextRecord[29]));
                record.put("genre_id", nextRecord[30].isEmpty() ? null :  nextRecord[30]);

                dataFileWriter.append(record);
                System.out.println(record);
            }
        }

        dataFileWriter.close();
    }
}