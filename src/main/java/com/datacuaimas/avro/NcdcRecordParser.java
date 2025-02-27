package com.datacuaimas.avro;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class NcdcRecordParser {

    public List<String[]> parse(Text value) throws IOException {
        List<String[]> records = new ArrayList<>();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(value.toString());
        FSDataInputStream inputStream = null;
        BufferedReader reader = null;

        try {
            inputStream = fs.open(path);
            reader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            // Skip the header row
            reader.readLine();
            // Read the rest of the records
            while ((line = reader.readLine()) != null) {
                // Use a more robust CSV parsing method
                String[] fields = parseCSVLine(line);
                records.add(fields);
            }
        } finally {
            IOUtils.closeStream(reader);
            IOUtils.closeStream(inputStream);
        }
        return records;
    }

    private String[] parseCSVLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean inQuotes = false;

        for (char c : line.toCharArray()) {
            if (c == '"') {
                inQuotes = !inQuotes; // Toggle the inQuotes flag
            } else if (c == ',' && !inQuotes) {
                // If we encounter a comma and we're not inside quotes, it's the end of a field
                fields.add(currentField.toString());
                currentField.setLength(0); // Clear the current field
            } else {
                // Otherwise, just add the character to the current field
                currentField.append(c);
            }
        }
        // Add the last field
        fields.add(currentField.toString());

        return fields.toArray(new String[0]);
    }
}