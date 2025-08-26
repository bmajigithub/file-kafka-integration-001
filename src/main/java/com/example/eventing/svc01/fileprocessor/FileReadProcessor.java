package com.example.eventing.svc01.fileprocessor;

import com.example.eventing.svc01.kafkaprocessor.KafkaWriter;
import com.example.eventing.svc01.kafkaprocessor.KafkaWriterConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.Scanner;

@Slf4j
public class FileReadProcessor {

    private String fname;
    private KafkaWriterConfig kafkaWriterConfig;

    public FileReadProcessor(Builder builder) {
        this.fname = builder.fname;
        this.kafkaWriterConfig = builder.kafkaWriterConfig;
    }

    static public class Builder {
        private String fname;
        private KafkaWriterConfig kafkaWriterConfig;

        public Builder() {/* Default constructor*/}

        public Builder fname(String fname) {
            this.fname = fname;
            return this;
        }

        public Builder kafkaWriterConfig(KafkaWriterConfig kafkaWriterConfig) {
            this.kafkaWriterConfig = kafkaWriterConfig;
            return this;
        }

        public FileReadProcessor build() {
            return new FileReadProcessor(this);
        }
    }

    @Override
    public String toString() {
        return "fname = " + fname + ", kafkaWriterConfig = " + kafkaWriterConfig;
    }

    public void processFile() {
        File file = new File(fname);
        log.info("Start processing file: " + file.getName());
        // Processing file
        try {
            if (kafkaWriterConfig != null) {
                KafkaWriter kafkaWriter = new KafkaWriter(kafkaWriterConfig);
                Scanner reader = new Scanner(file);
                // File reading loop
                while (reader.hasNextLine()) {
                    String line = reader.nextLine();
                    log.info("Processing line: " + line);
                    kafkaWriter.write(line);
                }
                // Close the Kafka Producer
                kafkaWriter.close();
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        // Simulate processing time
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupted status
        }
        // Delete the file after processing
        file.delete();
        log.info("End processing file: " + file.getName());
    }

}
