package com.ym.customer;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/customers")
public class CustomerController {

    private final Job csvToDbJob;
    private final Job csvToCsvJob;
    private final Job dbToCsvJob;
    private final JobLauncher jobLauncher;

    public CustomerController(@Qualifier("csvToDbJob") Job csvToDbJob,
                              @Qualifier("csvToCsvJob") Job csvToCsvJob ,
                              @Qualifier("dbToCsvJob") Job dbToCsvJob,
                              JobLauncher jobLauncher) {
        this.csvToDbJob = csvToDbJob;
        this.csvToCsvJob = csvToCsvJob;
        this.dbToCsvJob = dbToCsvJob;
        this.jobLauncher = jobLauncher;
    }

    @GetMapping("/load-csv-to-db")
    public ResponseEntity<?> loadCsvToDb() throws Exception{
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("start-at", System.currentTimeMillis())
                .toJobParameters();
        jobLauncher.run(csvToDbJob, jobParameters);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @GetMapping("/load-csv-to-csv")
    public ResponseEntity<?> loadCsvToCsv() throws Exception{
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("start-at", System.currentTimeMillis())
                .toJobParameters();
        jobLauncher.run(csvToCsvJob, jobParameters);
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @GetMapping("/load-db-to-csv")
    public ResponseEntity<?> loadDbToCsv() throws Exception{
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("start-at", System.currentTimeMillis())
                .toJobParameters();
        jobLauncher.run(dbToCsvJob, jobParameters);
        return new ResponseEntity<>(HttpStatus.OK);
    }


}
