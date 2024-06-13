package com.ym.config;

import com.ym.customer.Customer;
import jakarta.persistence.EntityManagerFactory;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class DbToCsv {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    private final EntityManagerFactory entityManagerFactory;

    @Value("${csv-destination-location}")
    private String csvDestinationLocation;

    public ItemReader<Customer> itemReader(){
        JpaPagingItemReader<Customer> jpaPagingItemReader = new JpaPagingItemReader<>();
        jpaPagingItemReader.setEntityManagerFactory(entityManagerFactory);
        jpaPagingItemReader.setQueryString("SELECT customer FROM Customer customer");
        jpaPagingItemReader.setPageSize(10);
        return jpaPagingItemReader;
    }

    public ItemProcessor<Customer, Customer> itemProcessor(){
        return new ItemProcessor<Customer, Customer>() {
            @Override
            public Customer process(@NonNull Customer customer) throws Exception {
                return customer;
            }
        };
    }

    public ItemWriter<Customer> itemWriter(){
        FlatFileItemWriter<Customer> fileItemWriter = new FlatFileItemWriter<>();
        fileItemWriter.setResource(new FileSystemResource(csvDestinationLocation));
        DelimitedLineAggregator<Customer> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        BeanWrapperFieldExtractor<Customer> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(Customer.fields());
        lineAggregator.setFieldExtractor(fieldExtractor);
        fileItemWriter.setLineAggregator(lineAggregator);
        return fileItemWriter;
    }

    public Step step(){
        return new StepBuilder("db-to-csv", jobRepository)
                .<Customer,Customer>chunk(10, transactionManager)
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }

    @Bean
    public Job dbToCsvJob() {
        return new JobBuilder("db-to-csv", jobRepository)
                .flow(step())
                .end()
                .build();
    }
}
