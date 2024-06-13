package com.ym.config;

import com.ym.customer.Customer;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.WritableResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class CsvToCsv {

    @Value("${csv-source-location}")
    private String csvSourceLocation;
    @Value("${csv-destination-location}")
    private String csvDestinationLocation;

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    public FlatFileItemReader<Customer> itemReader() {
        FlatFileItemReader<Customer> fileItemReader = new FlatFileItemReader<>();
        fileItemReader.setName("csv-reader");
        fileItemReader.setResource(new ClassPathResource(csvSourceLocation));
        fileItemReader.setLinesToSkip(1);
        fileItemReader.setLineMapper(lineMapper());
        return fileItemReader;
    }

    public LineMapper<Customer> lineMapper(){
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames(Customer.fields());

        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }

    public ItemProcessor<Customer, Customer> itemProcessor(){
        return new ItemProcessor<Customer, Customer>() {
            @Override
            public Customer process(@NonNull Customer customer) throws Exception {
                customer.setFirstName(customer.getFirstName().toUpperCase());
                customer.setLastName(customer.getLastName().toUpperCase());
                return customer;
            }
        };
    }

    public FlatFileItemWriter<Customer> itemWriter(){
        FlatFileItemWriter<Customer> fileItemWriter = new FlatFileItemWriter<>();
        fileItemWriter.setResource(new FileSystemResource(csvDestinationLocation));
        DelimitedLineAggregator<Customer> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(";");
        BeanWrapperFieldExtractor<Customer> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(Customer.fields());
        lineAggregator.setFieldExtractor(fieldExtractor);
        fileItemWriter.setLineAggregator(lineAggregator);
        return fileItemWriter;
    }

    public Step step(){
        return new StepBuilder("csv-to-csv", jobRepository)
                .<Customer,Customer>chunk(100, transactionManager)
                .reader(itemReader())
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }

    @Bean
    public Job csvToCsvJob(){
        return new JobBuilder("csv-to-csv", jobRepository)
                .flow(step())
                .end()
                .build();
    }
}
