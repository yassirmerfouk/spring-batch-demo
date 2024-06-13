package com.ym.config;

import com.ym.customer.Customer;
import com.ym.customer.CustomerRepository;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Configuration
@RequiredArgsConstructor
public class CsvToDb {

    @Value("${csv-location}")
    private String csvLocation;

    private final CustomerRepository customerRepository;

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    public FlatFileItemReader<Customer> customerItemReader(){
        FlatFileItemReader<Customer> fileItemReader = new FlatFileItemReader<>();
        fileItemReader.setResource(new FileSystemResource(csvLocation));
        fileItemReader.setName("csv-file-reader");
        fileItemReader.setLinesToSkip(1);
        fileItemReader.setLineMapper(cutomerLineMapper());
        return fileItemReader;
    }

    public LineMapper<Customer> cutomerLineMapper(){
        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("firstName", "lastName", "phone", "gender", "strBirthDate");

        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }

    public ItemProcessor<Customer, Customer> customerItemProcessor() {
        return customer -> {
            customer.setBirthDate(LocalDate.parse(customer.getStrBirthDate(), DateTimeFormatter.ofPattern("dd/MM/yyyy")));
            return customer;
        };
    }

/*    @Bean
    public ItemProcessor<Customer, Customer> customerItemProcessor() {
        return new ItemProcessor<Customer, Customer>() {
            @Override
            public Customer process(@NonNull Customer customer) throws Exception {
                customer.setBirthDate(LocalDate.parse(customer.getStrBirthDate(), DateTimeFormatter.ofPattern("MM/dd/yyyy")));
                return customer;
            }
        };
    }*/

/*    @Bean
    public RepositoryItemWriter<Customer> customerItemWriter() {
        RepositoryItemWriter<Customer> repositoryItemWriter = new RepositoryItemWriter<>();
        repositoryItemWriter.setRepository(customerRepository);
        repositoryItemWriter.setMethodName("save");
        return repositoryItemWriter;
    }*/

    public ItemWriter<Customer> customerItemWriter(){
        return new ItemWriter<Customer>() {
            @Override
            public void write(@NonNull Chunk<? extends Customer> customers) throws Exception {
                customerRepository.saveAll(customers);
            }
        };
    }

    public Step step() {
        return new StepBuilder("csv-step", jobRepository)
                .<Customer, Customer>chunk(10, transactionManager)
                .reader(customerItemReader())
                .processor(customerItemProcessor())
                .writer(customerItemWriter())
                .build();
    }

    @Bean
    public Job csvToDbJob() {
        return new JobBuilder("csv-job", jobRepository)
                .flow(step())
                .end()
                .build();
    }



}
