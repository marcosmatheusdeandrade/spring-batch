package com.batch.springbatch.config;

import com.batch.springbatch.model.Cliente;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class BatchConfig {

    @Bean
    public Job job(JobRepository jobRepository, Step step) {
        return new JobBuilder("job", jobRepository).start(step).build();
    }

    @Bean
    public Step step(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step", jobRepository)
                .tasklet((StepContribution contributon, ChunkContext chunkContext) -> {
                    System.out.println("testando tasklet");
                    return RepeatStatus.CONTINUABLE;
        }, transactionManager).build();
    }

    public Step chunk(JobRepository jobRepository, PlatformTransactionManager transactionManager, ItemReader<Cliente> reader, ItemWriter<Cliente> writer) {
        return new StepBuilder("chunk", jobRepository)
                .<Cliente, Cliente>chunk(10, transactionManager)
                .reader(reader)
                .writer(writer)
                .build();
    }

    @Bean
    public ItemReader<Cliente> reader() {
        return new FlatFileItemReaderBuilder<Cliente>()
                .name("reader")
                .resource(new FileSystemResource("files/clientes.csv"))
                .comments("--")
                .delimited()
                .names("nome", "email", "dataNascimento", "idade", "id")
                .targetType(Cliente.class)
                .build();
    }

    @Bean
    public ItemWriter<Cliente> writer(@Qualifier("appDS") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Cliente>()
                .dataSource(dataSource)
                .sql("INSERT INTO cliente (id, nome, email, data_nascimento, idade) VALUES (:id, :nome, :email, :dataNascimento, :idade)")
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .build();
    }
}
