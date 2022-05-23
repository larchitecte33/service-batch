package com.batch.creerServiceBatch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.sql.DataSource;

/**
 * Cette classe sert à mettre en place des batch jobs.
 */
@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
    @Autowired
    public JobBuilderFactory jobBuilderFactory; // Usine pratique pour un JobBuilder qui définit automatiquement le JobRepository.

    @Autowired
    public StepBuilderFactory stepBuilderFactory; // Usine pratique pour un StepBuilder qui définit automatiquement le JobRepository et le PlatformTransactionManager.

    // Crée un ItemReader. Il recherche un fichier nommé sample-data.csv et parse chaque ligne avec assez d'information
    // pour créer un objet Person.
    @Bean
    public FlatFileItemReader<Person> reader() {
        return new FlatFileItemReaderBuilder<Person>()
                .name("personItemReader") // Nom donné à l'ItemReader
                .resource(new ClassPathResource("donnees-exemple.csv")) // Resource à utiliser en entrée
                .delimited() // Retourne une instance d'un FlatFileItemReaderBuilder.DelimitedBuilder pour la construction d'un DelimitedLineTokenizer.
                .names(new String[]{"firstName", "lastName"}) // Noms de chacun des champs qui sont renvoyés dans l'ordre dans lequel ils apparaissent dans le fichier délimité.
                .fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{ // Utilisé pour mapper les données obtenues à partir du FieldSet dans un objet
                    setTargetType(Person.class);
                }})
                .build(); // Construit le FlatFileItemReader
    }

    // Crée une instance de PersonItemProcessor dont l'objectif est de convertir les données en majuscules.
    @Bean
    public PersonItemProcessor processor() {
        return new PersonItemProcessor();
    }

    // Crée un ItemWriter. Celui-ci est visé à une destination JDBC et obtient automatiquement une copie du dataSource
    // créé par @EnableBatchProcessing. Il inclut l'instruction SQL nécessaire pour insérer une personne unique, pilotée
    // par les propriétés du bean Java.
    @Bean
    public JdbcBatchItemWriter<Person> writer(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Person>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>()) // Configure un ItemSqlParameterSourceProvider à utiliser par le writer
                .sql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)") // Définit l'instruction SQL à utiliser pour les mises à jour de chaque élément.
                .dataSource(dataSource) // Configure la DataSource à utiliser
                .build(); // Valide la configuration et construit le JdbcBatchItemWriter
    }

    // Cette méthode définit le job.
    @Bean
    public Job importUserJob(JobCompletionNotificationListener listener, Step step1) {
        return jobBuilderFactory.get("importUserJob")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1)
                .end()
                .build();
    }

    // Cette méthode définit une étape simple.
    // Chaque étape peut impliquer un reader, un processor et un writer.
    @Bean
    public Step step1(JdbcBatchItemWriter<Person> writer) {
        return stepBuilderFactory.get("step1")
                .<Person, Person> chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer)
                .build();
    }
}
