package com.techprimers.springbatchexample1.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import com.techprimers.springbatchexample1.model.User;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {

    @Bean
    public Job job(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory,
	    ItemReader<User> itemReader, ItemProcessor<User, User> itemProcessor, ItemWriter<User> itemWriter) {

	Step step1 = stepBuilderFactory.get("ETL-file-load-Step-1").<User, User>chunk(10).reader(itemReader)
		.processor(itemProcessor).writer(itemWriter).build();

	Step step2 = stepBuilderFactory.get("ETL-file-load-Step-2").<User, User>chunk(10000).reader(itemReader)
		.processor(itemProcessor).writer(itemWriter).build();

	return jobBuilderFactory.get("ETL-Load-Job").incrementer(new RunIdIncrementer()).flow(step1).next(step2).build()
		.build();
    }

    @Bean
    public FlatFileItemReader<User> itemReader(@Value("${input}") Resource resource) {
	System.out.println("itemReader called");
	FlatFileItemReader<User> flatFileItemReader = new FlatFileItemReader<>();
	flatFileItemReader.setResource(resource);
	flatFileItemReader.setName("CSV-Reader");
	flatFileItemReader.setLinesToSkip(1);
	flatFileItemReader.setLineMapper(lineMapper());
	return flatFileItemReader;
    }

    @Bean
    public LineMapper<User> lineMapper() {

	DefaultLineMapper<User> defaultLineMapper = new DefaultLineMapper<>();
	DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();

	lineTokenizer.setDelimiter(",");
	lineTokenizer.setStrict(false);
	lineTokenizer.setNames(new String[] { "id", "name", "dept", "salary" });

	BeanWrapperFieldSetMapper<User> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
	fieldSetMapper.setTargetType(User.class);

	defaultLineMapper.setLineTokenizer(lineTokenizer);
	defaultLineMapper.setFieldSetMapper(fieldSetMapper);

	return defaultLineMapper;
    }

}
