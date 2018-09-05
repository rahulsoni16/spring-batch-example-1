package com.techprimers.springbatchexample1.config;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.H2PagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import com.techprimers.springbatchexample1.batch.SuperProcessor;
import com.techprimers.springbatchexample1.model.SuperUser;
import com.techprimers.springbatchexample1.model.User;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {

    // @Autowired
    // StepBuilderFactory stepBuilderFactory;

    @Autowired
    SuperProcessor superProcessor;

    @Autowired
    ItemReader<User> itemReader;

    @Autowired
    JdbcPagingItemReader<User> dbItemReader;

    @Autowired
    ItemReader<User> databaseItemReader;

    @Autowired
    ItemProcessor<User, User> itemUserProcessor;

    @Autowired
    ItemProcessor<User, SuperUser> itemSuperUserProcessor;

    @Autowired
    ItemWriter<User> itemUserWriter;

    @Autowired
    ItemWriter<SuperUser> itemSuperUserWriter;

    @Value("${input}")
    Resource resource;

    @Bean
    public Job job(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
	// Step 1 reads the data from file and persist to user table
	Step step1 = stepBuilderFactory.get("ETL-file-load-Step-1").<User, User>chunk(1000).reader(itemReader)
		.processor(itemUserProcessor).writer(itemUserWriter).build();

	// Step 1 reads the data from user table and persist to super_user table
	Step step2 = stepBuilderFactory.get("ETL-file-load-Step-2").<User, SuperUser>chunk(10000).reader(dbItemReader)
		.processor(itemSuperUserProcessor).writer(itemSuperUserWriter).build();

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

    @Bean
    public FlatFileItemReader<User> itemReader() {
	System.out.println("itemReader called");
	FlatFileItemReader<User> flatFileItemReader = new FlatFileItemReader<>();
	flatFileItemReader.setResource(resource);
	flatFileItemReader.setName("CSV-Reader");
	flatFileItemReader.setLinesToSkip(1);
	flatFileItemReader.setLineMapper(lineMapper());
	return flatFileItemReader;
    }

    @Bean
    public JdbcPagingItemReader<User> databaseItemReader(DataSource dataSource) {
	System.out.println("databaseItemReader called");
	JdbcPagingItemReader<User> databaseReader = new JdbcPagingItemReader<>();

	databaseReader.setDataSource(dataSource);
	databaseReader.setPageSize(1000);

	PagingQueryProvider queryProvider = createQueryProvider();
	databaseReader.setQueryProvider(queryProvider);

	databaseReader.setRowMapper(new BeanPropertyRowMapper<>(User.class));

	return databaseReader;
    }

    private PagingQueryProvider createQueryProvider() {
	H2PagingQueryProvider queryProvider = new H2PagingQueryProvider();

	queryProvider.setSelectClause("select ID, DEPT ,NAME ,SALARY ");
	queryProvider.setFromClause("from USER ");
	// queryProvider.setWhereClause("where ID < 1000 ");
	queryProvider.setSortKeys(sortByEmailAddressAsc());

	return queryProvider;
    }

    private Map<String, Order> sortByEmailAddressAsc() {
	Map<String, Order> sortConfiguration = new HashMap<>();
	sortConfiguration.put("ID", Order.ASCENDING);
	return sortConfiguration;
    }

}
