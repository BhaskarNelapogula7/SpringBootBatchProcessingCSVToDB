package com.app.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import com.app.entity.Customer;
import com.app.listener.JobCompletionNotificationListener;
import com.app.listener.StepSkipListener;
import com.app.repository.CustomerRepository; 

@Configuration
@EnableBatchProcessing
public class SpringBatchConfig {

	
	private final Logger logger = LoggerFactory.getLogger(SpringBatchConfig.class);
	
	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private CustomerRepository customerRepository;

	@Autowired
	private CustomerItemWriter customerItemWriter;


//	@Value("${file.path}")
//	private String filePath;
	
	private final String filePath;

    @Autowired
    public SpringBatchConfig(@Value("${file.path}") String filePath) {
        this.filePath = filePath;
    }
	
	@Bean
	// @StepScope
	public FlatFileItemReader<Customer> reader() { // @Value("#{jobParameters['filePath']}") String filePath
		FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();

		itemReader.setResource(new FileSystemResource(filePath));
		itemReader.setName("csvReader");
		itemReader.setLinesToSkip(1);
		itemReader.setLineMapper(lineMapper());
		return itemReader;
	}

	private LineMapper<Customer> lineMapper() {

		DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setDelimiter(",");
		lineTokenizer.setStrict(false);
		lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");

		BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
		fieldSetMapper.setTargetType(Customer.class);

		lineMapper.setLineTokenizer(lineTokenizer);
		lineMapper.setFieldSetMapper(fieldSetMapper);
		return lineMapper;

	}

	@Bean
	public CustomerProcessor processor() {
		return new CustomerProcessor();
	}

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("csv-step").<Customer, Customer>chunk(10)
				.reader(reader())
				.processor(processor())
				.writer(customerItemWriter)
				.faultTolerant()
				.skipLimit(1000)
				.listener(skipListener())
				//.listener(new JobCompletionNotificationListener(filePath))
				.skipPolicy(skipPolicy())
				.build();
	}

	@Bean
	public Job runJob(JobCompletionNotificationListener listener) {
		return jobBuilderFactory.get("importCustomers")
				.listener(listener)
				.flow(step1())
				.end()
				.build();

	}

	@Bean
	public SkipPolicy skipPolicy() {
		return new ExceptionSkipPolicy();
	}

	@Bean
	public SkipListener<?, ?> skipListener() {
		System.out.println("calling the skiplistener---------");
		return new StepSkipListener();
	}
	
//	public class JobCompletionNotificationListener extends JobExecutionListenerSupport {
//
//        private final String filePath;
//
//        public JobCompletionNotificationListener(String filePath) {
//            this.filePath = filePath;
//        }
//
//        @Override
//        public void afterJob(JobExecution jobExecution) {
//            if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
//                deleteFile(filePath);
//                logger.info("File deleted successfully: " + filePath);
//            } else {
//                logger.error("Job did not complete successfully. File deletion skipped.");
//            }
//        }
//
//        private void deleteFile(String filePath) {
//            try {
//                Path fileToDelete = Paths.get(filePath);
//                Files.delete(fileToDelete);
//            } catch (IOException e) {
//                logger.error("Failed to delete file: " + filePath, e);
//            }
//        }
//    }
}

