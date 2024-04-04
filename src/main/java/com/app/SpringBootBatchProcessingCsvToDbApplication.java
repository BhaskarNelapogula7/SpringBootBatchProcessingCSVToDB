package com.app;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import com.app.config.SpringBatchConfig;

@SpringBootApplication
@EnableScheduling
public class SpringBootBatchProcessingCsvToDbApplication {
	
	private final Logger logger = LoggerFactory.getLogger(SpringBootBatchProcessingCsvToDbApplication.class);
	
	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	private Job job;
	
	@Autowired
	private ResourceLoader resourceLoader;
	
	@Value("${file.path}")
	private String filePath;
	
	public static void main(String[] args) {
		SpringApplication.run(SpringBootBatchProcessingCsvToDbApplication.class, args);
	}

	//@Scheduled(fixedRate = 50000) // fixedDelay = 5000
	@Scheduled(cron="${cronExpression}")
	public String importCsvToDBJob() throws InterruptedException, IOException {
		JobParameters jobParameters = new JobParametersBuilder().addLong("startAt", System.currentTimeMillis())
				.toJobParameters();
		try {
			JobExecution jobExecution = jobLauncher.run(job, jobParameters);
			logger.info("Batch job has been invoked");
			
	// Check if the job is completed successfully
//	        if (jobExecution.getExitStatus().equals(ExitStatus.COMPLETED)) {
//	            Resource resource = resourceLoader.getResource(filePath);
//	            File file = resource.getFile();
//	            boolean deleted = file.delete();
//	            if (deleted) {
//	                logger.info("File deleted successfully: " + filePath);
//	            } else {
//	                logger.error("Failed to delete file: " + filePath);
//	            }
//	        } else {
//	            logger.error("Job did not complete successfully. File deletion skipped.");
//	        }
			
		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
				| JobParametersInvalidException e) {
			e.printStackTrace();
			logger.error("Failed to invoke batch job", e);
		}
		return "Batch job has been invoked";
	}
}

//	@Scheduled(fixedRate = 50000) // fixedDelay = 5000 D:\DataFolder\
//	public void importCsvToDBJob() throws InterruptedException {
//
//		String filePath = "D:/DataFolder/customer1.csv"; // Specify the file path here D:\DataFolder
//
//		JobParameters jobParameters = new JobParametersBuilder().addString("filePath", filePath)
//				.addLong("startAt", System.currentTimeMillis()).toJobParameters();
//
//		try {
//			jobLauncher.run(job, jobParameters);
//
//			logger.info("Batch job has been invoked");
//		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
//				| JobParametersInvalidException e) {
//			e.printStackTrace();
//			logger.error("Failed to invoke batch job", e);
//		}
//
//	}
//}
