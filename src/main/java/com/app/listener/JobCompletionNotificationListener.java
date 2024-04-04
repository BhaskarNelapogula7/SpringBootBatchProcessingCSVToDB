package com.app.listener;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;


@Component
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {
	
	private final Logger logger = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    private final String filePath;

    public JobCompletionNotificationListener(@Value("${file.path}") String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            deleteFile(filePath);
            logger.info("File deleted successfully: " + filePath);
        } else {
            logger.error("Job did not complete successfully. File deletion skipped.");
        }
    }

    private void deleteFile(String filePath) {
        try {
            Path fileToDelete = Paths.get(filePath);
            Files.delete(fileToDelete);
        } catch (IOException e) {
            logger.error("Failed to delete file: " + filePath, e);
        }
    }
}
