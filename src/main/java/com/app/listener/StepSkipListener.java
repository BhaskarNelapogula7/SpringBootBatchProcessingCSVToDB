package com.app.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.SkipListener;
import org.springframework.stereotype.Component;

import com.app.entity.Customer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.SneakyThrows;


public class StepSkipListener implements SkipListener<Customer, Number> {

	private final Logger logger = LoggerFactory.getLogger(StepSkipListener.class);

	@Override
	public void onSkipInRead(Throwable throwable) {
		
		System.out.println("a failure on read {}");
		
		logger.info("A failure on read {} ", throwable.getMessage());
		
	}

	@Override
	public void onSkipInWrite(Number item, Throwable throwable) {
		logger.info("A failure on write {} , item {}", throwable.getMessage(), item);
	}

	@Override
	public void onSkipInProcess(Customer customer, Throwable throwable) {
		try {
			logger.info("Item {}  was skipped due to the exception  {}",
					new ObjectMapper().writeValueAsString(customer), throwable.getMessage());
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}
}