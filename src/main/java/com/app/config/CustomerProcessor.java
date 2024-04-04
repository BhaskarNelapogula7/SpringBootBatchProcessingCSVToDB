package com.app.config;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.item.ItemProcessor;

import com.app.entity.Customer;

public class CustomerProcessor implements ItemProcessor<Customer, Customer> {

	//private static final String RESOURCE_FOLDER = "src/main/resources";

	private static final String CSV_FILE_NAME = "failed_customers.csv";

	@Override
	public Customer process(Customer customer) throws Exception {
		if (isValidCustomer(customer)) {
			return customer;
		}
		storeFailedCustomer(customer);
		return null;
	}

	private void storeFailedCustomer(Customer customer) throws Exception {

		if (!isValidCountry(customer.getCountry())) {

			System.out.println("Validation Failed - ID: " + customer.getId() + ", Field: Country, Value: "
					+ customer.getCountry());
		}
		if (!isValidContactNo(customer.getContactNo())) {

			System.out.println("Validation Failed - ID: " + customer.getId() + ", Field: Contact No, Value: "
					+ customer.getContactNo());
		}
		if (!isValidEmail(customer.getEmail())) {

			System.out.println(
					"Validation Failed - ID: " + customer.getId() + ", Field: Email, Value: " + customer.getEmail());
		}
		if (!isValidFirstName(customer.getFirstName())) {

			System.out.println("Validation Failed - ID: " + customer.getId() + ", Field: First Name, Value: "
					+ customer.getFirstName());
		}
		if (!isValidLastName(customer.getLastName())) {

			System.out.println("Validation Failed - ID: " + customer.getId() + ", Field: Last Name, Value: "
					+ customer.getLastName());
		}
		if (!isValidGender(customer.getGender())) {

			System.out.println(
					"Validation Failed - ID: " + customer.getId() + ", Field: Gender, Value: " + customer.getGender());
		}
		if (!isValidDob(customer.getDob())) {

			System.out.println("Validation Failed - ID: " + customer.getId() + ", Field: Date of Birth, Value: "
					+ customer.getDob());
		}

		List<Customer> failedCustomers = new ArrayList<>();
		failedCustomers.add(customer);

		try (FileWriter writer = new FileWriter(getCsvFilePath(), true)) {
			for (Customer failedCustomer : failedCustomers) {
				String csvLine = convertToCsvLine(failedCustomer);
				writer.write(csvLine);
				writer.write(System.lineSeparator());
			}
			writer.flush();
		} catch (IOException e) {
			throw new Exception("Failed to store failed customers to CSV file.", e);
		}
	}

	private boolean isValidCustomer(Customer customer) {

		return isValidCountry(customer.getCountry()) && isValidContactNo(customer.getContactNo())
				&& isValidEmail(customer.getEmail()) && isValidFirstName(customer.getFirstName())
				&& isValidLastName(customer.getLastName()) && isValidGender(customer.getGender())
				&& isValidDob(customer.getDob());
	}

	private boolean isValidCountry(String country) {
		return country != null && !country.isEmpty() && country.length() <= 50;
	}

	private boolean isValidContactNo(String contactNo) {
		return contactNo != null && contactNo.matches("\\d{10}");
	}

	private boolean isValidEmail(String email) {
		return email != null && email.matches("^[A-Za-z0-9+_.-]+@(.+)$");
	}

	private boolean isValidFirstName(String firstName) {
		return firstName != null && !firstName.isEmpty() && firstName.length() <= 50;
	}

	private boolean isValidLastName(String lastName) {
		return lastName != null && !lastName.isEmpty() && lastName.length() <= 50;
	}

	private boolean isValidGender(String gender) {
		return gender != null && (gender.equalsIgnoreCase("Male") || gender.equalsIgnoreCase("Female"));
	}

	private boolean isValidDob(String dob) {
		return dob != null && !dob.isEmpty();
	}

	private String convertToCsvLine(Customer customer) {
		StringBuilder sb = new StringBuilder();
		sb.append(customer.getId()).append(",");
		sb.append(customer.getFirstName()).append(",");
		sb.append(customer.getLastName()).append(",");
		sb.append(customer.getEmail()).append(",");
		sb.append(customer.getGender()).append(",");
		sb.append(customer.getContactNo()).append(",");
		sb.append(customer.getCountry()).append(",");
		sb.append(customer.getDob());
		return sb.toString();
	}

	private String getCsvFilePath() throws IOException {

		Path resourcePath = Paths.get("src", "main", "resources");
		Path csvFilePath = resourcePath.resolve(CSV_FILE_NAME);

		// Create the resource folder if it doesn't exist
		if (!Files.exists(resourcePath)) {
			Files.createDirectories(resourcePath);
		}

		// Create the CSV file if it doesn't exist
		if (!Files.exists(csvFilePath)) {
			Files.createFile(csvFilePath);
		}
		return csvFilePath.toString();
	}
}
