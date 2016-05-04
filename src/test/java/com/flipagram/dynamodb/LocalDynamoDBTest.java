package com.flipagram.dynamodb;

import static org.junit.Assert.assertFalse;

import java.util.logging.Logger;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.flipagram.dynamodb.LocalDynamoDBServer;

public class LocalDynamoDBTest {

	private static final Logger LOGGER = Logger.getLogger(LocalDynamoDBTest.class.getName());

	private LocalDynamoDBServer server;

	@Before
	public void setUp() {
		server = new LocalDynamoDBServer();
	}

	@After
	public void tearDown() {
		server.stopForcibly();
	}

	@Test
	public void testStartInMemory()
			throws Exception {
		server.startInMemory();
		assertTrue(server.isRunning());
		assertTrue(server.getPort() > 0);
		server.stop();
		server.awaitTermination();
		assertFalse(server.isRunning());
	}

	@Test
	public void testStart()
			throws Exception {
		server.start();
		assertTrue(server.isRunning());
		assertTrue(server.getPort() > 0);
		server.stop();
		server.awaitTermination();
		assertFalse(server.isRunning());
	}

	@Test
	public void testConnectAndUse()
			throws Exception {
		server.startInMemory();
		assertTrue(server.isRunning());

		AmazonDynamoDBClient client = new AmazonDynamoDBClient(new BasicAWSCredentials("Fake", "Fake"));
		client.setEndpoint("http://localhost:"+server.getPort());

		CreateTableResult tableResult = client.createTable(new CreateTableRequest()
				.withTableName("test_tabe")
				.withKeySchema(new KeySchemaElement()
	                .withAttributeName("id")
	                .withKeyType(KeyType.HASH))
				.withAttributeDefinitions(new AttributeDefinition()
	                .withAttributeName("id")
	                .withAttributeType(ScalarAttributeType.S))
				.withProvisionedThroughput(new ProvisionedThroughput()
			        .withReadCapacityUnits(5L)
			        .withWriteCapacityUnits(2L)));
		assertNotNull(tableResult);
		LOGGER.info(tableResult.getTableDescription().toString());

		ListTablesResult result = client.listTables();
		assertNotNull(result);
		LOGGER.info(result.getTableNames().toString());
	}

}
