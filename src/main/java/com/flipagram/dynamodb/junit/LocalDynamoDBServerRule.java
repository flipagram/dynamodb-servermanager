package com.flipagram.dynamodb.junit;

import org.junit.rules.ExternalResource;

import com.flipagram.dynamodb.LocalDynamoDBServer;

public class LocalDynamoDBServerRule
		extends ExternalResource {

	private LocalDynamoDBServer server;
	private final boolean inMemory;
	private final int port;
	private final boolean delayTransientStatuses;
	private final String dbPath;
	private final boolean optimizeDbBeforeStartup;
	private final boolean sharedDb;

	public static LocalDynamoDBServerRule create(boolean inMemory) {
		return create(inMemory, false);
	}

	public static LocalDynamoDBServerRule create(boolean inMemory, boolean delayTransientStatuses) {
		return new LocalDynamoDBServerRule(inMemory, -1, delayTransientStatuses, null, false, false);
	}

	public LocalDynamoDBServerRule(
			boolean inMemory,
			int port,
			boolean delayTransientStatuses,
			String dbPath,
			boolean optimizeDbBeforeStartup,
			boolean sharedDb) {
		this.inMemory 					= inMemory;
		this.port 						= port;
		this.delayTransientStatuses 	= delayTransientStatuses;
		this.dbPath 					= dbPath;
		this.optimizeDbBeforeStartup 	= optimizeDbBeforeStartup;
		this.sharedDb 					= sharedDb;
	}

	public LocalDynamoDBServer getLocalDynamoDBServer() {
		return server;
	}

	@Override
	protected void before() throws Throwable {
		server = new LocalDynamoDBServer();
		if (inMemory) {
			server.startInMemory(port, delayTransientStatuses);
		} else {
			server.start(port, delayTransientStatuses, dbPath, optimizeDbBeforeStartup, sharedDb);
		}
	}

	@Override
	protected void after() {
		try {
			server.stop();
			if (server.awaitTermination() != 0) {
				server.stopForcibly();
			}
		} catch(Throwable t) {
			throw new RuntimeException("Unable to tear down DynamoDB server", t);
		}
	}

}
