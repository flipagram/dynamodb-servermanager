package com.flipagram.junit.dynamodb;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class LocalDynamoDBServer {

	private static final Logger LOGGER = Logger.getLogger(LocalDynamoDBServer.class.getName());

	public static final String DEFAULT_DATA_DIR = "./data";
	public static final File JAVA = new File(System.getProperty("java.home"), "bin/java");

	private Process process;
	private int port;

	public LocalDynamoDBServer() {
		Runtime.getRuntime().addShutdownHook(new Thread(this::stopForcibly));
	}

	public void start(int port, boolean delayTransientStatuses)
			throws IOException {
		start(port, delayTransientStatuses, DEFAULT_DATA_DIR, true, true);
	}

	public void start(int port) throws IOException {
		start(port, false, DEFAULT_DATA_DIR, true, true);
	}

	public void start() throws IOException {
		start(-1, false, DEFAULT_DATA_DIR, true, true);
	}

	public void start(
			int port, boolean delayTransientStatuses, String dbPath,
			boolean optimizeDbBeforeStartup, boolean sharedDb)
			throws IOException {

		File dbDir = new File(dbPath);
		if (!dbDir.exists()) {
			dbDir.mkdirs();
		}

		List<String> args = new ArrayList<>();
		args.add("-dbPath"); args.add(dbDir.getAbsolutePath());
		if (sharedDb) {
			args.add("-sharedDb");
		}
		if (optimizeDbBeforeStartup) {
			args.add("-optimizeDbBeforeStartup");
		}
		if (delayTransientStatuses) {
			args.add("-delayTransientStatuses");
		}
		start(port, args);
	}

	public void startInMemory()
			throws IOException {
		startInMemory(-1, false);
	}

	public void startInMemory(int port)
			throws IOException {
		startInMemory(port, false);
	}

	public void startInMemory(int port, boolean delayTransientStatuses)
			throws IOException {
		List<String> args = new ArrayList<>();
		args.add("-inMemory");
		if (delayTransientStatuses) {
			args.add("-delayTransientStatuses");
		}
		start(port, args);
	}

	/**
	 * Starts the server.
	 */
	public void start(int port, List<String> daemonArgs)
			throws IOException {
		if (isRunning()) {
			throw new IllegalStateException("Already running");
		}

		if (port == -1) {
			ServerSocket socket = new ServerSocket(0);
			port = socket.getLocalPort();
			socket.close();
			socket = null;
		}

		File daemonDir	= Archive.extractToTemporaryDirectory();
		File libDir		= new File(daemonDir, "./DynamoDBLocal_lib");
		File jarFile	= new File(daemonDir, "DynamoDBLocal.jar");

		List<String> args = new ArrayList<>();
		args.add(JAVA.getAbsolutePath());
		args.add("-Djava.library.path="+libDir.getAbsolutePath());
		args.add("-jar"); args.add(jarFile.getAbsolutePath());
		args.add("-port"); args.add(port+"");
		args.addAll(daemonArgs);

		LOGGER.info("Starting local DynamoDB servers: "+String.join(" ", args));
		this.port = port;
		this.process = new ProcessBuilder()
				.inheritIO()
				.command(args)
				.start();

		// give it 10 seconds to start
		long end = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
		while (System.currentTimeMillis() < end) {
			try (Socket client = new Socket("localhost", port)) {
				if (client.isConnected()) {
					break;
				}
			} catch(IOException ioe) {
				try {
					Thread.sleep(10L);
				} catch (InterruptedException e) {
					throw new IOException(e);
				}
			}
			if (!process.isAlive()) {
				break;
			}
		}

		LOGGER.info("Started local DynamoDB server on port "+port);
		if (!process.isAlive()) {
			throw new IOException("Server didn't startup within 10 seconds");
		}
	}

	public boolean isRunning() {
		return process != null && process.isAlive();
	}

	public int getPort() {
		return this.port;
	}

	public int stop()
			throws InterruptedException {
		if (!isRunning()) {
			return 0;
		}
		process.destroy();
		int ret = awaitTermination();
		process = null;
		return ret;
	}

	public void stopForcibly() {
		if (!isRunning()) {
			return;
		}
		process.destroyForcibly();
		process = null;
	}

	public int awaitTermination()
			throws InterruptedException {
		if (!isRunning()) {
			return 0;
		}
		return process.waitFor();
	}

	public int awaitTermination(long timeout, TimeUnit unit)
			throws InterruptedException {
		if (!isRunning()) {
			return 0;
		}
		int ret = -1;
		if (!process.waitFor(timeout, unit)) {
			throw new InterruptedException("Wait timedout");
		}
		ret = process.exitValue();
		return ret;
	}

}
