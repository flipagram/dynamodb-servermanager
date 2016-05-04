package com.flipagram.dynamodb;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.flipagram.dynamodb.Archive;

public class ArchiveTest {

	@Before
	public void setUp()
			throws IOException {
		Archive.deleteTemporaryDirectory();
	}

	@Test
	public void testExtract()
			throws Exception {
		assertFalse(Archive.DYNAMO_DB_DIRECTORY.exists());
		File directory = Archive.extractToTemporaryDirectory();
		assertNotNull(directory);
		assertTrue(directory.exists());
		Archive.deleteTemporaryDirectory();
		assertFalse(Archive.DYNAMO_DB_DIRECTORY.exists());
	}

}
