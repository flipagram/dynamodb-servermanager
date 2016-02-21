package com.flipagram.junit.dynamodb;

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;

import org.rauschig.jarchivelib.ArchiveFormat;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;
import org.rauschig.jarchivelib.CompressionType;

public class Archive {

	public static final String CURRENT 		= "dynamodb_local_2016-01-07_1.0.tar.gz";
	public static final String VERIFY_FILE 	= "README.txt";

	public static final File DYNAMO_DB_DIRECTORY = new File(System.getProperty("java.io.tmpdir"), "junit-dynamodb");

	/**
	 * Deletes the temporary directory.
	 * @throws IOException
	 */
	public static void deleteTemporaryDirectory()
			throws IOException {
		if (!DYNAMO_DB_DIRECTORY.exists()) {
			return;
		}
		Files.walkFileTree(DYNAMO_DB_DIRECTORY.toPath(), new FileVisitor<Path>() {
			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
				return FileVisitResult.CONTINUE;
			}
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				Files.delete(file);
				return FileVisitResult.CONTINUE;
			}
			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
				return FileVisitResult.CONTINUE;
			}
			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
				Files.delete(dir);
				return FileVisitResult.CONTINUE;
			}
		});
	}

	/**
	 * Extracts the DynamoDB archive to a consistent temp directory, but
	 * only if it hasn't already been extracted.
	 * @return the directory extracted to
	 * @throws IOException on error
	 */
	public static File extractToTemporaryDirectory()
			throws IOException {
		extract(DYNAMO_DB_DIRECTORY);
		return DYNAMO_DB_DIRECTORY;
	}

	/**
	 * Extracts the DynamoDB archive to the given direction, but
	 * only if it hasn't already been extracted.  The intention
	 * is that no matter the return value of this method, the
	 * archive has been extracted.
	 * @param toDir the directory to extract to
	 * @return true if it was extracted.
	 * @throws IOException on error
	 */
	public static boolean extract(File toDir)
			throws IOException {
		if (!toDir.exists() && !toDir.mkdirs()) {
			throw new IllegalArgumentException(
					"Unable to create directory: "+toDir.getAbsolutePath());
		}
		if (!toDir.isDirectory()) {
			throw new IllegalArgumentException(
					"Not a directory: "+toDir.getAbsolutePath());
		}
		if (!toDir.canRead()) {
			throw new IllegalArgumentException(
					"Unable to read directory: "+toDir.getAbsolutePath());
		}

		// open stream, get size
		InputStream archiveStream = Archive.class.getResourceAsStream("/"+CURRENT);
		requireNonNull(archiveStream, "Unable to open archive stream");
		long archiveSize = 0;
		while (archiveStream.available() > 0) {
			int available = archiveStream.available();
			archiveSize += available;
			archiveStream.skip(available);
		}
		archiveStream.close();

		// extract it
		boolean extracted = false;
		File archive = new File(toDir, CURRENT);
		if (!archive.exists() || archive.length() != archiveSize) {
			if (archive.exists() && !archive.delete()) {
				throw new IllegalStateException(
						"Unable to delete potentially corrupt archive: "
						+ archive.getAbsolutePath());
			}

			archiveStream = Archive.class.getResourceAsStream("/"+CURRENT);
			requireNonNull(archiveStream, "Unable to open archive stream");
			FileOutputStream fops = new FileOutputStream(archive);
			byte[] buff = new byte[1024];
			while (archiveStream.available() > 0) {
				int read = archiveStream.read(buff);
				fops.write(buff, 0, read);
			}
			fops.flush();
			fops.close();
		}

		// extract the archive
		if (!new File(toDir, VERIFY_FILE).exists()) {
			extracted = true;
			Archiver archiver = ArchiverFactory.createArchiver(
					ArchiveFormat.TAR, CompressionType.GZIP);
			archiver.extract(archive, toDir);
		}

		return extracted;
	}

}
