package net.mezzdev.filewatcher;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 10, unit = TimeUnit.SECONDS)
class FileWatcherOsIntegrationTest {
	private static final Duration QUIET_TIME = Duration.ofMillis(25);
	private static final Duration DIRECTORY_RECHECK_INTERVAL = Duration.ofMillis(1);
	private static final Duration EVENT_TIMEOUT = Duration.ofSeconds(5);

	@TempDir
	Path tempDir;

	@Test
	void defaultWatchServiceDetectsCreatedWatchedFile() throws Exception {
		Path watchedFile = tempDir.resolve("created.toml");
		AtomicInteger callbackCount = new AtomicInteger();
		CountDownLatch callbackLatch = new CountDownLatch(1);
		FileWatcherThread thread = createThread();
		try {
			thread.addCallback(watchedFile, () -> {
				callbackCount.incrementAndGet();
				callbackLatch.countDown();
			});
			thread.runIteration();

			writeNewFile(watchedFile, "created");

			runUntilCallback(thread, callbackLatch);
		} finally {
			thread.shutdown();
		}

		assertTrue(callbackCount.get() >= 1, "expected create callback to run");
	}

	@Test
	void defaultWatchServiceDetectsModifiedWatchedFile() throws Exception {
		Path watchedFile = tempDir.resolve("modified.toml");
		writeNewFile(watchedFile, "initial");
		AtomicInteger callbackCount = new AtomicInteger();
		CountDownLatch callbackLatch = new CountDownLatch(1);
		FileWatcherThread thread = createThread();
		try {
			thread.addCallback(watchedFile, () -> {
				callbackCount.incrementAndGet();
				callbackLatch.countDown();
			});
			thread.runIteration();

			overwriteFile(watchedFile, "modified");

			runUntilCallback(thread, callbackLatch);
		} finally {
			thread.shutdown();
		}

		assertTrue(callbackCount.get() >= 1, "expected modify callback to run");
	}

	@Test
	void defaultWatchServiceDetectsDeletedWatchedFile() throws Exception {
		Path watchedFile = tempDir.resolve("deleted.toml");
		writeNewFile(watchedFile, "initial");
		AtomicInteger callbackCount = new AtomicInteger();
		CountDownLatch callbackLatch = new CountDownLatch(1);
		FileWatcherThread thread = createThread();
		try {
			thread.addCallback(watchedFile, () -> {
				callbackCount.incrementAndGet();
				callbackLatch.countDown();
			});
			thread.runIteration();

			Files.delete(watchedFile);

			runUntilCallback(thread, callbackLatch);
		} finally {
			thread.shutdown();
		}

		assertTrue(callbackCount.get() >= 1, "expected delete callback to run");
	}

	@Test
	void defaultWatchServiceIgnoresOtherFilesInWatchedDirectory() throws Exception {
		Path watchedFile = tempDir.resolve("watched.toml");
		Path otherFile = tempDir.resolve("other.toml");
		AtomicInteger callbackCount = new AtomicInteger();
		FileWatcherThread thread = createThread();
		try {
			thread.addCallback(watchedFile, callbackCount::incrementAndGet);
			thread.runIteration();

			writeNewFile(otherFile, "other");

			runFor(thread, Duration.ofMillis(500));
		} finally {
			thread.shutdown();
		}

		assertEquals(0, callbackCount.get(), "callback ran for an unrelated file");
	}

	@Test
	void defaultWatchServiceWatchesDirectoryCreatedAfterCallbackRegistration() throws Exception {
		Path watchedDirectory = tempDir.resolve("new-directory");
		Path watchedFile = watchedDirectory.resolve("created.toml");
		AtomicInteger callbackCount = new AtomicInteger();
		CountDownLatch callbackLatch = new CountDownLatch(1);
		FileWatcherThread thread = createThread();
		try {
			thread.addCallback(watchedFile, () -> {
				callbackCount.incrementAndGet();
				callbackLatch.countDown();
			});
			thread.runIteration();

			Files.createDirectory(watchedDirectory);
			thread.runIteration();
			writeNewFile(watchedFile, "created");

			runUntilCallback(thread, callbackLatch);
		} finally {
			thread.shutdown();
		}

		assertTrue(callbackCount.get() >= 1, "expected callback for file in newly created directory");
	}

	@Test
	void realCallbacksRunOnNonDaemonThread() throws Exception {
		Path watchedFile = tempDir.resolve("thread.toml");
		writeNewFile(watchedFile, "initial");
		AtomicBoolean callbackThreadWasDaemon = new AtomicBoolean(true);
		CountDownLatch callbackLatch = new CountDownLatch(1);
		FileWatcherThread thread = createThread();
		try {
			thread.addCallback(watchedFile, () -> {
				callbackThreadWasDaemon.set(Thread.currentThread().isDaemon());
				callbackLatch.countDown();
			});
			thread.runIteration();

			overwriteFile(watchedFile, "modified");

			runUntilCallback(thread, callbackLatch);
		} finally {
			thread.shutdown();
		}

		assertFalse(callbackThreadWasDaemon.get(), "callbacks should run on a non-daemon thread");
	}

	@Test
	void fileWatcherDetectsModifiedWatchedFile() throws Exception {
		Path watchedFile = tempDir.resolve("public.toml");
		writeNewFile(watchedFile, "initial");
		CountDownLatch callbackLatch = new CountDownLatch(1);

		try (FileWatcher fileWatcher = new FileWatcher(
			"FileWatcher OS integration test",
			QUIET_TIME,
			DIRECTORY_RECHECK_INTERVAL
		)) {
			fileWatcher.addCallback(watchedFile, callbackLatch::countDown);
			fileWatcher.start();

			runMutationUntilCallback(callbackLatch, () -> overwriteFile(watchedFile, "modified"));
		}
	}

	private static FileWatcherThread createThread() throws IOException {
		return new FileWatcherThread("FileWatcher OS integration test", QUIET_TIME, DIRECTORY_RECHECK_INTERVAL);
	}

	private static void runUntilCallback(FileWatcherThread thread, CountDownLatch callbackLatch) throws Exception {
		long deadline = System.nanoTime() + EVENT_TIMEOUT.toNanos();
		while (callbackLatch.getCount() > 0 && System.nanoTime() < deadline) {
			thread.runIteration();
		}
		assertTrue(callbackLatch.await(100, TimeUnit.MILLISECONDS), "expected callback to run");
	}

	private static void runFor(FileWatcherThread thread, Duration duration) throws Exception {
		long deadline = System.nanoTime() + duration.toNanos();
		while (System.nanoTime() < deadline) {
			thread.runIteration();
		}
	}

	private static void runMutationUntilCallback(CountDownLatch callbackLatch, ThrowingRunnable mutation) throws Exception {
		long deadline = System.nanoTime() + EVENT_TIMEOUT.toNanos();
		while (callbackLatch.getCount() > 0 && System.nanoTime() < deadline) {
			mutation.run();
			if (callbackLatch.await(100, TimeUnit.MILLISECONDS)) {
				return;
			}
		}
		assertEquals(0, callbackLatch.getCount(), "expected callback to run");
	}

	private static void writeNewFile(Path file, String content) throws IOException {
		Files.writeString(
			file,
			uniqueContent(content),
			StandardCharsets.UTF_8,
			StandardOpenOption.CREATE_NEW,
			StandardOpenOption.WRITE
		);
	}

	private static void overwriteFile(Path file, String content) throws IOException {
		Files.writeString(
			file,
			uniqueContent(content),
			StandardCharsets.UTF_8,
			StandardOpenOption.TRUNCATE_EXISTING,
			StandardOpenOption.WRITE
		);
	}

	private static String uniqueContent(String content) {
		return content + "-" + System.nanoTime();
	}

	@FunctionalInterface
	private interface ThrowingRunnable {
		void run() throws Exception;
	}
}
