package net.mezzdev.filewatcher;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileWatcherTest {
	private static final Duration VALID_QUIET_TIME = Duration.ofMillis(1);
	private static final Duration VALID_DIRECTORY_RECHECK_INTERVAL = Duration.ofMillis(1);
	private static final Path FIRST_PATH = Path.of("config/client.toml");
	private static final Path SECOND_PATH = Path.of("config/server.toml");

	@TempDir
	Path tempDir;

	@SuppressWarnings({"DataFlowIssue", "resource"})
	@Test
	void rejectsNullPublicConstructorArguments() {
		assertThrows(NullPointerException.class, () -> new FileWatcher((String) null));
	}

	@Test
	void publicConstructorCreatesUsableThreadWatcher() {
		FileWatcher fileWatcher = new FileWatcher("FileWatcher public constructor test");
		try {
			fileWatcher.addCallback(tempDir.resolve("config.toml"), () -> {});
			fileWatcher.start();
		} finally {
			fileWatcher.close();
		}
	}

	@SuppressWarnings({"DataFlowIssue", "resource"})
	@Test
	void rejectsNullPackageConstructorArguments() {
		assertThrows(NullPointerException.class, () -> new FileWatcher(
			null,
			VALID_QUIET_TIME,
			VALID_DIRECTORY_RECHECK_INTERVAL
		));
		assertThrows(NullPointerException.class, () -> new FileWatcher(
			"test",
			null,
			VALID_DIRECTORY_RECHECK_INTERVAL
		));
		assertThrows(NullPointerException.class, () -> new FileWatcher(
			"test",
			VALID_QUIET_TIME,
			null
		));
		assertThrows(NullPointerException.class, () -> new FileWatcher(
			"test",
			VALID_QUIET_TIME,
			VALID_DIRECTORY_RECHECK_INTERVAL,
			null
		));
	}

	@SuppressWarnings("resource")
	@Test
	void rejectsQuietTimeShorterThanOneMillisecond() {
		assertThrows(IllegalArgumentException.class, () -> new FileWatcher(
			"test",
			Duration.ZERO,
			VALID_DIRECTORY_RECHECK_INTERVAL
		));
		assertThrows(IllegalArgumentException.class, () -> new FileWatcher(
			"test",
			Duration.ofNanos(1),
			VALID_DIRECTORY_RECHECK_INTERVAL
		));
	}

	@SuppressWarnings("resource")
	@Test
	void rejectsDirectoryRecheckIntervalShorterThanOneMillisecond() {
		assertThrows(IllegalArgumentException.class, () -> new FileWatcher(
			"test",
			VALID_QUIET_TIME,
			Duration.ZERO
		));
		assertThrows(IllegalArgumentException.class, () -> new FileWatcher(
			"test",
			VALID_QUIET_TIME,
			Duration.ofNanos(1)
		));
	}

	@Test
	void watcherFactoryFailureCreatesUnavailableWatcher() {
		try (FileWatcher fileWatcher = new FileWatcher(
			"test",
			VALID_QUIET_TIME,
			VALID_DIRECTORY_RECHECK_INTERVAL,
			(threadName, quietTime, directoryRecheckInterval) -> {
				throw new UnsupportedOperationException("watching is unavailable");
			}
		)) {
			assertDoesNotThrow(() -> {
				fileWatcher.addCallback(FIRST_PATH, () -> {});
				fileWatcher.start();
				fileWatcher.close();
			});
		}
	}

	@SuppressWarnings({"DataFlowIssue"})
	@Test
	void rejectsNullCallbackArguments() {
		try (FileWatcher fileWatcher = createTestWatcher()) {
			assertThrows(NullPointerException.class, () -> fileWatcher.addCallback(null, () -> {}));
			assertThrows(NullPointerException.class, () -> fileWatcher.addCallback(FIRST_PATH, null));
		}
	}

	@SuppressWarnings({"DataFlowIssue"})
	@Test
	void unavailableWatcherStillValidatesCallbackArguments() {
		try (FileWatcher fileWatcher = unavailableWatcher()) {
			assertThrows(NullPointerException.class, () -> fileWatcher.addCallback(null, () -> {}));
			assertThrows(NullPointerException.class, () -> fileWatcher.addCallback(FIRST_PATH, null));
		}
	}

	@Test
	void addCallbackForwardsToWatcher() {
		RecordingWatcher watcher = new RecordingWatcher();
		Runnable callback = () -> {};

		try (FileWatcher fileWatcher = new FileWatcher(watcher)) {
			fileWatcher.addCallback(FIRST_PATH, callback);
		}

		assertEquals(List.of(FIRST_PATH), watcher.paths);
		assertSame(callback, watcher.callbacks.get(0));
	}

	@Test
	void addCallbackForwardsEachRegistrationInOrder() {
		RecordingWatcher watcher = new RecordingWatcher();
		Runnable firstCallback = () -> {};
		Runnable secondCallback = () -> {};

		try (FileWatcher fileWatcher = new FileWatcher(watcher)) {
			fileWatcher.addCallback(FIRST_PATH, firstCallback);
			fileWatcher.addCallback(SECOND_PATH, secondCallback);
		}

		assertEquals(List.of(FIRST_PATH, SECOND_PATH), watcher.paths);
		assertSame(firstCallback, watcher.callbacks.get(0));
		assertSame(secondCallback, watcher.callbacks.get(1));
	}

	@Test
	void addCallbackAfterStartForwardsToWatcher() {
		RecordingWatcher watcher = new RecordingWatcher();
		Runnable callback = () -> {};

		try (FileWatcher fileWatcher = new FileWatcher(watcher)) {
			fileWatcher.start();
			fileWatcher.addCallback(FIRST_PATH, callback);
		}

		assertEquals(1, watcher.startCount());
		assertEquals(List.of(FIRST_PATH), watcher.paths);
		assertSame(callback, watcher.callbacks.get(0));
	}

	@Test
	void addCallbackAfterCloseDoesNothing() {
		RecordingWatcher watcher = new RecordingWatcher();
		Runnable callback = () -> {};

		FileWatcher fileWatcher = new FileWatcher(watcher);
		fileWatcher.close();
		fileWatcher.addCallback(FIRST_PATH, callback);

		assertEquals(List.of(), watcher.paths);
		assertEquals(List.of(), watcher.callbacks);
		assertEquals(1, watcher.shutdownCount());
	}

	@Test
	void startForwardsToWatcher() {
		RecordingWatcher watcher = new RecordingWatcher();

		try (FileWatcher fileWatcher = new FileWatcher(watcher)) {
			fileWatcher.start();

			assertEquals(1, watcher.startCount());
			assertEquals(0, watcher.shutdownCount());
		}
	}

	@Test
	void startIsIdempotent() {
		RecordingWatcher watcher = new RecordingWatcher();

		try (FileWatcher fileWatcher = new FileWatcher(watcher)) {
			fileWatcher.start();
			fileWatcher.start();
			fileWatcher.start();
		}

		assertEquals(1, watcher.startCount());
	}

	@Test
	void closeForwardsToWatcher() {
		RecordingWatcher watcher = new RecordingWatcher();

		FileWatcher fileWatcher = new FileWatcher(watcher);
		fileWatcher.close();

		assertEquals(0, watcher.startCount());
		assertEquals(1, watcher.shutdownCount());
	}

	@Test
	void closeIsIdempotent() {
		RecordingWatcher watcher = new RecordingWatcher();

		FileWatcher fileWatcher = new FileWatcher(watcher);
		fileWatcher.close();
		fileWatcher.close();
		fileWatcher.close();

		assertEquals(1, watcher.shutdownCount());
	}

	@Test
	void tryWithResourcesClosesWatcher() {
		RecordingWatcher watcher = new RecordingWatcher();

		//noinspection EmptyTryBlock
		try (FileWatcher ignored = new FileWatcher(watcher)) {
			// Closing is performed by try-with-resources.
		}

		assertEquals(1, watcher.shutdownCount());
	}

	@Test
	void closeAfterStartShutsDownWatcherOnce() {
		RecordingWatcher watcher = new RecordingWatcher();

		FileWatcher fileWatcher = new FileWatcher(watcher);
		fileWatcher.start();
		fileWatcher.close();
		fileWatcher.close();

		assertEquals(1, watcher.startCount());
		assertEquals(1, watcher.shutdownCount());
	}

	@Test
	void startAfterCloseDoesNothing() {
		RecordingWatcher watcher = new RecordingWatcher();

		FileWatcher fileWatcher = new FileWatcher(watcher);
		fileWatcher.close();
		fileWatcher.start();

		assertEquals(0, watcher.startCount());
		assertEquals(1, watcher.shutdownCount());
	}

	@Test
	void startAfterStartedAndClosedDoesNotRestartWatcher() {
		RecordingWatcher watcher = new RecordingWatcher();

		FileWatcher fileWatcher = new FileWatcher(watcher);
		fileWatcher.start();
		fileWatcher.close();
		fileWatcher.start();

		assertEquals(1, watcher.startCount());
		assertEquals(1, watcher.shutdownCount());
	}

	@Test
	void unavailableWatcherNoOps() {
		try (FileWatcher fileWatcher = unavailableWatcher()) {
			assertDoesNotThrow(() -> {
				fileWatcher.addCallback(FIRST_PATH, () -> {
				});
				fileWatcher.start();
				fileWatcher.start();
				fileWatcher.close();
				fileWatcher.close();
				fileWatcher.start();
			});
		}
	}

	@Test
	void concurrentStartOnlyStartsWatcherOnce() throws Exception {
		RecordingWatcher watcher = new RecordingWatcher();

		try (FileWatcher fileWatcher = new FileWatcher(watcher)) {
			runConcurrently(16, fileWatcher::start);
		}

		assertEquals(1, watcher.startCount());
	}

	@Test
	void concurrentCloseOnlyShutsDownWatcherOnce() throws Exception {
		RecordingWatcher watcher = new RecordingWatcher();
		try (FileWatcher fileWatcher = new FileWatcher(watcher)) {

			runConcurrently(16, fileWatcher::close);

			assertEquals(1, watcher.shutdownCount());
		}
	}

	private static FileWatcher createTestWatcher() {
		return new FileWatcher(new RecordingWatcher());
	}

	private static FileWatcher unavailableWatcher() {
		return new FileWatcher((FileWatcher.Watcher) null);
	}

	private static void runConcurrently(int taskCount, Runnable task) throws Exception {
		@SuppressWarnings("resource") ExecutorService executor = Executors.newFixedThreadPool(taskCount);
		try {
			CountDownLatch ready = new CountDownLatch(taskCount);
			CountDownLatch start = new CountDownLatch(1);
			List<Future<?>> futures = new ArrayList<>(taskCount);
			for (int i = 0; i < taskCount; i++) {
				futures.add(executor.submit(() -> {
					ready.countDown();
					start.await();
					task.run();
					return null;
				}));
			}

			assertTrue(ready.await(5, TimeUnit.SECONDS), "workers did not become ready");
			start.countDown();
			for (Future<?> future : futures) {
				future.get(5, TimeUnit.SECONDS);
			}
		} finally {
			executor.shutdownNow();
			assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "workers did not stop");
		}
	}

	private static class RecordingWatcher implements FileWatcher.Watcher {
		private final List<Path> paths = new ArrayList<>();
		private final List<Runnable> callbacks = new ArrayList<>();
		private final AtomicInteger startCount = new AtomicInteger();
		private final AtomicInteger shutdownCount = new AtomicInteger();

		@Override
		public void addCallback(Path path, Runnable callback) {
			paths.add(path);
			callbacks.add(callback);
		}

		@Override
		public void start() {
			startCount.incrementAndGet();
		}

		@Override
		public void shutdown() {
			shutdownCount.incrementAndGet();
		}

		int startCount() {
			return startCount.get();
		}

		int shutdownCount() {
			return shutdownCount.get();
		}
	}
}
