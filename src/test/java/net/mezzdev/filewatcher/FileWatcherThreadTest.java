package net.mezzdev.filewatcher;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.Watchable;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileWatcherThreadTest {
	private static final Duration QUIET_TIME = Duration.ofMillis(10);
	private static final Duration DIRECTORY_RECHECK_INTERVAL = Duration.ofMillis(100);

	@TempDir
	Path tempDir;

	@SuppressWarnings({"DataFlowIssue"})
	@Test
	void rejectsInvalidConstructorArguments() {
		Fixture fixture = new Fixture();

		assertThrows(NullPointerException.class, () -> createThread(fixture, null, QUIET_TIME, DIRECTORY_RECHECK_INTERVAL));
		assertThrows(NullPointerException.class, () -> createThread(fixture, "test", null, DIRECTORY_RECHECK_INTERVAL));
		assertThrows(NullPointerException.class, () -> createThread(fixture, "test", QUIET_TIME, null));
		assertThrows(NullPointerException.class, () -> new FileWatcherThread(
			"test",
			QUIET_TIME,
			DIRECTORY_RECHECK_INTERVAL,
			null,
			fixture::isDirectory,
			fixture::register,
			fixture.clock::currentTimeMillis,
			fixture::executeCallbacks
		));
		assertThrows(NullPointerException.class, () -> new FileWatcherThread(
			"test",
			QUIET_TIME,
			DIRECTORY_RECHECK_INTERVAL,
			fixture.watchService,
			fixture::isDirectory,
			fixture::register,
			fixture.clock::currentTimeMillis,
			fixture::executeCallbacks,
			null
		));
		assertThrows(IllegalArgumentException.class, () -> createThread(
			fixture,
			"test",
			Duration.ZERO,
			DIRECTORY_RECHECK_INTERVAL
		));
		assertThrows(IllegalArgumentException.class, () -> createThread(
			fixture,
			"test",
			Duration.ofNanos(1),
			DIRECTORY_RECHECK_INTERVAL
		));
		assertThrows(IllegalArgumentException.class, () -> createThread(
			fixture,
			"test",
			QUIET_TIME,
			Duration.ZERO
		));
	}

	@Test
	void rejectsWatchedPathWithoutParent() {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		Path root = Objects.requireNonNull(tempDir.toAbsolutePath().getRoot());

		assertThrows(IllegalArgumentException.class, () -> thread.addCallback(root, () -> {}));
	}

	@Test
	void registersExistingDirectoryOnceAndPollsWithQuietTime() throws Exception {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		Path watchedFile = tempDir.resolve("config.toml");

		fixture.markDirectoryExists(tempDir);
		thread.addCallback(watchedFile, () -> {});

		thread.runIteration();
		thread.runIteration();

		assertEquals(1, fixture.registrationCount(tempDir));
		assertEquals(QUIET_TIME.toMillis(), fixture.watchService.lastPollTimeout);
		assertEquals(TimeUnit.MILLISECONDS, fixture.watchService.lastPollUnit);
	}

	@Test
	void defaultWatchServiceRegistersExistingDirectory() throws Exception {
		Path watchedFile = tempDir.resolve("config.toml");
		FileWatcherThread thread = new FileWatcherThread(
			"FileWatcherThread default watch service test",
			QUIET_TIME,
			DIRECTORY_RECHECK_INTERVAL
		);
		try {
			thread.addCallback(watchedFile, () -> {});

			thread.runIteration();
		} finally {
			thread.shutdown();
		}
	}

	@Test
	void doesNotRegisterParentDirectoryForMissingDirectory() throws Exception {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		Path watchedDirectory = tempDir.resolve("new-directory");
		Path watchedFile = watchedDirectory.resolve("config.toml");

		fixture.markDirectoryExists(tempDir);
		thread.addCallback(watchedFile, () -> {});
		thread.runIteration();

		fixture.clock.advance(DIRECTORY_RECHECK_INTERVAL);
		thread.runIteration();

		fixture.clock.advance(DIRECTORY_RECHECK_INTERVAL);
		thread.runIteration();

		assertEquals(0, fixture.registrationCount(tempDir));
		assertEquals(0, fixture.registrationCount(watchedDirectory));
	}

	@Test
	void directoryRecheckDetectsWatchedFileCreatedBeforeDirectoryRegistration() throws Exception {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		Path watchedDirectory = tempDir.resolve("new-directory");
		Path watchedFile = watchedDirectory.resolve("config.toml");
		AtomicInteger callbackCount = new AtomicInteger();

		thread.addCallback(watchedFile, callbackCount::incrementAndGet);
		thread.runIteration();

		Files.createDirectory(watchedDirectory);
		Files.writeString(watchedFile, "created");
		fixture.markDirectoryExists(watchedDirectory);
		fixture.clock.advance(DIRECTORY_RECHECK_INTERVAL);
		thread.runIteration();

		assertEquals(1, fixture.registrationCount(watchedDirectory));
		assertEquals(1, callbackCount.get());
		assertEquals(1, fixture.callbackBatches.size());
	}

	@Test
	void doesNotRegisterAlreadyWatchedDirectoryAgainWhenRecheckTimeArrives() throws Exception {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		Path watchedFile = tempDir.resolve("config.toml");

		fixture.markDirectoryExists(tempDir);
		thread.addCallback(watchedFile, () -> {});
		thread.runIteration();

		fixture.clock.advance(DIRECTORY_RECHECK_INTERVAL);
		thread.runIteration();

		assertEquals(1, fixture.registrationCount(tempDir));
	}

	@Test
	void registersDirectoryAfterItAppearsAndRecheckTimeArrives() throws Exception {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		Path watchedDirectory = tempDir.resolve("new-directory");
		Path watchedFile = watchedDirectory.resolve("config.toml");

		thread.addCallback(watchedFile, () -> {});

		thread.runIteration();
		fixture.markDirectoryExists(watchedDirectory);
		fixture.clock.advance(DIRECTORY_RECHECK_INTERVAL.minusMillis(1));
		thread.runIteration();
		assertEquals(0, fixture.registrationCount(watchedDirectory));

		fixture.clock.advance(Duration.ofMillis(1));
		thread.runIteration();
		assertEquals(1, fixture.registrationCount(watchedDirectory));
	}

	@Test
	void defersCallbacksUntilQuietPoll() throws Exception {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		Path watchedFile = tempDir.resolve("config.toml");
		AtomicInteger callbackCount = new AtomicInteger();

		fixture.markDirectoryExists(tempDir);
		thread.addCallback(watchedFile, callbackCount::incrementAndGet);
		thread.runIteration();

		FakeWatchKey key = fixture.latestKey(tempDir);
		key.addEvent(pathEvent(StandardWatchEventKinds.ENTRY_MODIFY, watchedFile.getFileName()));
		fixture.watchService.enqueue(key);
		thread.runIteration();

		assertEquals(0, callbackCount.get());
		assertEquals(0, fixture.callbackBatches.size());
		assertEquals(1, key.resetCount);

		thread.runIteration();

		assertEquals(1, callbackCount.get());
		assertEquals(1, fixture.callbackBatches.size());
		assertEquals(1, fixture.callbackBatches.get(0).size());
	}

	@Test
	void coalescesDuplicateEventsForOneFile() throws Exception {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		Path watchedFile = tempDir.resolve("config.toml");
		AtomicInteger callbackCount = new AtomicInteger();

		fixture.markDirectoryExists(tempDir);
		thread.addCallback(watchedFile, callbackCount::incrementAndGet);
		thread.runIteration();

		FakeWatchKey key = fixture.latestKey(tempDir);
		key.addEvent(pathEvent(StandardWatchEventKinds.ENTRY_MODIFY, watchedFile.getFileName()));
		key.addEvent(pathEvent(StandardWatchEventKinds.ENTRY_MODIFY, watchedFile.getFileName()));
		fixture.watchService.enqueue(key);
		thread.runIteration();
		thread.runIteration();

		assertEquals(1, callbackCount.get());
		assertEquals(1, fixture.callbackBatches.size());
		assertEquals(1, fixture.callbackBatches.get(0).size());
	}

	@Test
	void batchesCallbacksForDifferentWatchedFiles() throws Exception {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		Path firstFile = tempDir.resolve("first.toml");
		Path secondFile = tempDir.resolve("second.toml");
		AtomicInteger firstCallbackCount = new AtomicInteger();
		AtomicInteger secondCallbackCount = new AtomicInteger();

		fixture.markDirectoryExists(tempDir);
		thread.addCallback(firstFile, firstCallbackCount::incrementAndGet);
		thread.addCallback(secondFile, secondCallbackCount::incrementAndGet);
		thread.runIteration();

		FakeWatchKey key = fixture.latestKey(tempDir);
		key.addEvent(pathEvent(StandardWatchEventKinds.ENTRY_MODIFY, firstFile.getFileName()));
		key.addEvent(pathEvent(StandardWatchEventKinds.ENTRY_MODIFY, secondFile.getFileName()));
		fixture.watchService.enqueue(key);
		thread.runIteration();
		thread.runIteration();

		assertEquals(1, firstCallbackCount.get());
		assertEquals(1, secondCallbackCount.get());
		assertEquals(1, fixture.callbackBatches.size());
		assertEquals(2, fixture.callbackBatches.get(0).size());
	}

	@Test
	void ignoresEventsForUnwatchedFiles() throws Exception {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		Path watchedFile = tempDir.resolve("watched.toml");
		Path otherFile = tempDir.resolve("other.toml");
		AtomicInteger callbackCount = new AtomicInteger();

		fixture.markDirectoryExists(tempDir);
		thread.addCallback(watchedFile, callbackCount::incrementAndGet);
		thread.runIteration();

		FakeWatchKey key = fixture.latestKey(tempDir);
		key.addEvent(pathEvent(StandardWatchEventKinds.ENTRY_MODIFY, otherFile.getFileName()));
		fixture.watchService.enqueue(key);
		thread.runIteration();
		thread.runIteration();

		assertEquals(0, callbackCount.get());
		assertEquals(0, fixture.callbackBatches.size());
	}

	@Test
	void ignoresEventsWithNonPathContext() throws Exception {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		Path watchedFile = tempDir.resolve("watched.toml");
		AtomicInteger callbackCount = new AtomicInteger();

		fixture.markDirectoryExists(tempDir);
		thread.addCallback(watchedFile, callbackCount::incrementAndGet);
		thread.runIteration();

		FakeWatchKey key = fixture.latestKey(tempDir);
		key.addEvent(nonPathContextEvent());
		fixture.watchService.enqueue(key);
		thread.runIteration();
		thread.runIteration();

		assertEquals(0, callbackCount.get());
		assertEquals(0, fixture.callbackBatches.size());
	}

	@Test
	void ignoresWatchKeysForUnknownDirectories() throws Exception {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		FakeWatchKey key = new FakeWatchKey(tempDir);
		key.addEvent(pathEvent(StandardWatchEventKinds.ENTRY_MODIFY, Path.of("config.toml")));

		fixture.watchService.enqueue(key);
		thread.runIteration();

		assertEquals(0, key.resetCount);
	}

	@Test
	void deleteEventMarksWatchedFileChanged() throws Exception {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		Path watchedFile = tempDir.resolve("config.toml");
		AtomicInteger callbackCount = new AtomicInteger();

		fixture.markDirectoryExists(tempDir);
		thread.addCallback(watchedFile, callbackCount::incrementAndGet);
		thread.runIteration();

		FakeWatchKey key = fixture.latestKey(tempDir);
		key.addEvent(pathEvent(StandardWatchEventKinds.ENTRY_DELETE, watchedFile.getFileName()));
		fixture.watchService.enqueue(key);
		thread.runIteration();
		thread.runIteration();

		assertEquals(1, callbackCount.get());
		assertEquals(1, fixture.callbackBatches.size());
		assertEquals(1, fixture.callbackBatches.get(0).size());
	}

	@Test
	void overflowMarksChangedWatchedFilesInTheAffectedDirectory() throws Exception {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		Path firstFile = tempDir.resolve("first.toml");
		Path secondFile = tempDir.resolve("second.toml");
		Path otherDirectory = tempDir.resolve("other");
		Path otherDirectoryFile = otherDirectory.resolve("other.toml");
		AtomicInteger firstCallbackCount = new AtomicInteger();
		AtomicInteger secondCallbackCount = new AtomicInteger();
		AtomicInteger otherDirectoryCallbackCount = new AtomicInteger();

		Files.writeString(firstFile, "first");
		Files.writeString(secondFile, "second");
		Files.createDirectory(otherDirectory);
		Files.writeString(otherDirectoryFile, "other");
		fixture.markDirectoryExists(tempDir);
		fixture.markDirectoryExists(otherDirectory);
		thread.addCallback(firstFile, firstCallbackCount::incrementAndGet);
		thread.addCallback(secondFile, secondCallbackCount::incrementAndGet);
		thread.addCallback(otherDirectoryFile, otherDirectoryCallbackCount::incrementAndGet);
		thread.runIteration();

		Files.writeString(firstFile, "first changed");
		Files.writeString(secondFile, "second changed");

		FakeWatchKey key = fixture.latestKey(tempDir);
		key.addEvent(overflowEvent());
		fixture.watchService.enqueue(key);
		thread.runIteration();
		thread.runIteration();

		assertEquals(1, firstCallbackCount.get());
		assertEquals(1, secondCallbackCount.get());
		assertEquals(0, otherDirectoryCallbackCount.get());
		assertEquals(1, fixture.callbackBatches.size());
		assertEquals(2, fixture.callbackBatches.get(0).size());

		key.addEvent(overflowEvent());
		fixture.watchService.enqueue(key);
		thread.runIteration();
		thread.runIteration();

		assertEquals(1, firstCallbackCount.get());
		assertEquals(1, secondCallbackCount.get());
		assertEquals(0, otherDirectoryCallbackCount.get());
		assertEquals(1, fixture.callbackBatches.size());
	}

	@Test
	void overflowSkipsUnchangedWatchedFiles() throws Exception {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		Path watchedFile = tempDir.resolve("config.toml");
		AtomicInteger callbackCount = new AtomicInteger();

		Files.writeString(watchedFile, "initial");
		fixture.markDirectoryExists(tempDir);
		thread.addCallback(watchedFile, callbackCount::incrementAndGet);
		thread.runIteration();

		FakeWatchKey key = fixture.latestKey(tempDir);
		key.addEvent(overflowEvent());
		fixture.watchService.enqueue(key);
		thread.runIteration();
		thread.runIteration();

		assertEquals(0, callbackCount.get());
		assertEquals(0, fixture.callbackBatches.size());
	}

	@Test
	void overflowDetectsCreatedAndDeletedWatchedFiles() throws Exception {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		Path createdFile = tempDir.resolve("created.toml");
		Path deletedFile = tempDir.resolve("deleted.toml");
		AtomicInteger createdCallbackCount = new AtomicInteger();
		AtomicInteger deletedCallbackCount = new AtomicInteger();

		Files.writeString(deletedFile, "initial");
		fixture.markDirectoryExists(tempDir);
		thread.addCallback(createdFile, createdCallbackCount::incrementAndGet);
		thread.addCallback(deletedFile, deletedCallbackCount::incrementAndGet);
		thread.runIteration();

		Files.writeString(createdFile, "created");
		Files.delete(deletedFile);

		FakeWatchKey key = fixture.latestKey(tempDir);
		key.addEvent(overflowEvent());
		fixture.watchService.enqueue(key);
		thread.runIteration();
		thread.runIteration();

		assertEquals(1, createdCallbackCount.get());
		assertEquals(1, deletedCallbackCount.get());
		assertEquals(1, fixture.callbackBatches.size());
		assertEquals(2, fixture.callbackBatches.get(0).size());
	}

	@Test
	void normalEventsUpdateSnapshotsForLaterOverflowChecks() throws Exception {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		Path watchedFile = tempDir.resolve("config.toml");
		AtomicInteger callbackCount = new AtomicInteger();

		Files.writeString(watchedFile, "initial");
		fixture.markDirectoryExists(tempDir);
		thread.addCallback(watchedFile, callbackCount::incrementAndGet);
		thread.runIteration();

		Files.writeString(watchedFile, "modified");
		FakeWatchKey key = fixture.latestKey(tempDir);
		key.addEvent(pathEvent(StandardWatchEventKinds.ENTRY_MODIFY, watchedFile.getFileName()));
		fixture.watchService.enqueue(key);
		thread.runIteration();
		thread.runIteration();

		key.addEvent(overflowEvent());
		fixture.watchService.enqueue(key);
		thread.runIteration();
		thread.runIteration();

		assertEquals(1, callbackCount.get());
		assertEquals(1, fixture.callbackBatches.size());
	}

	@Test
	void overflowTreatsUnreadableSnapshotsAsChanged() throws Exception {
		Fixture fixture = new Fixture();
		AtomicBoolean metadataUnavailable = new AtomicBoolean();
		FileWatcherThread thread = createThread(
			fixture,
			path -> {
				if (metadataUnavailable.get()) {
					throw new IOException("metadata unavailable");
				}
				return Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
			}
		);
		Path watchedFile = tempDir.resolve("config.toml");
		AtomicInteger callbackCount = new AtomicInteger();

		Files.writeString(watchedFile, "initial");
		fixture.markDirectoryExists(tempDir);
		thread.addCallback(watchedFile, callbackCount::incrementAndGet);
		thread.runIteration();
		metadataUnavailable.set(true);

		FakeWatchKey key = fixture.latestKey(tempDir);
		key.addEvent(overflowEvent());
		fixture.watchService.enqueue(key);
		thread.runIteration();
		thread.runIteration();

		assertEquals(1, callbackCount.get());
		assertEquals(1, fixture.callbackBatches.size());
	}

	@Test
	void resetFailureAllowsDirectoryToBeRegisteredAgain() throws Exception {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		Path watchedFile = tempDir.resolve("config.toml");
		AtomicInteger callbackCount = new AtomicInteger();

		fixture.markDirectoryExists(tempDir);
		thread.addCallback(watchedFile, callbackCount::incrementAndGet);
		thread.runIteration();

		FakeWatchKey key = fixture.latestKey(tempDir);
		key.resetResult = false;
		key.addEvent(pathEvent(StandardWatchEventKinds.ENTRY_MODIFY, watchedFile.getFileName()));
		fixture.watchService.enqueue(key);
		thread.runIteration();

		fixture.clock.advance(DIRECTORY_RECHECK_INTERVAL);
		thread.runIteration();

		assertEquals(2, fixture.registrationCount(tempDir));
		assertEquals(1, callbackCount.get());
		assertFalse(key.isValid());
	}

	@Test
	void interruptedEventPollingThrowsInterruptedException() throws Exception {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		Path watchedFile = tempDir.resolve("config.toml");

		fixture.markDirectoryExists(tempDir);
		thread.addCallback(watchedFile, () -> {});
		thread.runIteration();

		FakeWatchKey key = fixture.latestKey(tempDir);
		key.addEvent(pathEvent(StandardWatchEventKinds.ENTRY_MODIFY, watchedFile.getFileName()));
		fixture.watchService.enqueue(key);

		Thread.currentThread().interrupt();
		try {
			assertThrows(InterruptedException.class, thread::runIteration);
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	void interruptedDirectoryWatchDoesNotRegisterDirectories() throws Exception {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		Path watchedFile = tempDir.resolve("config.toml");

		fixture.markDirectoryExists(tempDir);
		thread.addCallback(watchedFile, () -> {});

		Thread.currentThread().interrupt();
		try {
			thread.runIteration();
		} finally {
			Thread.interrupted();
		}

		assertEquals(0, fixture.registrationCount(tempDir));
	}

	@Test
	void registrationFailureAllowsFutureRegistrationAttempt() throws Exception {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		Path watchedFile = tempDir.resolve("config.toml");

		fixture.markDirectoryExists(tempDir);
		fixture.registrationException = new IOException("registration failed");
		thread.addCallback(watchedFile, () -> {});
		thread.runIteration();
		assertEquals(0, fixture.registrationCount(tempDir));

		fixture.registrationException = null;
		fixture.clock.advance(DIRECTORY_RECHECK_INTERVAL);
		thread.runIteration();

		assertEquals(1, fixture.registrationCount(tempDir));
	}

	@Test
	void shutdownInterruptsThreadAndClosesWatchService() {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);

		thread.shutdown();

		assertTrue(thread.isInterrupted());
		assertTrue(fixture.watchService.closed);
		assertEquals(1, fixture.watchService.closeCount);
	}

	@Test
	void shutdownIgnoresWatchServiceCloseFailure() {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		fixture.watchService.closeException = new IOException("close failed");

		thread.shutdown();

		assertTrue(thread.isInterrupted());
		assertTrue(fixture.watchService.closed);
		assertEquals(1, fixture.watchService.closeCount);
	}

	@Test
	void runStopsWhenWatchServiceClosesAndCancelsWatchedDirectories() {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		Path watchedFile = tempDir.resolve("config.toml");

		fixture.markDirectoryExists(tempDir);
		thread.addCallback(watchedFile, () -> {});
		assertDoesNotThrowInterrupted(thread::runIteration);
		FakeWatchKey key = fixture.latestKey(tempDir);

		fixture.watchService.pollClosed = true;
		thread.run();

		assertFalse(key.isValid());
		assertTrue(fixture.watchService.closed);
		assertEquals(1, fixture.watchService.closeCount);
	}

	@Test
	void runStopsWhenInterruptedAndPreservesInterruptStatus() {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		fixture.watchService.pollInterrupted = true;

		try {
			thread.run();

			assertTrue(Thread.currentThread().isInterrupted());
			assertTrue(fixture.watchService.closed);
			assertEquals(1, fixture.watchService.closeCount);
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	void runHandlesWatchServiceCloseFailure() {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		fixture.watchService.closeException = new IOException("close failed");

		Thread.currentThread().interrupt();
		try {
			thread.run();

			assertTrue(fixture.watchService.closed);
			assertEquals(1, fixture.watchService.closeCount);
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	void runStopsAfterCurrentThreadIsInterruptedDuringIteration() {
		Fixture fixture = new Fixture();
		FileWatcherThread thread = createThread(fixture);
		fixture.watchService.interruptAfterPoll = true;

		try {
			thread.run();

			assertTrue(fixture.watchService.closed);
			assertEquals(1, fixture.watchService.closeCount);
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	void threadedCallbackExecutorRunsCallbacksOnNonDaemonThreadAndContinuesAfterFailure() throws Exception {
		FileWatcherThread.CallbackExecutor callbackExecutor = threadedCallbackExecutor();
		CountDownLatch latch = new CountDownLatch(1);
		AtomicBoolean callbackThreadWasDaemon = new AtomicBoolean(true);
		AtomicInteger callbackCount = new AtomicInteger();

		callbackExecutor.execute(List.of(
			() -> {
				callbackCount.incrementAndGet();
				throw new IllegalStateException("callback failed");
			},
			() -> {
				callbackThreadWasDaemon.set(Thread.currentThread().isDaemon());
				callbackCount.incrementAndGet();
				latch.countDown();
			}
		));

		assertTrue(latch.await(5, TimeUnit.SECONDS), "callbacks did not run");
		assertFalse(callbackThreadWasDaemon.get(), "callbacks should run on a non-daemon thread");
		assertEquals(2, callbackCount.get());
	}

	private static FileWatcherThread createThread(Fixture fixture) {
		return createThread(fixture, "FileWatcherThread test", QUIET_TIME, DIRECTORY_RECHECK_INTERVAL);
	}

	private static FileWatcherThread createThread(
		Fixture fixture,
		FileWatcherThread.FileAttributesReader fileAttributesReader
	) {
		return new FileWatcherThread(
			"FileWatcherThread test",
			QUIET_TIME,
			DIRECTORY_RECHECK_INTERVAL,
			fixture.watchService,
			fixture::isDirectory,
			fixture::register,
			fixture.clock::currentTimeMillis,
			fixture::executeCallbacks,
			fileAttributesReader
		);
	}

	private static FileWatcherThread createThread(
		Fixture fixture,
		String name,
		Duration quietTime,
		Duration directoryRecheckInterval
	) {
		return new FileWatcherThread(
			name,
			quietTime,
			directoryRecheckInterval,
			fixture.watchService,
			fixture::isDirectory,
			fixture::register,
			fixture.clock::currentTimeMillis,
			fixture::executeCallbacks
		);
	}

	private static void assertDoesNotThrowInterrupted(ThrowingRunnable runnable) {
		try {
			runnable.run();
		} catch (InterruptedException e) {
			throw new AssertionError("Unexpected interruption.", e);
		}
	}

	private static FileWatcherThread.CallbackExecutor threadedCallbackExecutor() throws Exception {
		Class<?> executorClass = Class.forName(FileWatcherThread.class.getName() + "$ThreadedCallbackExecutor");
		Field instanceField = executorClass.getDeclaredField("INSTANCE");
		instanceField.setAccessible(true);
		return (FileWatcherThread.CallbackExecutor) instanceField.get(null);
	}

	private static WatchEvent<Path> pathEvent(WatchEvent.Kind<Path> kind, Path context) {
		return new WatchEvent<>() {
			@Override
			public WatchEvent.Kind<Path> kind() {
				return kind;
			}

			@Override
			public int count() {
				return 1;
			}

			@Override
			public Path context() {
				return context;
			}
		};
	}

	private static WatchEvent<?> overflowEvent() {
		return new WatchEvent<>() {
			@Override
			public WatchEvent.Kind<Object> kind() {
				return StandardWatchEventKinds.OVERFLOW;
			}

			@Override
			public int count() {
				return 1;
			}

			@Override
			public @Nullable Object context() {
				return null;
			}
		};
	}

	private static WatchEvent<?> nonPathContextEvent() {
		return new WatchEvent<>() {
			@SuppressWarnings({"rawtypes", "unchecked"})
			@Override
			public WatchEvent.Kind<Object> kind() {
				return (WatchEvent.Kind) StandardWatchEventKinds.ENTRY_MODIFY;
			}

			@Override
			public int count() {
				return 1;
			}

			@Override
			public String context() {
				return "config.toml";
			}
		};
	}

	private static Path normalize(Path path) {
		return path.toAbsolutePath()
			.normalize();
	}

	private static class Fixture {
		private final FakeWatchService watchService = new FakeWatchService();
		private final MutableClock clock = new MutableClock(1_000);
		private final Set<Path> existingDirectories = new HashSet<>();
		private final Map<Path, List<FakeWatchKey>> registrations = new HashMap<>();
		private final List<List<Runnable>> callbackBatches = new ArrayList<>();
		private @Nullable IOException registrationException;

		void markDirectoryExists(Path directory) {
			existingDirectories.add(normalize(directory));
		}

		boolean isDirectory(Path directory) {
			return existingDirectories.contains(directory);
		}

		FakeWatchKey register(Path directory, WatchService watchService) throws IOException {
			assertSame(this.watchService, watchService);
			if (registrationException != null) {
				throw registrationException;
			}
			FakeWatchKey key = new FakeWatchKey(directory);
			registrations.computeIfAbsent(directory, ignored -> new ArrayList<>())
				.add(key);
			return key;
		}

		void executeCallbacks(List<Runnable> runnables) {
			callbackBatches.add(List.copyOf(runnables));
			runnables.forEach(Runnable::run);
		}

		int registrationCount(Path directory) {
			return registrations.getOrDefault(normalize(directory), List.of())
				.size();
		}

		FakeWatchKey latestKey(Path directory) {
			List<FakeWatchKey> keys = registrations.getOrDefault(normalize(directory), List.of());
			if (keys.isEmpty()) {
				throw new AssertionError("No watch key registered for " + directory);
			}
			return keys.get(keys.size() - 1);
		}
	}

	private static class MutableClock {
		private long currentTimeMillis;

		MutableClock(long currentTimeMillis) {
			this.currentTimeMillis = currentTimeMillis;
		}

		long currentTimeMillis() {
			return currentTimeMillis;
		}

		void advance(Duration duration) {
			currentTimeMillis += duration.toMillis();
		}
	}

	private static class FakeWatchService implements WatchService {
		private final ArrayDeque<WatchKey> keys = new ArrayDeque<>();
		private long lastPollTimeout;
		private @Nullable TimeUnit lastPollUnit;
		private boolean closed;
		private boolean pollClosed;
		private boolean pollInterrupted;
		private boolean interruptAfterPoll;
		private @Nullable IOException closeException;
		private int closeCount;

		void enqueue(WatchKey key) {
			keys.add(key);
		}

		@Override
		public @Nullable WatchKey poll() {
			return keys.poll();
		}

		@Override
		public @Nullable WatchKey poll(long timeout, TimeUnit unit) throws InterruptedException {
			lastPollTimeout = timeout;
			lastPollUnit = unit;
			if (pollClosed) {
				throw new ClosedWatchServiceException();
			}
			if (pollInterrupted) {
				throw new InterruptedException("poll interrupted");
			}
			if (interruptAfterPoll) {
				Thread.currentThread().interrupt();
			}
			return keys.poll();
		}

		@Override
		public WatchKey take() throws InterruptedException {
			WatchKey key = keys.poll();
			if (key != null) {
				return key;
			}
			throw new InterruptedException("No fake watch key queued.");
		}

		@Override
		public void close() throws IOException {
			closeCount++;
			closed = true;
			if (closeException != null) {
				throw closeException;
			}
		}
	}

	@FunctionalInterface
	private interface ThrowingRunnable {
		void run() throws InterruptedException;
	}

	private static class FakeWatchKey implements WatchKey {
		private final Watchable watchable;
		private final List<WatchEvent<?>> events = new ArrayList<>();
		private boolean valid = true;
		private boolean resetResult = true;
		private int resetCount;

		FakeWatchKey(Watchable watchable) {
			this.watchable = watchable;
		}

		void addEvent(WatchEvent<?> event) {
			events.add(event);
		}

		@Override
		public boolean isValid() {
			return valid;
		}

		@Override
		public List<WatchEvent<?>> pollEvents() {
			List<WatchEvent<?>> result = List.copyOf(events);
			events.clear();
			return result;
		}

		@Override
		public boolean reset() {
			resetCount++;
			valid = resetResult;
			return resetResult;
		}

		@Override
		public void cancel() {
			valid = false;
		}

		@Override
		public Watchable watchable() {
			return watchable;
		}
	}
}
