package net.mezzdev.filewatcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Watches individual files and runs callbacks when those files are created, modified, or deleted.
 */
public final class FileWatcher implements AutoCloseable {
	static final Duration DEFAULT_QUIET_TIME = Duration.ofMillis(500);
	static final Duration DEFAULT_DIRECTORY_RECHECK_INTERVAL = Duration.ofMinutes(1);

	private static final Logger LOGGER = LogManager.getLogger(FileWatcher.class);

	private final @Nullable Watcher watcher;
	private final AtomicBoolean started = new AtomicBoolean();
	private final AtomicBoolean closed = new AtomicBoolean();

	/**
	 * Creates a file watcher backed by the default filesystem {@link java.nio.file.WatchService}.
	 *
	 * @param threadName the name to use for the watcher thread
	 */
	public FileWatcher(String threadName) {
		this(Objects.requireNonNull(threadName, "threadName"), DEFAULT_QUIET_TIME, DEFAULT_DIRECTORY_RECHECK_INTERVAL);
	}

	FileWatcher(String threadName, Duration quietTime, Duration directoryRecheckInterval) {
		this(threadName, quietTime, directoryRecheckInterval, FileWatcherThread::new);
	}

	FileWatcher(
		String threadName,
		Duration quietTime,
		Duration directoryRecheckInterval,
		WatcherFactory watcherFactory
	) {
		this(createWatcher(
			Objects.requireNonNull(threadName, "threadName"),
			requirePositiveDuration(quietTime, "quietTime"),
			requirePositiveDuration(directoryRecheckInterval, "directoryRecheckInterval"),
			Objects.requireNonNull(watcherFactory, "watcherFactory")
		));
	}

	FileWatcher(@Nullable Watcher watcher) {
		this.watcher = watcher;
	}

	private static @Nullable Watcher createWatcher(
		String threadName,
		Duration quietTime,
		Duration directoryRecheckInterval,
		WatcherFactory watcherFactory
	) {
		try {
			return new ThreadWatcher(watcherFactory.create(threadName, quietTime, directoryRecheckInterval));
		} catch (UnsupportedOperationException | IOException e) {
			LOGGER.error("Unable to create file watcher: ", e);
			return null;
		}
	}

	private static Duration requirePositiveDuration(Duration duration, String name) {
		Objects.requireNonNull(duration, name);
		if (duration.toMillis() < 1) {
			throw new IllegalArgumentException(name + " must be at least 1 millisecond.");
		}
		return duration;
	}

	/**
	 * Adds or replaces the callback for a file.
	 *
	 * @param path     the file to watch
	 * @param callback the callback to call when the file changes.
	 *                 Callbacks must be thread-safe; they run from a callback runner thread.
	 */
	public void addCallback(Path path, Runnable callback) {
		Objects.requireNonNull(path, "path");
		Objects.requireNonNull(callback, "callback");
		if (watcher != null && !closed.get()) {
			watcher.addCallback(path, callback);
		}
	}

	/**
	 * Starts watching files. Calling this method more than once has no effect.
	 */
	public void start() {
		if (watcher != null && !closed.get() && started.compareAndSet(false, true)) {
			watcher.start();
		}
	}

	/**
	 * Stops the watcher thread. Calling this method more than once has no effect.
	 */
	@Override
	public void close() {
		if (watcher != null && closed.compareAndSet(false, true)) {
			watcher.shutdown();
		}
	}

	interface Watcher {
		void addCallback(Path path, Runnable callback);

		void start();

		void shutdown();
	}

	@FunctionalInterface
	interface WatcherFactory {
		FileWatcherThread create(String threadName, Duration quietTime, Duration directoryRecheckInterval) throws IOException;
	}

	private record ThreadWatcher(FileWatcherThread thread) implements Watcher {
		private ThreadWatcher {
			Objects.requireNonNull(thread, "thread");
		}

		@Override
		public void addCallback(Path path, Runnable callback) {
			thread.addCallback(path, callback);
		}

		@Override
		public void start() {
			thread.start();
		}

		@Override
		public void shutdown() {
			thread.shutdown();
		}
	}
}
