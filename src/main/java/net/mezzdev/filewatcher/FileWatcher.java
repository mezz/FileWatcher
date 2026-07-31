package net.mezzdev.filewatcher;

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

	private final Watcher watcher;
	private final AtomicBoolean started = new AtomicBoolean();
	private final AtomicBoolean closed = new AtomicBoolean();

	/**
	 * Creates a file watcher backed by the default filesystem {@link java.nio.file.WatchService}.
	 *
	 * @param threadName the name to use for the watcher thread
	 * @throws FileWatcherUnavailableException when the default filesystem cannot create a usable watcher
	 */
	public FileWatcher(String threadName) throws FileWatcherUnavailableException {
		this(Objects.requireNonNull(threadName, "threadName"), DEFAULT_QUIET_TIME, DEFAULT_DIRECTORY_RECHECK_INTERVAL);
	}

	FileWatcher(String threadName, Duration quietTime, Duration directoryRecheckInterval) throws FileWatcherUnavailableException {
		this(threadName, quietTime, directoryRecheckInterval, FileWatcherThread::new);
	}

	FileWatcher(
		String threadName,
		Duration quietTime,
		Duration directoryRecheckInterval,
		WatcherFactory watcherFactory
	) throws FileWatcherUnavailableException {
		this(createWatcher(
			Objects.requireNonNull(threadName, "threadName"),
			requirePositiveDuration(quietTime, "quietTime"),
			requirePositiveDuration(directoryRecheckInterval, "directoryRecheckInterval"),
			Objects.requireNonNull(watcherFactory, "watcherFactory")
		));
	}

	FileWatcher(Watcher watcher) {
		this.watcher = Objects.requireNonNull(watcher, "watcher");
	}

	private static Watcher createWatcher(
		String threadName,
		Duration quietTime,
		Duration directoryRecheckInterval,
		WatcherFactory watcherFactory
	) throws FileWatcherUnavailableException {
		try {
			return createThreadWatcher(threadName, quietTime, directoryRecheckInterval, watcherFactory);
		} catch (UnsupportedOperationException | IOException e) {
			throw new FileWatcherUnavailableException(
				"Unable to create a file watcher for the default filesystem. " +
					"Handle FileWatcherUnavailableException to support platforms without a usable file watcher.",
				e
			);
		}
	}

	private static Watcher createThreadWatcher(
		String threadName,
		Duration quietTime,
		Duration directoryRecheckInterval,
		WatcherFactory watcherFactory
	) throws IOException {
		return new ThreadWatcher(watcherFactory.create(threadName, quietTime, directoryRecheckInterval));
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
		if (!closed.get()) {
			watcher.addCallback(path, callback);
		}
	}

	/**
	 * Starts watching files. Calling this method more than once has no effect.
	 */
	public void start() {
		if (!closed.get() && started.compareAndSet(false, true)) {
			watcher.start();
		}
	}

	/**
	 * Stops the watcher thread. Calling this method more than once has no effect.
	 */
	@Override
	public void close() {
		if (closed.compareAndSet(false, true)) {
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
