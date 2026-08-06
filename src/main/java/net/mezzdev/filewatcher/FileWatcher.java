package net.mezzdev.filewatcher;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;

/**
 * Watches individual files and runs callbacks when those files are created, modified, or deleted.
 */
public final class FileWatcher implements AutoCloseable {
	private static final Runnable NO_OP = () -> {};

	/**
	 * The default change-settling delay.
	 */
	public static final Duration DEFAULT_CHANGE_SETTLING_DELAY = Duration.ofMillis(500);
	/**
	 * The default missing-directory retry interval.
	 */
	public static final Duration DEFAULT_MISSING_DIRECTORY_RETRY_INTERVAL = Duration.ofMinutes(1);

	private final Watcher watcher;
	private boolean started;
	private boolean closed;

	/**
	 * Creates a file watcher backed by the default filesystem {@link java.nio.file.WatchService}.
	 *
	 * @param threadName the name to use for the watcher thread
	 * @throws FileWatcherUnavailableException when the default filesystem cannot create a usable watcher
	 * @see #FileWatcher(String, Duration, Duration)
	 */
	public FileWatcher(String threadName) throws FileWatcherUnavailableException {
		this(Objects.requireNonNull(threadName, "threadName"), DEFAULT_CHANGE_SETTLING_DELAY, DEFAULT_MISSING_DIRECTORY_RETRY_INTERVAL);
	}

	/**
	 * Creates a file watcher backed by the default filesystem {@link java.nio.file.WatchService}.
	 *
	 * @param threadName                    the name to use for the watcher thread
	 * @param changeSettlingDelay           the time to wait without receiving new events before running callbacks for
	 *                                      the changed files, coalescing bursts of filesystem events into one callback
	 *                                      run; values shorter than one millisecond are treated as one millisecond, and
	 *                                      values too large to fit in long milliseconds are treated as
	 *                                      {@link Long#MAX_VALUE} milliseconds (~292.3 million years)
	 * @param missingDirectoryRetryInterval the time between attempts to watch missing parent directories that do not
	 *                                      exist yet; values shorter than one millisecond are treated as one
	 *                                      millisecond, and values too large to fit in long milliseconds are treated as
	 *                                      {@link Long#MAX_VALUE} milliseconds (~292.3 million years)
	 * @throws FileWatcherUnavailableException when the default filesystem cannot create a usable watcher
	 */
	public FileWatcher(String threadName, Duration changeSettlingDelay, Duration missingDirectoryRetryInterval) throws FileWatcherUnavailableException {
		this(threadName, changeSettlingDelay, missingDirectoryRetryInterval, FileWatcherThread::new);
	}

	FileWatcher(
		String threadName,
		Duration changeSettlingDelay,
		Duration missingDirectoryRetryInterval,
		WatcherFactory watcherFactory
	) throws FileWatcherUnavailableException {
		this(createWatcher(
			Objects.requireNonNull(threadName, "threadName"),
			clampDurationToMillis(changeSettlingDelay, "changeSettlingDelay"),
			clampDurationToMillis(missingDirectoryRetryInterval, "missingDirectoryRetryInterval"),
			Objects.requireNonNull(watcherFactory, "watcherFactory")
		));
	}

	FileWatcher(Watcher watcher) {
		this.watcher = Objects.requireNonNull(watcher, "watcher");
	}

	private static Watcher createWatcher(
		String threadName,
		Duration changeSettlingDelay,
		Duration missingDirectoryRetryInterval,
		WatcherFactory watcherFactory
	) throws FileWatcherUnavailableException {
		try {
			return createThreadWatcher(threadName, changeSettlingDelay, missingDirectoryRetryInterval, watcherFactory);
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
		Duration changeSettlingDelay,
		Duration missingDirectoryRetryInterval,
		WatcherFactory watcherFactory
	) throws IOException {
		return new ThreadWatcher(watcherFactory.create(threadName, changeSettlingDelay, missingDirectoryRetryInterval));
	}

	private static Duration clampDurationToMillis(Duration duration, String name) {
		Objects.requireNonNull(duration, name);
		if (duration.isNegative() || duration.isZero()) {
			return Duration.ofMillis(1);
		}
		try {
			return Duration.ofMillis(Math.max(1, duration.toMillis()));
		} catch (ArithmeticException e) {
			return Duration.ofMillis(Long.MAX_VALUE);
		}
	}

	/**
	 * Adds a callback for a file. Multiple callbacks may be registered for the same file.
	 *
	 * @param path     the file to watch
	 * @param callback the callback to call when the file changes.
	 *                 Callbacks must be thread-safe; they run from a callback runner thread.
	 * @return an idempotent callback that removes only this registration. It has no effect if this registration has
	 * already been removed.
	 */
	public synchronized Runnable addCallback(Path path, Runnable callback) {
		Objects.requireNonNull(path, "path");
		Objects.requireNonNull(callback, "callback");
		if (closed) {
			return NO_OP;
		}
		return watcher.addCallback(path, callback);
	}

	/**
	 * Starts watching files. Calling this method more than once has no effect.
	 */
	public synchronized void start() {
		if (!closed && !started) {
			started = true;
			watcher.start();
		}
	}

	/**
	 * Stops the watcher thread. Calling this method more than once has no effect.
	 */
	@Override
	public synchronized void close() {
		if (!closed) {
			closed = true;
			watcher.shutdown();
		}
	}

	interface Watcher {
		Runnable addCallback(Path path, Runnable callback);

		void start();

		void shutdown();
	}

	@FunctionalInterface
	interface WatcherFactory {
		FileWatcherThread create(String threadName, Duration changeSettlingDelay, Duration missingDirectoryRetryInterval) throws IOException;
	}

	private record ThreadWatcher(FileWatcherThread thread) implements Watcher {
		private ThreadWatcher {
			Objects.requireNonNull(thread, "thread");
		}

		@Override
		public Runnable addCallback(Path path, Runnable callback) {
			return thread.addCallback(path, callback);
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
