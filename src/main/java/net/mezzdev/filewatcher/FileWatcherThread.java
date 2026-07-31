package net.mezzdev.filewatcher;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Thread implementation used by {@link FileWatcher}.
 */
final class FileWatcherThread extends Thread {
	private static final Logger LOGGER = LogManager.getLogger(FileWatcherThread.class);
	private static final WatchEvent.Kind<?>[] WATCH_EVENT_KINDS = {
		StandardWatchEventKinds.ENTRY_DELETE,
		StandardWatchEventKinds.ENTRY_CREATE,
		StandardWatchEventKinds.ENTRY_MODIFY,
		StandardWatchEventKinds.OVERFLOW
	};

	private final WatchService watchService;
	private final Map<Path, Runnable> callbacks;
	private final Set<Path> directoriesToWatch;
	private final Predicate<Path> isDirectory;
	private final DirectoryRegistrar directoryRegistrar;
	private final LongSupplier currentTimeMillis;
	private final CallbackExecutor callbackExecutor;
	private final FileAttributesReader fileAttributesReader;
	private final long quietTimeMillis;
	private final long directoryRecheckIntervalMillis;

	private final Map<WatchKey, Path> watchedDirectories = new HashMap<>();
	private final Set<Path> changedPaths = new HashSet<>();
	private final Map<Path, FileSnapshot> lastKnownSnapshots = new HashMap<>();
	private long nextDirectoryCheckTime;

	FileWatcherThread(String name, Duration quietTime, Duration directoryRecheckInterval) throws IOException {
		this(
			name,
			quietTime,
			directoryRecheckInterval,
			FileSystems.getDefault().newWatchService(),
			Files::isDirectory,
			FileWatcherThread::registerDirectory,
			System::currentTimeMillis,
			ThreadedCallbackExecutor.INSTANCE
		);
	}

	FileWatcherThread(
		String name,
		Duration quietTime,
		Duration directoryRecheckInterval,
		WatchService watchService,
		Predicate<Path> isDirectory,
		DirectoryRegistrar directoryRegistrar,
		LongSupplier currentTimeMillis,
		CallbackExecutor callbackExecutor
	) {
		this(
			name,
			quietTime,
			directoryRecheckInterval,
			watchService,
			isDirectory,
			directoryRegistrar,
			currentTimeMillis,
			callbackExecutor,
			FileWatcherThread::readAttributes
		);
	}

	FileWatcherThread(
		String name,
		Duration quietTime,
		Duration directoryRecheckInterval,
		WatchService watchService,
		Predicate<Path> isDirectory,
		DirectoryRegistrar directoryRegistrar,
		LongSupplier currentTimeMillis,
		CallbackExecutor callbackExecutor,
		FileAttributesReader fileAttributesReader
	) {
		super(Objects.requireNonNull(name, "name"));
		this.setDaemon(true);
		this.callbacks = new HashMap<>();
		this.directoriesToWatch = new HashSet<>();
		this.watchService = Objects.requireNonNull(watchService, "watchService");
		this.isDirectory = Objects.requireNonNull(isDirectory, "isDirectory");
		this.directoryRegistrar = Objects.requireNonNull(directoryRegistrar, "directoryRegistrar");
		this.currentTimeMillis = Objects.requireNonNull(currentTimeMillis, "currentTimeMillis");
		this.callbackExecutor = Objects.requireNonNull(callbackExecutor, "callbackExecutor");
		this.fileAttributesReader = Objects.requireNonNull(fileAttributesReader, "fileAttributesReader");
		this.quietTimeMillis = requirePositiveMillis(quietTime, "quietTime");
		this.directoryRecheckIntervalMillis = requirePositiveMillis(directoryRecheckInterval, "directoryRecheckInterval");
		this.nextDirectoryCheckTime = currentTimeMillis.getAsLong();
	}

	private static WatchKey registerDirectory(Path directory, WatchService watchService) throws IOException {
		return directory.register(watchService, WATCH_EVENT_KINDS);
	}

	private static BasicFileAttributes readAttributes(Path path) throws IOException {
		return Files.readAttributes(
			path,
			BasicFileAttributes.class,
			LinkOption.NOFOLLOW_LINKS
		);
	}

	private static long requirePositiveMillis(Duration duration, String name) {
		Objects.requireNonNull(duration, name);
		long millis = duration.toMillis();
		if (millis < 1) {
			throw new IllegalArgumentException(name + " must be at least 1 millisecond.");
		}
		return millis;
	}

	synchronized void addCallback(Path path, Runnable callback) {
		Path absolutePath = Objects.requireNonNull(path, "path")
			.toAbsolutePath()
			.normalize();
		Path directory = absolutePath.getParent();
		if (directory == null) {
			throw new IllegalArgumentException("Watched path must have a parent directory: " + path);
		}

		this.callbacks.put(absolutePath, Objects.requireNonNull(callback, "callback"));
		rememberSnapshot(absolutePath);
		if (this.directoriesToWatch.add(directory)) {
			this.nextDirectoryCheckTime = currentTimeMillis.getAsLong();
		}
	}

	@Override
	public void run() {
		try (watchService) {
			while (!Thread.currentThread().isInterrupted()) {
				runIteration();
			}
		} catch (InterruptedException ignored) {
			LOGGER.info("FileWatcher was interrupted, stopping.");
			Thread.currentThread().interrupt();
		} catch (ClosedWatchServiceException ignored) {
			LOGGER.info("FileWatcher was closed, stopping.");
		} catch (IOException e) {
			LOGGER.error("FileWatcher encountered an unhandled IOException, stopping.", e);
		} finally {
			synchronized (this) {
				watchedDirectories
					.keySet()
					.forEach(WatchKey::cancel);
				watchedDirectories.clear();
			}
		}
	}

	void shutdown() {
		interrupt();
		try {
			watchService.close();
		} catch (IOException e) {
			LOGGER.debug("Failed to close FileWatcher watch service.", e);
		}
	}

	void runIteration() throws InterruptedException {
		long time = currentTimeMillis.getAsLong();
		if (time >= nextDirectoryCheckTime) {
			nextDirectoryCheckTime = time + directoryRecheckIntervalMillis;
			watchDirectories();
		}

		// Collect as many changes as we can, and notify the callbacks when we stop getting new changes.
		WatchKey watchKey = watchService.poll(quietTimeMillis, TimeUnit.MILLISECONDS);
		if (watchKey != null) {
			pollWatchKey(watchKey);
		} else {
			notifyChanges();
		}
	}

	private synchronized void pollWatchKey(WatchKey watchKey) throws InterruptedException {
		Path watchedDirectory = watchedDirectories.get(watchKey);
		if (watchedDirectory == null) {
			return;
		}

		List<WatchEvent<?>> events = watchKey.pollEvents();
		boolean shouldCheckDirectoriesAfterEvents = false;
		for (WatchEvent<?> event : events) {
			if (Thread.currentThread().isInterrupted()) {
				throw new InterruptedException();
			}
			if (event.kind() == StandardWatchEventKinds.OVERFLOW) {
				checkWatchedFilesInDirectoryForChanges(watchedDirectory);
				shouldCheckDirectoriesAfterEvents = true;
				break;
			} else if (event.context() instanceof Path eventPath) {
				shouldCheckDirectoriesAfterEvents = true;
				Path fullPath = watchedDirectory.resolve(eventPath)
					.toAbsolutePath()
					.normalize();
				if (callbacks.containsKey(fullPath)) {
					changedPaths.add(fullPath);
					rememberSnapshot(fullPath);
				}
			}
		}

		if (!watchKey.reset()) {
			LOGGER.info("Failed to re-watch directory {}. It may have been deleted.", watchedDirectory);
			watchedDirectories.remove(watchKey);
		}
		if (shouldCheckDirectoriesAfterEvents) {
			nextDirectoryCheckTime = currentTimeMillis.getAsLong();
		}
	}

	private void checkWatchedFilesInDirectoryForChanges(Path watchedDirectory) {
		// We missed some events, so compare the affected directory's watched files
		// against their last known metadata instead of assuming all of them changed.
		for (Path path : callbacks.keySet()) {
			if (path.getParent().equals(watchedDirectory) && rememberSnapshot(path)) {
				changedPaths.add(path);
			}
		}
	}

	/**
	 * Records the current metadata snapshot for a watched file.
	 *
	 * @param path the watched file path to snapshot
	 * @return true when the current snapshot differs from the last known snapshot,
	 * or when the current snapshot cannot be read
	 */
	private boolean rememberSnapshot(Path path) {
		FileSnapshot previousSnapshot = lastKnownSnapshots.get(path);
		FileSnapshot currentSnapshot;
		try {
			currentSnapshot = FileSnapshot.read(path, fileAttributesReader);
		} catch (IOException e) {
			LOGGER.debug("Unable to read file metadata for {}, treating it as changed.", path, e);
			lastKnownSnapshots.remove(path);
			return true;
		}

		lastKnownSnapshots.put(path, currentSnapshot);
		return !currentSnapshot.equals(previousSnapshot);
	}

	private synchronized void notifyChanges() {
		if (changedPaths.isEmpty()) {
			return;
		}
		LOGGER.debug(
			"Detected changes in files:\n{}",
			changedPaths.stream()
				.map(Path::toString)
				.collect(Collectors.joining("\n"))
		);

		List<Runnable> runnables = changedPaths.stream()
			.map(callbacks::get)
			.filter(Objects::nonNull)
			.toList();

		changedPaths.clear();

		// The FileWatcherThread is a daemon thread, so it can stop suddenly when the JVM exits.
		// Because callbacks often read and write from disk, run them in a separate non-daemon
		// thread so they can complete cleanly.
		callbackExecutor.execute(runnables);
	}

	private synchronized void watchDirectories() {
		for (Path directory : directoriesToWatch) {
			if (Thread.currentThread().isInterrupted()) {
				return;
			}
			if (isWatched(directory)) {
				continue;
			}
			if (isDirectory.test(directory)) {
				if (watchDirectory(directory)) {
					checkWatchedFilesInDirectoryForChanges(directory);
				}
			}
		}
	}

	private boolean isWatched(Path directory) {
		return watchedDirectories.containsValue(directory);
	}

	private boolean watchDirectory(Path directory) {
		try {
			WatchKey key = directoryRegistrar.register(directory, watchService);
			watchedDirectories.put(key, directory);
			return true;
		} catch (IOException e) {
			LOGGER.error("Failed to watch directory: {}", directory, e);
			return false;
		}
	}

	@FunctionalInterface
	interface DirectoryRegistrar {
		WatchKey register(Path directory, WatchService watchService) throws IOException;
	}

	@FunctionalInterface
	interface CallbackExecutor {
		void execute(List<Runnable> runnables);
	}

	@FunctionalInterface
	interface FileAttributesReader {
		BasicFileAttributes read(Path path) throws IOException;
	}

	private record FileSnapshot(
		boolean exists,
		boolean regularFile,
		boolean directory,
		boolean symbolicLink,
		long size,
		FileTime lastModifiedTime,
		FileTime creationTime,
		@Nullable Object fileKey
	) {
		private static final FileTime MISSING_FILE_TIME = FileTime.fromMillis(0);
		private static final FileSnapshot MISSING = new FileSnapshot(
			false,
			false,
			false,
			false,
			-1,
			MISSING_FILE_TIME,
			MISSING_FILE_TIME,
			null
		);

		private static FileSnapshot read(Path path, FileAttributesReader fileAttributesReader) throws IOException {
			try {
				BasicFileAttributes attributes = fileAttributesReader.read(path);
				return new FileSnapshot(
					true,
					attributes.isRegularFile(),
					attributes.isDirectory(),
					attributes.isSymbolicLink(),
					attributes.size(),
					attributes.lastModifiedTime(),
					attributes.creationTime(),
					attributes.fileKey()
				);
			} catch (NoSuchFileException e) {
				return MISSING;
			}
		}
	}

	private enum ThreadedCallbackExecutor implements CallbackExecutor {
		INSTANCE;

		@Override
		public void execute(List<Runnable> runnables) {
			Thread runThread = new CallbackRunner(runnables);
			runThread.start();
		}
	}

	private static class CallbackRunner extends Thread {
		private final List<Runnable> runnables;

		CallbackRunner(List<Runnable> runnables) {
			super("FileWatcher Callback Runner");
			this.setDaemon(false);
			this.runnables = List.copyOf(runnables);
		}

		@Override
		public void run() {
			runnables.forEach(runnable -> {
				try {
					runnable.run();
				} catch (RuntimeException e) {
					LOGGER.error("FileWatcher callback failed.", e);
				}
			});
		}
	}
}
