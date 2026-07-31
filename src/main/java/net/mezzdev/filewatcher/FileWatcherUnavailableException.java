package net.mezzdev.filewatcher;

/**
 * Thrown when the platform cannot create a usable {@link FileWatcher}.
 */
public final class FileWatcherUnavailableException extends Exception {
	public FileWatcherUnavailableException(String message, Throwable cause) {
		super(message, cause);
	}
}
