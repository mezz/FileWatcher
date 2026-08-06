# FileWatcher

A small Java library for watching individual files and running callbacks when they are created, modified, or deleted.

## Usage

```java
import net.mezzdev.filewatcher.FileWatcher;
import net.mezzdev.filewatcher.FileWatcherUnavailableException;

import java.nio.file.Path;

FileWatcher fileWatcher;
try {
    fileWatcher = new FileWatcher("config watcher");
} catch (FileWatcherUnavailableException e) {
    // Watching is not available on this filesystem.
    return;
}

Runnable unsubscribe = fileWatcher.addCallback(Path.of("config/client.toml"), () -> {
    // Reload the file here.
});
fileWatcher.start();

// When this file is no longer relevant:
unsubscribe.run();

Runtime.getRuntime().addShutdownHook(new Thread(fileWatcher::close));
```

Calling the returned unsubscribe callback more than once is safe. It removes
only its own registration. Multiple callbacks can be registered for the same
path, including multiple registrations of the same callback object.
Registration and unsubscription are safe before or after `start()`, and
unsubscribe callbacks remain safe after the watcher is closed.

If the current filesystem cannot create a watcher, the constructor throws the
checked exception `FileWatcherUnavailableException`.

To customize the change-settling delay and missing-directory retry interval,
pass explicit durations:

```java
import java.time.Duration;

FileWatcher fileWatcher = new FileWatcher(
    "config watcher",
    Duration.ofMillis(250), // changeSettlingDelay
    Duration.ofSeconds(10)  // missingDirectoryRetryInterval
);
```

## Installation

FileWatcher is published to Maven Central:

```xml
<dependency>
    <groupId>net.mezzdev</groupId>
    <artifactId>filewatcher</artifactId>
    <version>0.3.0</version>
</dependency>
```

## Requirements

- Java 17 or newer

## Development

Run the test suite with:

```sh
./mvnw verify
```
