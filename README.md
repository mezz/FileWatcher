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

fileWatcher.addCallback(Path.of("config/client.toml"), () -> {
    // Reload the file here.
});
fileWatcher.start();

Runtime.getRuntime().addShutdownHook(new Thread(fileWatcher::close));
```

If the current filesystem cannot create a watcher, the constructor throws the
checked exception `FileWatcherUnavailableException`.

## Installation

FileWatcher is published to Maven Central:

```xml
<dependency>
    <groupId>net.mezzdev</groupId>
    <artifactId>filewatcher</artifactId>
    <version>0.2.0</version>
</dependency>
```

## Requirements

- Java 17 or newer

## Development

Run the test suite with:

```sh
./mvnw verify
```
