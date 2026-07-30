# FileWatcher

A small Java library for watching individual files and running callbacks when they are created, modified, or deleted.

## Usage

```java
import net.mezzdev.filewatcher.FileWatcher;

import java.nio.file.Path;

FileWatcher fileWatcher = new FileWatcher("config watcher");
fileWatcher.addCallback(Path.of("config/client.toml"), () -> {
    // Reload the file here.
});
fileWatcher.start();

Runtime.getRuntime().addShutdownHook(new Thread(fileWatcher::close));
```

## Installation

FileWatcher is published to Maven Central:

```xml
<dependency>
    <groupId>net.mezzdev</groupId>
    <artifactId>filewatcher</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Requirements

- Java 17 or newer

## Development

Run the test suite with:

```sh
./mvnw verify
```
