package com.lolski.ignite;

import java.nio.file.Path;

class PersistenceSettings {
    public final Path location;

    public PersistenceSettings(Path location) {
        this.location = location;
    }
}
