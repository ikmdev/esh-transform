package dev.ikm.ike.esh.tinkarizer.etl;

import java.io.File;

public interface Pipeline {

	void run(File dbPath, String dbName);
}