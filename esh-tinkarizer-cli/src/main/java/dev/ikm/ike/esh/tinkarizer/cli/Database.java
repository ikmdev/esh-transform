package dev.ikm.ike.esh.tinkarizer.cli;

import java.io.File;

import dev.ikm.tinkar.common.service.CachingService;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.service.ServiceKeys;
import dev.ikm.tinkar.common.service.ServiceProperties;
import dev.ikm.tinkar.entity.EntityService;

public class Database implements AutoCloseable {

	public Database(File dbPath, String databaseName) {
		CachingService.clearAll();
		ServiceProperties.set(ServiceKeys.DATA_STORE_ROOT, dbPath);
		PrimitiveData.selectControllerByName(databaseName);
		PrimitiveData.start();
		EntityService.get().beginLoadPhase();
	}

	@Override
	public void close() {
		EntityService.get().endLoadPhase();
		PrimitiveData.stop();
	}
}
