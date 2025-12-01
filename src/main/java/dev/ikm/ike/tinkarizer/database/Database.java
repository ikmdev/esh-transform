package dev.ikm.ike.tinkarizer.database;

import dev.ikm.tinkar.common.service.CachingService;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.service.ServiceKeys;
import dev.ikm.tinkar.common.service.ServiceProperties;
import dev.ikm.tinkar.entity.EntityService;

import java.io.File;

public class Database implements AutoCloseable {

	public Database(File dbPath, String databaseName) {
		CachingService.clearAll();
		ServiceProperties.set(ServiceKeys.DATA_STORE_ROOT, dbPath);
		PrimitiveData.selectControllerByName(databaseName);
		PrimitiveData.start();
		EntityService.get().beginLoadPhase();
	}

	@Override
	public void close() throws Exception {
		EntityService.get().endLoadPhase();
		PrimitiveData.stop();
	}
}
