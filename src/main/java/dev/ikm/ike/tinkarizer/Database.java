package dev.ikm.ike.tinkarizer;

import dev.ikm.tinkar.common.service.CachingService;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.service.ServiceKeys;
import dev.ikm.tinkar.common.service.ServiceProperties;
import dev.ikm.tinkar.entity.EntityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

class Database {

	Logger LOG = LoggerFactory.getLogger(Database.class);

	private final String databaseName;
	private final File dbPath;

	public Database(String databaseName, File dbPath) {
		this.databaseName = databaseName;
		this.dbPath = dbPath;

	}

	public void start() {
		LOG.info("Starting database...");
		CachingService.clearAll();
		ServiceProperties.set(ServiceKeys.DATA_STORE_ROOT, dbPath);
		PrimitiveData.selectControllerByName(databaseName);
		PrimitiveData.start();
		EntityService.get().beginLoadPhase();
	}

	public void shutdown() {
		LOG.info("Stopping database...");
		EntityService.get().endLoadPhase();
		PrimitiveData.stop();
	}
}
