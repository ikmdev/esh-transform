package dev.ikm.ike.esh.tinkarizer.etl.impl;

import java.io.File;
import java.util.List;

import dev.ikm.ike.esh.tinkarizer.etl.Database;
import dev.ikm.ike.esh.tinkarizer.etl.Extractor;
import dev.ikm.ike.esh.tinkarizer.etl.Loader;
import dev.ikm.ike.esh.tinkarizer.etl.Transformer;
import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableSourceRecord;

public class SimplePipeline extends AbstractPipeline {

	public SimplePipeline(Extractor extractor, Transformer transformer, Loader loader) {
		super(extractor, transformer, loader);
	}

	@Override
	public void run(File dbPath, String dbName) {
		try (@SuppressWarnings("unused") Database ignored = new Database(dbPath, dbName)) {
			// Extract
			List<ViewableSourceRecord> viewableSourceRecords = extractor.getExtractedViewableData();
			List<NavigableSourceRecord> navigableSourceRecords = extractor.getExtractedNavigableData();
			// Transform
			List<ViewableCanonicalRecord> viewableData = transformer.transformViewables(viewableSourceRecords);
			List<NavigableCanonicalRecord> navigableData = transformer.transformNavigables(navigableSourceRecords);
			// Load
			loader.loadViewableData(viewableData);
			loader.loadNavigableData(navigableData);
		}
	}
}
