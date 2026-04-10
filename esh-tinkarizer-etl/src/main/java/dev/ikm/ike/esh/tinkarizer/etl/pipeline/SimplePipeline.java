package dev.ikm.ike.esh.tinkarizer.etl.pipeline;

import java.util.List;

import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.extractor.Extractor;
import dev.ikm.ike.esh.tinkarizer.etl.loader.Loader;
import dev.ikm.ike.esh.tinkarizer.etl.transformer.Transformer;

public class SimplePipeline extends AbstractPipeline {

	public SimplePipeline(Extractor extractor, Transformer transformer, Loader loader) {
		super(extractor, transformer, loader);
	}

	@Override
	public void executeViewableETL() {
		List<ViewableSourceRecord> viewableSourceRecords = extractor.getExtractedViewableData();
		List<ViewableCanonicalRecord> viewableData = transformer.transformViewables(viewableSourceRecords);
		loader.loadViewableData(viewableData);

	}

	@Override
	public void executeNavigableETL() {
		List<NavigableSourceRecord> navigableSourceRecords = extractor.getExtractedNavigableData();
		List<NavigableCanonicalRecord> navigableData = transformer.transformNavigables(navigableSourceRecords);
		loader.loadNavigableData(navigableData);
	}

}
