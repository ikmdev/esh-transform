package dev.ikm.ike.esh.tinkarizer.etl.load;

import java.util.List;

import org.slf4j.Logger;

import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableCanonicalRecord;

public class SimpleLoader implements Loader {

	private final static Logger LOG = org.slf4j.LoggerFactory.getLogger(SimpleLoader.class);

	@Override
	public void loadNavigableData(LoadContext loadContext, List<NavigableCanonicalRecord> navigableCanonicalRecords) {
		navigableCanonicalRecords.forEach(navigableRecord -> {
			//NEED to have creation of context to be autowired in the pipeline
			// need to add to transaction
			// then call write methods
		});
	}

	@Override
	public void loadViewableData(LoadContext loadContext, List<ViewableCanonicalRecord> viewableCanonicalRecords) {
		viewableCanonicalRecords.forEach(viewableRecord -> {
			// need to add to transaction
			// then call write methods
		});
	}

}
