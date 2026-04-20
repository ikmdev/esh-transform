package dev.ikm.ike.esh.tinkarizer.etl.load;

import java.util.List;

import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableCanonicalRecord;

public interface Loader {

	void loadViewableData(LoadContext loadContext, List<ViewableCanonicalRecord> viewableCanonicalRecords);

	void loadNavigableData(LoadContext loadContext, List<NavigableCanonicalRecord> navigableCanonicalRecords);

}