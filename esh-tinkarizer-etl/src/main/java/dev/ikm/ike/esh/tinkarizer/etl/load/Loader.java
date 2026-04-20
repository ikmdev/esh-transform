package dev.ikm.ike.esh.tinkarizer.etl.load;

import java.util.List;

import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableCanonicalRecord;

public interface Loader {

	void loadViewableData(Writer write, List<ViewableCanonicalRecord> viewableCanonicalRecords);

	void loadNavigableData(Writer write, List<NavigableCanonicalRecord> navigableCanonicalRecords);

}