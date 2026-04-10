package dev.ikm.ike.esh.tinkarizer.etl.loader;

import java.util.List;

import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableCanonicalRecord;

public interface Loader {

	void loadViewableData(List<ViewableCanonicalRecord> viewableData);

	void loadNavigableData(List<NavigableCanonicalRecord> navigableData);
}
