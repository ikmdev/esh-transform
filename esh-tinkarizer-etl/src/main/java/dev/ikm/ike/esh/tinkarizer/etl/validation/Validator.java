package dev.ikm.ike.esh.tinkarizer.etl.validation;

import java.util.List;

import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableCanonicalRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableCanonicalRecord;

public interface Validator {

	void validateViewableData(List<ViewableCanonicalRecord> viewableCanonicalRecords);
	
	void validateNavigableData(List<NavigableCanonicalRecord> navigableCanonicalRecords);

}
