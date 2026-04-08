package dev.ikm.ike.esh.tinkarizer.etl;

import java.io.File;
import java.util.List;

import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableSourceRecord;

public interface Extractor {

	void addFileToExtract(File file);

	void addFilesToExtract(List<File> files);

	List<ViewableSourceRecord> getExtractedViewableData();

	List<NavigableSourceRecord> getExtractedNavigableData();
}
