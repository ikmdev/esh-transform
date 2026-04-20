package dev.ikm.ike.esh.tinkarizer.specialty.input;

import java.io.File;
import java.util.List;

import org.apache.commons.csv.CSVFormat;

import dev.ikm.ike.esh.tinkarizer.etl.domain.NavigableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.domain.ViewableSourceRecord;
import dev.ikm.ike.esh.tinkarizer.etl.extract.Extractor;

public class SpecialtyInputExtractor implements Extractor {

	private File esCSV;
	private File ecCSV;

	private final CSVFormat csvFormat;

	public SpecialtyInputExtractor() {
		this.csvFormat = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).get();
	}

	@Override
	public void addFileToExtract(File file) {
		if (file.getName().contains("event_set")) {
			this.esCSV = file;
		} else if (file.getName().contains("event_code")) {
			this.ecCSV = file;
		} else {
			throw new IllegalArgumentException("Unexpected file: " + file.getName());
		}
	}

	@Override
	public void addFilesToExtract(List<File> files) {
		files.forEach(this::addFileToExtract);
	}

	@Override
	public List<NavigableSourceRecord> getExtractedNavigableData() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ViewableSourceRecord> extractViewableData() {
		// TODO Auto-generated method stub
		return null;
	}

}
