package dev.ikm.ike.esh.tinkarizer.etl.impl;

import java.io.File;
import java.util.Objects;

import dev.ikm.ike.esh.tinkarizer.etl.Extractor;
import dev.ikm.ike.esh.tinkarizer.etl.Loader;
import dev.ikm.ike.esh.tinkarizer.etl.Pipeline;
import dev.ikm.ike.esh.tinkarizer.etl.Transformer;

public abstract class AbstractPipeline implements Pipeline {

	protected final Extractor extractor;
	protected final Transformer transformer;
	protected final Loader loader;

	public AbstractPipeline(Extractor extractor, Transformer transformer, Loader loader) {
		this.extractor = Objects.requireNonNull(extractor, "extractor cannot be null");
		this.transformer = Objects.requireNonNull(transformer, "transformer cannot be null");
		this.loader = Objects.requireNonNull(loader, "loader cannot be null");
	}

	@Override
	public abstract void run(File dbPath, String dbName);

}
