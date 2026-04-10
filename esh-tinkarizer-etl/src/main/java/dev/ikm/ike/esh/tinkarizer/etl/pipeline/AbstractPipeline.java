package dev.ikm.ike.esh.tinkarizer.etl.pipeline;

import java.util.Objects;

import dev.ikm.ike.esh.tinkarizer.etl.extractor.Extractor;
import dev.ikm.ike.esh.tinkarizer.etl.loader.Loader;
import dev.ikm.ike.esh.tinkarizer.etl.transformer.Transformer;

public abstract class AbstractPipeline implements Pipeline {

	protected final Extractor extractor;
	protected final Transformer transformer;
	protected final Loader loader;

	public AbstractPipeline(Extractor extractor, Transformer transformer, Loader loader) {
		this.extractor = Objects.requireNonNull(extractor, "extractor cannot be null");
		this.transformer = Objects.requireNonNull(transformer, "transformer cannot be null");
		this.loader = Objects.requireNonNull(loader, "loader cannot be null");
	}

}
