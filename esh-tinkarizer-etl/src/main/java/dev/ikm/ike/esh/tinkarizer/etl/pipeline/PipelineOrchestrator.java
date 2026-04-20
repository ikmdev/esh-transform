package dev.ikm.ike.esh.tinkarizer.etl.pipeline;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;

public class PipelineOrchestrator {

	private final static Logger LOG = org.slf4j.LoggerFactory.getLogger(PipelineOrchestrator.class);

	private final List<Pipeline> pipelines;

	public PipelineOrchestrator() {
		this.pipelines = new ArrayList<>();
	}

	public void addPipeline(Pipeline pipeline) {
		this.pipelines.add(pipeline);
	}

	public void runPipelines() {
		Instant startTime = Instant.now();
		LOG.info("Starting pipeline execution with {} pipelines", pipelines.size());

		for (Pipeline pipeline : pipelines) {
			LOG.info("Running pipeline: {}", pipeline.getName());
			try {
				pipeline.run();
			} catch (Exception ex) {
				LOG.error("Error executing pipeline: {}", pipeline.getName(), ex);
			}
		}

		Instant endTime = Instant.now();
		Duration duration = Duration.between(startTime, endTime);
		LOG.info("Completed {} pipelines in {} minutes {} seconds", pipelines.size(), duration.toMinutes(),
				duration.getSeconds());
	}

}
