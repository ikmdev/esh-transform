package dev.ikm.ike.tinkarizer;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ESHTinkarizer {

	private final Database database;
	private final Parser parser;
	private final Transformer transformer;

	public ESHTinkarizer(String databaseName, File dbPath, File eventSetCSV, File eventCodeCSV, long time) {
		this.database = new Database(databaseName, dbPath);
		this.database.start();
		this.parser = new Parser(100_000, eventSetCSV, eventCodeCSV);
		this.transformer = new Transformer(time);
	}

	public void run() {
		parser.parseViewableData(viewableData -> {
			try(ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
				Future<?> transformFuture = executor.submit(() -> transformer.viewableTransformation().accept(viewableData));
				transformFuture.get();
			} catch (Exception e) {
				e.printStackTrace();
			}
		});

		transformer.commit();

		//Shutdown database
		database.shutdown();
	}
}
