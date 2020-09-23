package edu.umd.lib.camel.processors;

import edu.umd.lib.camel.utils.CsvWithoutHeaderOutput;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolutionMap;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.shared.NoWriterForLangException;
import org.apache.jena.sparql.resultset.ResultsFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class SparqlQueryProcessor implements Processor {
  private Logger log = LoggerFactory.getLogger(SparqlQueryProcessor.class);

  public static final String CSV_WITHOUT_HEADER = "csvWithoutHeader";

  private final String query;
  private final String resultsFormatName;

  private Map<String, String> binding = new HashMap<>();

  private static String getStringFromFile(File file) throws IOException {
    return new String(Files.readAllBytes(Paths.get(file.toURI())));
  }

  public SparqlQueryProcessor(File queryFile, String resultsFormatName) throws IOException {
    this(getStringFromFile(queryFile), resultsFormatName);
  }

  public SparqlQueryProcessor(String query, String resultsFormatName) {
    this.query = query;
    this.resultsFormatName = resultsFormatName.trim();
    ResultsFormat resultsFormat = ResultsFormat.lookup(resultsFormatName);

    if ((resultsFormat == null) &&
        !(CSV_WITHOUT_HEADER.toLowerCase().equals(resultsFormatName.toLowerCase()))) {
      throw new IllegalArgumentException("Unknown resultFormatName: " + resultsFormatName);
    }
  }

  @Override
  public void process(final Exchange exchange) throws IOException {
    final Object body = exchange.getIn().getBody();
    InputStream in = new ByteArrayInputStream((byte[]) body);
    // process headers for runtime bindings
    log.info("checking headers for binding definitions");
    for (Map.Entry<String, Object> entry : exchange.getIn().getHeaders().entrySet()) {
      final String key = entry.getKey();
      log.trace("Found key {}", key);
      if (key.matches("^CamelSparqlQueryBinding-.+")) {
        final String bindingName = key.substring(key.indexOf("-") + 1);
        //TODO: verify bindingName is a valid SPARQL variable name
        final String bindingValue = (String) entry.getValue();
        log.info("Adding binding {} with value {}", bindingName, bindingValue);
        binding.put(bindingName, bindingValue);
      }
    }

    String result = executeQuery(in);
    exchange.getIn().setBody(result);
  }

  protected String executeQuery(InputStream in) {
    log.debug("Executing query: {}, resultFormatName: {}", query, resultsFormatName);
    Model model = ModelFactory.createDefaultModel();
    model.read(in, null);

    // Create a new query
    Query q = QueryFactory.create(query);

    // Execute the query and obtain results
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (QueryExecution qe = QueryExecutionFactory.create(q, model)) {
      if (q.isSelectType()) {
        setInitialBindings(model, qe);
        ResultSet results = qe.execSelect();

        ResultsFormat resultsFormat = ResultsFormat.lookup(resultsFormatName);
        if (resultsFormat == null) {
          if ((resultsFormatName != null) && CSV_WITHOUT_HEADER.toLowerCase().equals(resultsFormatName.toLowerCase())) {
            CsvWithoutHeaderOutput csvOutput = new CsvWithoutHeaderOutput();
            csvOutput.format(out, results);
          } else {
            throw new IllegalArgumentException("Unknown resultFormatName: " + resultsFormatName);
          }
        } else {
          ResultSetFormatter.output(out, results, resultsFormat);
        }
      } else if (q.isConstructType()) {
        setInitialBindings(model, qe);
        Model results = qe.execConstruct();
        try {
          results.write(out, resultsFormatName);
        } catch (NoWriterForLangException e) {
          throw new IllegalArgumentException("Unknown resultFormatName: " + resultsFormatName);
        }
      } else {
        throw new IllegalArgumentException("Only SELECT and CONSTRUCT queries are allowed as values of query");
      }
    }

    return out.toString();
  }

  private void setInitialBindings(Model model, QueryExecution qe) {
    if (binding != null && !binding.isEmpty()) {
      QuerySolutionMap map = new QuerySolutionMap();
      for (Map.Entry<String, String> b : binding.entrySet()) {
        map.add(b.getKey(), model.createLiteral(b.getValue()));
      }
      qe.setInitialBinding(map);
    }
  }

  public Map<String, String> getBinding() {
    return binding;
  }

  public void setBinding(Map<String, String> binding) {
    this.binding = binding;
  }
}
