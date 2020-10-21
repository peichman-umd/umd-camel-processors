package edu.umd.lib.camel.processors;

import edu.umd.lib.camel.utils.CsvWithoutHeaderOutput;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
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
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class SparqlQueryProcessor implements Processor, Serializable {
  private final Logger logger = LoggerFactory.getLogger(SparqlQueryProcessor.class);

  public static final String CSV_WITHOUT_HEADER = "csvWithoutHeader";

  private String query;

  private String resultsFormatName;


  private static String getStringFromFile(File file) throws IOException {
    return new String(Files.readAllBytes(Paths.get(file.toURI())));
  }

  public SparqlQueryProcessor() {}

  @Override
  public void process(final Exchange exchange) {
    final Message in = exchange.getIn();
    final ByteArrayInputStream body = (ByteArrayInputStream) in.getBody();
    in.setBody(executeQuery(body, getBindings(in)));
  }

  Map<String, String> getBindings(final Message message) {
    final Map<String, String> bindings = new HashMap<>();

    // process headers for runtime bindings
    logger.info("Checking headers for binding definitions");
    for (Map.Entry<String, Object> entry : message.getHeaders().entrySet()) {
      final String key = entry.getKey();
      logger.trace("Found key {}", key);
      if (key.matches("^CamelSparqlQueryBinding-.+")) {
        final String bindingName = key.substring(key.indexOf("-") + 1);
        //TODO: verify bindingName is a valid SPARQL variable name
        final String bindingValue = (String) entry.getValue();
        logger.info("Adding binding {} with value {}", bindingName, bindingValue);
        bindings.put(bindingName, bindingValue);
      }
    }

    return bindings;
  }

  protected String executeQuery(InputStream in, Map<String, String> bindings) {
    logger.debug("Executing query: {}, resultFormatName: {}", query, resultsFormatName);
    Model model = ModelFactory.createDefaultModel();
    model.read(in, null);

    // Create a new query
    Query q = QueryFactory.create(query);

    // Execute the query and obtain results
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (QueryExecution qe = QueryExecutionFactory.create(q, model)) {
      if (q.isSelectType()) {
        setInitialBindings(model, qe, bindings);
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
        setInitialBindings(model, qe, bindings);
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

  private void setInitialBindings(Model model, QueryExecution qe, Map<String, String> bindings) {
    if (bindings != null && !bindings.isEmpty()) {
      QuerySolutionMap map = new QuerySolutionMap();
      for (Map.Entry<String, String> b : bindings.entrySet()) {
        map.add(b.getKey(), model.createLiteral(b.getValue()));
      }
      qe.setInitialBinding(map);
    }
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public String getResultsFormatName() {
    return resultsFormatName;
  }

  public void setResultsFormatName(String resultsFormatName) {
    this.resultsFormatName = resultsFormatName;
  }
}
