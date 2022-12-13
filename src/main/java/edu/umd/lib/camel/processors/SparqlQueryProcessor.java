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
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.shared.NoWriterForLangException;
import org.apache.jena.sparql.resultset.ResultsFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SparqlQueryProcessor implements Processor, Serializable {
  private final Logger logger = LoggerFactory.getLogger(SparqlQueryProcessor.class);

  public static final String CSV_WITHOUT_HEADER = "csvWithoutHeader";

  private String query;

  private String resultsFormatName;

  public SparqlQueryProcessor() {}

  @Override
  public void process(final Exchange exchange) {
    final Message in = exchange.getIn();
    in.setBody(executeQuery(in));
  }

  Map<String, RDFNode> parseBindings(final Message message, final Model model) {
    final Map<String, RDFNode> bindings = new HashMap<>();

    // process headers for runtime bindings
    logger.debug("Checking headers for binding definitions");
    for (Map.Entry<String, Object> entry : message.getHeaders().entrySet()) {
      final String key = entry.getKey();
      logger.trace("Found key {}", key);
      if (key.matches("^CamelSparqlQueryBinding-Literal-.+")) {
        final String bindingName = extractBindingName(key);
        final String bindingValue = (String) entry.getValue();
        logger.debug("Binding ?{} to literal: \"{}\"", bindingName, bindingValue);
        bindings.put(bindingName, model.createLiteral(bindingValue));
      }
      if (key.matches("^CamelSparqlQueryBinding-URI-.+")) {
        final String bindingName = extractBindingName(key);
        final String bindingValue = (String) entry.getValue();
        logger.debug("Binding ?{} to URI {}", bindingName, bindingValue);
        bindings.put(bindingName, model.createResource(bindingValue));
      }
    }

    return bindings;
  }

  private String extractBindingName(final String key) {
    final String[] parts = key.split("-", 3);
    logger.debug("Extracted {} from key name {}", parts[2], key);
    //TODO: verify bindingName is a valid SPARQL variable name
    return parts[2];
  }

  protected String executeQuery(Message in) {
    logger.debug("Executing query: {}, resultFormatName: {}", query, resultsFormatName);
    final InputStream body = in.getBody(InputStream.class);
    logger.debug("Got InputStream (Message ID: {})", in.getMessageId());
    // XXX: creating the default model appears to be where the Camel route is failing
    final Model model = ModelFactory.createDefaultModel();
    logger.debug("Created default model");
    model.read(body, in.getHeader("CamelFcrepoUri", String.class), "RDF/XML");
    logger.debug("Read message body into model");

    final Map<String, RDFNode> bindings = parseBindings(in, model);

    // Create a new query
    final Query q = QueryFactory.create(query);

    // Execute the query and obtain results
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    final QueryExecution qe = QueryExecutionFactory.create(q, model);
    if (q.isSelectType()) {
      logger.debug("Executing SELECT query");
      setInitialBindings(qe, bindings);
      final ResultSet results = qe.execSelect();

      final ResultsFormat resultsFormat = ResultsFormat.lookup(resultsFormatName);
      if (resultsFormat == null) {
        if ((resultsFormatName != null) && CSV_WITHOUT_HEADER.toLowerCase().equals(resultsFormatName.toLowerCase())) {
          final CsvWithoutHeaderOutput csvOutput = new CsvWithoutHeaderOutput();
          csvOutput.format(out, results);
        } else {
          logger.error("Unknown resultsFormatName: " + resultsFormatName);
          throw new IllegalArgumentException("Unknown resultsFormatName: " + resultsFormatName);
        }
      } else {
        ResultSetFormatter.output(out, results, resultsFormat);
      }
    } else if (q.isConstructType()) {
      logger.debug("Executing CONSTRUCT query");
      setInitialBindings(qe, bindings);
      final Model results = qe.execConstruct();
      try {
        results.write(out, resultsFormatName);
      } catch (NoWriterForLangException e) {
        logger.error("Unknown resultsFormatName: " + resultsFormatName);
        throw new IllegalArgumentException("Unknown resultsFormatName: " + resultsFormatName);
      }
    } else {
      logger.error("Only SELECT and CONSTRUCT queries are allowed as values of query");
      throw new IllegalArgumentException("Only SELECT and CONSTRUCT queries are allowed as values of query");
    }

    return out.toString();
  }

  private void setInitialBindings(QueryExecution qe, Map<String, RDFNode> bindings) {
    if (bindings != null && !bindings.isEmpty()) {
      QuerySolutionMap map = new QuerySolutionMap();
      for (Map.Entry<String, RDFNode> b : bindings.entrySet()) {
        map.add(b.getKey(), b.getValue());
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
