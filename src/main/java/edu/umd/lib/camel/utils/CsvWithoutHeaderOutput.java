package edu.umd.lib.camel.utils;

import org.apache.jena.graph.Node;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.ARQException;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.resultset.CSVOutput;
import org.apache.jena.sparql.util.NodeToLabelMap;
import org.apache.jena.util.FileUtils;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * Subclass of CSVOutput that does not output the header line.
 */
public class CsvWithoutHeaderOutput extends CSVOutput {
  /**
   * Copied from CSVOutput
   */
  protected static String NL = "\r\n";

  @Override
  public void format(OutputStream out, ResultSet resultSet) {
    try {
      Writer w = FileUtils.asUTF8(out);
      NodeToLabelMap bnodes = new NodeToLabelMap();
      w = new BufferedWriter(w);

      List<String> varNames = resultSet.getResultVars();
      List<Var> vars = new ArrayList<>(varNames.size());

      // Convert varNames to Vars and add to list
      for (String v : varNames) {
        vars.add(Var.alloc(v));
      }

      // Data output
      while (resultSet.hasNext()) {
        String sep = null;
        Binding b = resultSet.nextBinding();

        for (Var v : vars) {
          if (sep != null)
            w.write(sep);
          sep = ",";

          Node n = b.get(v);
          if (n != null)
            output(w, n, bnodes);
        }
        w.write(NL);
      }
      w.flush();
    } catch (IOException ex) {
      throw new ARQException(ex);
    }
  }

  /**
   * Copied from CSVOutput
   */
  protected void output(Writer w, Node n, NodeToLabelMap bnodes) throws IOException {
    // String str = FmtUtils.stringForNode(n) ;
    String str = "?";
    if (n.isLiteral())
      str = n.getLiteralLexicalForm();
    else if (n.isURI())
      str = n.getURI();
    else if (n.isBlank())
      str = bnodes.asString(n);

    str = csvSafe(str);
    w.write(str);
  }

  /**
   * Copied from CSVOutput
   */
  protected String csvSafe(String str) {
    // Apparently, there are CSV parsers that only accept "" as an escaped quote if inside a "..."
    if (str.contains("\"")
        || str.contains(",")
        || str.contains("\r")
        || str.contains("\n"))
      str = "\"" + str.replaceAll("\"", "\"\"") + "\"";
    else if (str.isEmpty())
      // Return the quoted empty string.
      str = "\"\"";
    return str;
  }
}
