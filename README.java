package mux41;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.core.PathBlock;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.graph.GraphFactory;
import org.apache.jena.sparql.path.PathCompiler;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementOptional;
import org.apache.jena.sparql.syntax.ElementPathBlock;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.stream.Stream;

public class QueryMatch {

    public static void main(String[] args) {
        solve(QueryFactory.create("SELECT * {" +
                "?s <http://a>/<http://b>/<http://c> ?o. " +
                "Filter NOT EXISTS {?d ?n ?e. ?d2 ?n2 ?e2} " +
                "?s <http://n> ?dd ." +
                "MINUS {?m ?i ?n. ?min1 ?min2 ?min3} ." +
                "?s <http://n> \"poop\" ." +
                "?s <http://n4> \"poop\" ." +
                "}"));
    }

    public static void solve(Query query) {
        var data = ModelFactory.createDefaultModel();
        var s = "<http://sttt> <http://a> <http://x> . " +
                "<http://x> <http://b> <http://y> . " +
                "<http://y> <http://c> <http://o> . " +
                "<http://sttt> <http://n> <http://p> . ";
        data.read(new ByteArrayInputStream(s.getBytes()), null, "TTL");

        List<Triple> queryTriples = new ArrayList<>();
        Map<Var, Object> literalBindings = new HashMap<>();
        processQuery(query, queryTriples, literalBindings);
        var matchQuery = createMatchQuery(queryTriples);

        System.out.println(solve(matchQuery, data, queryTriples, literalBindings));
    }

    private static final PathCompiler pathCompiler = new PathCompiler();

    public static void processQuery(Query query, List<Triple> triples, Map<Var, Object> literalBindings) {
        getQueryPathBlocks(query)
                .flatMap(pathBlock -> pathCompiler.reduce(pathBlock).getList().stream())
                .map(TriplePath::asTriple)
                .forEach(triple -> {
                    triple = removeAnonVars(triple);
                    triple = replaceLiteral(triple, literalBindings);
                    triples.add(triple);
                });
    }

    public static Optional<QueryMatchResult> solve(Query query, Model data, List<Triple> queryTriples, Map<Var, Object> queryLiteralVars) {
        List<QueryMatchResult> results = new ArrayList<>();
        var resultSet = QueryExecutionFactory.create(query, data).execSelect();
        while (resultSet.hasNext()) {
            Map<Node, Node> bindings = new HashMap<>();
            var solution = resultSet.next();
            var varNames = solution.varNames();
            while (varNames.hasNext()) {
                var varName = varNames.next();
                bindings.put(Var.alloc(varName), solution.get(varName).asNode());
            }
            var alignedData = substituteVars(data, bindings);
            var unmatched = new HashSet<>(queryTriples);
            var resultCoverage = coverage(alignedData, queryTriples, unmatched);
            var boundLiterals = bindLiterals(bindings, queryLiteralVars);
            results.add(new QueryMatchResult(resultCoverage, bindings, alignedData, unmatched, boundLiterals));
        }
        return results.stream().max(Comparator.comparingDouble(QueryMatchResult::getCoverage));
    }

    private static Map<Node, Object> bindLiterals(Map<Node, Node> dataBindings, Map<Var, Object> literalBindings) {
        Map<Node, Object> boundLiterals = new HashMap<>();
        literalBindings.forEach((var, literal) -> {
            var dataNode = dataBindings.get(var);
            if (dataNode != null) {
                boundLiterals.put(dataNode, literal);
            }
        });
        return boundLiterals;
    }

    public static Graph substituteVars(Model m, Map<Node, Node> bindings) {
        Graph g = GraphFactory.createDefaultGraph();
        m.listStatements()
                .mapWith(s -> substituteVars(s.asTriple(), bindings))
                .forEach(g::add);
        return g;
    }

    public static Triple substituteVars(Triple s, Map<Node, Node> bindings) {
        return new Triple(
                Var.lookup(bindings::get, s.getSubject()),
                Var.lookup(bindings::get, s.getPredicate()),
                Var.lookup(bindings::get, s.getObject()));
    }

    public static double coverage(Graph data, List<Triple> query, Set<Triple> unmatched) {
        int i = 0;
        for (var triple : query) {
            var iter = data.find(triple);
            if (iter.hasNext()) {
                i++;
                unmatched.remove(triple);
            }
        }
        return (double) i / query.size();
    }

    public static Stream<PathBlock> getQueryPathBlocks(Query query) {
        return getQueryPathBlocks(query.getQueryPattern());
    }

    public static Stream<PathBlock> getQueryPathBlocks(Element el) {
        if (el instanceof ElementPathBlock) {
            return Stream.of(((ElementPathBlock) el).getPattern());
        }
        if (el instanceof ElementGroup) {
            return ((ElementGroup) el).getElements().stream()
                    .flatMap(QueryMatch::getQueryPathBlocks);
        }
        if (el instanceof ElementOptional) {
            return getQueryPathBlocks(((ElementOptional) el).getOptionalElement());
        }
        return Stream.empty();
    }

    public static Query createMatchQuery(List<Triple> triples) {
        Query query = QueryFactory.create();
        query.setQuerySelectType();
        query.setQueryResultStar(true);

        var groupEl = new ElementGroup();
        triples.forEach(t -> {
            var pathBlock = new ElementPathBlock();
            pathBlock.addTriple(t);
            groupEl.addElement(new ElementOptional(pathBlock));
        });
        query.setQueryPattern(groupEl);

        return query;
    }

    public static Triple removeAnonVars(Triple t) {
        Node s = t.getSubject();
        Node p = t.getPredicate();
        Node o = t.getObject();

        Node s1 = removeAnonVars(s);
        Node p1 = removeAnonVars(p);
        Node o1 = removeAnonVars(o);

        if (s1 != s || p1 != p || o1 != o) {
            return new Triple(s1, p1, o1);
        }

        return t;
    }

    public static Node removeAnonVars(Node n) {
        if (n.isVariable() && n.getName().startsWith(ARQConstants.allocVarAnonMarker)) {
            return allocVar();
        }
        return n;
    }

    public static Triple replaceLiteral(Triple t, Map<Var, Object> literalBindings) {
        Node o = t.getObject();

        if (o.isLiteral()) {
            var var = allocVar();
            literalBindings.put(var, o.getLiteral().getValue());
            return new Triple(t.getSubject(), t.getPredicate(), var);
        }

        return t;
    }

    public static Var allocVar() {
        return Var.alloc("var_" + UUID.randomUUID().toString().substring(24));
    }

    @Data
    @AllArgsConstructor
    public static class QueryMatchResult {
        private double coverage;
        private Map<Node, Node> bindings;
        private Graph alignedData;
        private Set<Triple> unmatched;
        Map<Node, Object> boundLiterals;
    }

}
