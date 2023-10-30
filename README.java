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
import org.apache.jena.rdf.model.ResourceFactory;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryMatch {

    public static final String VAR_NS = "http://vars#";

    public static final int MAX_SOLUTIONS_POLLED = 100;
    public static final int MAX_RESULTS_ADDED = 100;

    public static void main(String[] args) {
        match(QueryFactory.create("SELECT * {" +
                "?s <http://a>/<http://b>/<http://c> ?o. " +
                "Filter NOT EXISTS {?d ?n ?e. ?d2 ?n2 ?e2} " +
                "MINUS {?m ?i ?n. ?min1 ?min2 ?min3} ." +
                "?s <http://n> \"poop\" ." +
                "?s <http://n4> \"poop\" ." +
                "}"));
    }

    public static void match(Query query) {
        var data = ModelFactory.createDefaultModel();
        var s = "<http://sttt> <http://a> <http://x> . " +
                "<http://x> <http://b> <http://y> . " +
                "<http://y> <http://c> <http://o> . " +
                "<http://sttt> <http://n> <http://p> . " +
                "<http://sttt> <http://n4> <http://p> . ";
        data.read(new ByteArrayInputStream(s.getBytes()), null, "TTL");

        List<Triple> queryTriples = new ArrayList<>();
        List<Triple> queryGroundedTriples = new ArrayList<>();
        Map<Var, Object> literalBindings = new HashMap<>();
        processQuery(query, queryTriples, queryGroundedTriples, literalBindings);
        var matchQuery = createMatchQuery(queryTriples);

        queryGroundedTriples.forEach(System.out::println);

        System.out.println(literalBindings);
        System.out.println(match(matchQuery, data, queryGroundedTriples, literalBindings));
    }

    private static final PathCompiler pathCompiler = new PathCompiler();

    public static void processQuery(Query query, List<Triple> queryTriples, List<Triple> queryGroundedTriples, Map<Var, Object> literalBindings) {
        Map<Node, Var> anonVarReplacements = new HashMap<>();
        getQueryPathBlocks(query)
                .flatMap(pathBlock -> pathCompiler.reduce(pathBlock).getList().stream())
                .map(TriplePath::asTriple)
                .forEach(triple -> {
                    triple = removeAnonVars(triple, anonVarReplacements);
                    triple = replaceLiteral(triple, literalBindings);
                    queryTriples.add(triple);
                    queryGroundedTriples.add(new Triple(
                            groundVar(triple.getSubject()),
                            groundVar(triple.getPredicate()),
                            groundVar(triple.getObject())
                            ));
                });
    }

    public static Optional<QueryMatchResult> match(Query query, Model data, List<Triple> queryGroundedTriples, Map<Var, Object> queryLiteralVars) {
        List<QueryMatchResult> results = new ArrayList<>();
        var resultSet = QueryExecutionFactory.create(query, data).execSelect();
        int nSolutionsPolled = 0;
        pollSolutions: while (resultSet.hasNext()) {
            if (++nSolutionsPolled > MAX_SOLUTIONS_POLLED) {
                break;
            }
            Map<Node, Set<Var>> bindings = new HashMap<>();
            var solution = resultSet.next();
            var varNames = solution.varNames();
            while (varNames.hasNext()) {
                var varName = varNames.next();
                bindings.compute(solution.get(varName).asNode(), (k, v) -> {
                    if (v == null) {
                        v = new HashSet<>();
                    }
                    v.add(Var.alloc(varName));
                    return v;
                });
            }
            for (var binding : deduplicateBindings(bindings)) {
                var alignedData = substituteVars(data, binding);
                var unmatched = new HashSet<>(queryGroundedTriples);
                var resultCoverage = coverage(alignedData, queryGroundedTriples, unmatched);
                var boundLiterals = bindLiterals(binding, queryLiteralVars);
                results.add(new QueryMatchResult(resultCoverage, binding, alignedData, unmatched, boundLiterals));
                if (results.size() >= MAX_RESULTS_ADDED) {
                    break pollSolutions;
                }
            }
        }
        return results.stream()
                .filter(result -> result.coverage > 0.0)
                .max(Comparator.comparingDouble(QueryMatchResult::getCoverage));
    }

    private static List<Map<Node, Var>> deduplicateBindings(Map<Node, Set<Var>> bindings) {
        List<Map<Node, Var>> results = new ArrayList<>();
        List<List<Var>> vars = bindings.values().stream().map(ArrayList::new).collect(Collectors.toList());
        for (var deduplicated : cartesianProduct(vars)) {
            Map<Node, Var> result = new HashMap<>();
            results.add(result);
            int i = 0;
            for (var v : bindings.keySet()) {
                result.put(v, deduplicated.get(i));
                i++;
            }
        }
        return results;
    }

    private static  <T> List<List<T>> cartesianProduct(List<List<T>> lists) {
        List<List<T>> resultLists = new ArrayList<>();
        if (lists.size() == 0) {
            resultLists.add(new ArrayList<T>());
            return resultLists;
        } else {
            List<T> firstList = lists.get(0);
            List<List<T>> remainingLists = cartesianProduct(lists.subList(1, lists.size()));
            for (T condition : firstList) {
                for (List<T> remainingList : remainingLists) {
                    ArrayList<T> resultList = new ArrayList<T>();
                    resultList.add(condition);
                    resultList.addAll(remainingList);
                    resultLists.add(resultList);
                }
            }
        }
        return resultLists;
    }

    private static Map<Node, Object> bindLiterals(Map<Node, Var> dataBindings, Map<Var, Object> literalBindings) {
        Map<Node, Object> boundLiterals = new HashMap<>();
        dataBindings.forEach((dataNode, var) -> {
            var literal = literalBindings.get(var);
            if (literal != null) {
                boundLiterals.put(dataNode, literal);
            }
        });
        return boundLiterals;
    }

    public static Graph substituteVars(Model m, Map<Node, Var> bindings) {
        Graph g = GraphFactory.createDefaultGraph();
        m.listStatements()
                .mapWith(s -> substituteVars(s.asTriple(), bindings))
                .forEach(g::add);
        return g;
    }

    public static Triple substituteVars(Triple t, Map<Node, Var> bindings) {
        return new Triple(
                substituteVars(t.getSubject(), bindings),
                substituteVars(t.getPredicate(), bindings),
                substituteVars(t.getObject(), bindings));
    }

    public static Node substituteVars(Node n, Map<Node, Var> bindings) {
        var varNode = bindings.get(n);
        if (varNode != null) {
            return groundVar(varNode);
        }
        return n;
    }

    public static Node groundVar(Node n) {
        if (n.isVariable()) {
            return ResourceFactory.createResource(VAR_NS + n.getName()).asNode();
        }
        return n;
    }

    public static double coverage(Graph data, List<Triple> groundedQueryTriples, Set<Triple> unmatched) {
        int i = 0;
        for (var triple : groundedQueryTriples) {
            var iter = data.find(triple);
            if (iter.hasNext()) {
                i++;
                unmatched.remove(triple);
            }
        }
        return (double) i / groundedQueryTriples.size();
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

    public static Triple removeAnonVars(Triple t, Map<Node, Var> anonVarReplacements) {
        Node s = t.getSubject();
        Node p = t.getPredicate();
        Node o = t.getObject();

        Node s1 = removeAnonVars(s, anonVarReplacements);
        Node p1 = removeAnonVars(p, anonVarReplacements);
        Node o1 = removeAnonVars(o, anonVarReplacements);

        if (s1 != s || p1 != p || o1 != o) {
            return new Triple(s1, p1, o1);
        }

        return t;
    }

    public static Node removeAnonVars(Node n, Map<Node, Var> anonVarReplacements) {
        if (n.isVariable() && n.getName().startsWith(ARQConstants.allocVarAnonMarker)) {
            var existingReplacement = anonVarReplacements.get(n);
            if (existingReplacement != null) {
                return existingReplacement;
            }
            var var = allocVar();
            anonVarReplacements.put(n, var);
            return var;
        }
        return n;
    }

    public static Triple replaceLiteral(Triple t, Map<Var, Object> literalBindings) {
        Node o = t.getObject();

        if (o.isLiteral()) {
            var literalValue = o.getLiteral().getValue();
            var var = literalBindings.entrySet().stream()
                    .filter(e -> e.getValue() != null && e.getValue().equals(literalValue))
                    .findAny()
                    .map(Map.Entry::getKey)
                    .orElseGet(QueryMatch::allocVar);
            literalBindings.put(var, literalValue);
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
        private Map<Node, Var> bindings;
        private Graph alignedData;
        private Set<Triple> unmatched;
        Map<Node, Object> boundLiterals;
    }

}
