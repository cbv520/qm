package mux41;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.ARQConstants;
import org.apache.jena.sparql.core.PathBlock;
import org.apache.jena.sparql.core.TriplePath;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.path.PathCompiler;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementOptional;
import org.apache.jena.sparql.syntax.ElementPathBlock;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryMatch {

    public static void main(String[] args) {
        s(QueryFactory.create("SELECT * {" +
                "?s ^<http://a>/<http://b>/<http://c> ?o. " +
                "Filter NOT EXISTS {?d ?n ?e. ?d2 ?n2 ?e2} " +
                "?s ?b ?dd " +
                "MINUS {?m ?i ?n. ?min1 ?min2 ?min3}" +
                "}"));
    }

    public static void s(Query query) {
        var m = ModelFactory.createDefaultModel();
        var s = "<http://s> <http://a> <http://x> . " +
                "<http://x> <http://b> <http://y> . " +
                "<http://y> <http://c> <http://o> . ";
        m.read(new ByteArrayInputStream(s.getBytes()), null, "TTL");
//       // QueryExecutionFactory.create(query, m).execSelect();
//        System.out.println(getQueryTriples(query));
//
//        Query q = QueryFactory.create("SELECT * {?s ?p ?o}");
//        var g = ((ElementGroup)q.getQueryPattern()).getElements();
//        g.clear();
//        var h = new ElementPathBlock();
//        getQueryTriples(query).forEach(h::addTriple);
//        g.add(h);
//        System.out.println(q);
//        System.out.println(createMatchQuery(getQueryTriples(query)));

        var fuzzy = createMatchQuery(getQueryTriples(query));
        QueryExecutionFactory.create(fuzzy, m)
                .execSelect()
                .next();
        System.out.println(fuzzy);
    }

    private static final PathCompiler pathCompiler = new PathCompiler();

    public static List<Triple> getQueryTriples(Query query) {
        return getQueryPathBlocks(query)
                .flatMap(pathBlock -> pathCompiler.reduce(pathBlock).getList().stream())
                .map(TriplePath::asTriple)
                .map(QueryMatch::removeAnonVars)
                .collect(Collectors.toList());
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
            return Var.alloc("var_" + UUID.randomUUID().toString().substring(24));
        }
        return n;
    }

}
