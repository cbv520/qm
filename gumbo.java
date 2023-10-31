package mux41;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.stream.Collectors;

public class Gumbo2 {

    public static void main(String[] args) {
        var query = model(
                "<http://cat> <http://p> <http://loc> .\n" +
                "<http://ship> <http://p> <http://loc> .\n" +
                "<http://ship> <http://p> <http://type> .\n"
        );

        var queryTriples = query.listStatements()
                .toList().stream()
                .map(Statement::asTriple)
                .collect(Collectors.toList());

        var partials = List.of(
                new ServiceOutput("catLoc", model("<http://cat> <http://p> <http://loc>")),
                new ServiceOutput("personLoc", model("<http://person> <http://p> <http://loc>")),
                new ServiceOutput("phoneLoc", model("<http://phone> <http://p> <http://loc>")),
                new ServiceOutput("shipLoc", model("<http://ship> <http://p> <http://loc>")),
                new ServiceOutput("shipType", model("<http://ship> <http://p> <http://type>")),
                new ServiceOutput( "personType", model("<http://person> <http://p> <http://type>"))
        );

        var chosen = choosePartials(partials, queryTriples);

        System.out.println(chosen.stream().map(ServiceOutput::getName).collect(Collectors.toList()));
    }

    public static Set<ServiceOutput> choosePartials(List<ServiceOutput> partials, List<Triple> triples) {
        return triples.stream()
            .flatMap(triple -> {
                List<PartialSpan> partialsSpanForTriple = partials.stream()
                        .map(partial -> {
                            var dataGraph = partial.getDefinition().getGraph();
                            if (dataGraph.contains(triple.getSubject(), Node.ANY, Node.ANY) ||
                                    dataGraph.contains(Node.ANY, Node.ANY, triple.getObject())) {
                                var span = traverse(triple.getSubject(), dataGraph) + traverse(triple.getObject(), dataGraph);
                                return new PartialSpan(partial, span);
                            } else {
                                return new PartialSpan(partial, 0);
                            }
                        })
                        .collect(Collectors.toList());
                int maxSpan = partialsSpanForTriple.stream()
                        .map(PartialSpan::getSpan)
                        .max(Comparator.comparingInt(i -> i))
                        .orElse(0);
                return partialsSpanForTriple.stream()
                        .filter(h -> h.getSpan() == maxSpan);
            })
            .map(PartialSpan::getPartial)
            .collect(Collectors.toSet());
    }

    @Data
    @AllArgsConstructor
    public static class ServiceOutput {
        private String name;
        private Model definition;
    }

    @Data
    @AllArgsConstructor
    public static class PartialSpan {
        private ServiceOutput partial;
        private int span;
    }

    private static int traverse(Node subject, Graph dataGraph) {
        return dataGraph.find(subject, Node.ANY, Node.ANY).toList().size() +
               dataGraph.find(Node.ANY, Node.ANY, subject).toList().size();
    }

    public static Model model(String str) {
        var model = ModelFactory.createDefaultModel();
        model.read(new ByteArrayInputStream(str.getBytes()), null, "TTL");
        return model;
    }
}
