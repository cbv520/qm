private static final int OPTIMIZATION_ITERATIONS = 100;
public static void findBestBinding(List<TriplePath> gtps, Model data, Map<Node, Set<Var>> bindings) {
    Map<Node, List<Var>> orderedBindings = bindings.entrySet().stream()
            .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> new ArrayList<>(e.getValue())
            ));
    Map<Node, Var> globalBinding = orderedBindings.entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> e.getValue().get(0)
    ));
    double globalCoverage = 0.0;
    Model globalData = null;
    Random r = new Random(1);
    int iters = 0;
    optimiseLoop: while (iters < OPTIMIZATION_ITERATIONS) {
        iters++;
        var annealingTemp = 1 - (double) iters / OPTIMIZATION_ITERATIONS;
        for (var node : orderedBindings.keySet()) {
            var bindingsForNode = orderedBindings.get(node);
            if (annealingTemp < r.nextDouble()) {
                globalBinding.put(node, bindingsForNode.get(r.nextInt(bindingsForNode.size())));
                globalData = alignData(data, globalBinding);
                globalCoverage = coverage(gtps, createExecutionContext(globalData));
                continue;
            }
            double best = -1.0;
            int bestIdx = 0;
            int i = 0;
            for (var f : bindingsForNode) {
                var binding = new HashMap<>(globalBinding);
                binding.put(node, f);
                var alignedData = alignData(data, binding);
                var ctx = createExecutionContext(alignedData);
                var cvg = coverage(gtps, ctx);
                if (cvg > best) {
                    best = cvg;
                    bestIdx = i;
                    if (cvg > globalCoverage) {
                        globalCoverage = cvg;
                        globalData = alignedData;
                        if (globalCoverage == 1.0) {
                            break optimiseLoop;
                        }
                    }
                }
                i++;
            }
            globalBinding.put(node, orderedBindings.get(node).get(bestIdx));
        }
    }
    System.out.println(globalData);
}
