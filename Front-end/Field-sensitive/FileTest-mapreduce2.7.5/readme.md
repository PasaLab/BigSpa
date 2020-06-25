# Instructions for running the code:

1. Generate intragraph. The program entry is in ```edu.zuo.setree.client.IntraMain.java```. The generated graph can be found in ```intraOutput```. There are in total 5 files, namely ```conditionalSmt2, consEdgeGraph, set.conditional, stateNode.json, var2indexMap```.
2. Generate intergraph. The program entry is in ```edu.zuo.setree.intergraph.interGraph.java```. The generated graph can be found in ```interOutput```. There are in total 5 files, namely ```func2indexMap.txt, index2varMap.txt, interGraph.txt, interSmt2.txt, pair2indexMap.txt```
3. Generate finalGraphFile and callinfoFile. The program entry is in ```intraFormat.java``` of default package. Intermediate files ```tempGraphFile-mapreduce.txt``` and ```forReturnFile-mapreduce.txt``` are generated during the generation process.
4. Run the code in ```GaraphInline```to inline the ```finalGraphFile``` and ```callinfoFile```, to get ```final-mapreduce```.
5. Format the ```final-mapreduce``` file. The program entry is in ```Final.java``` of default package.

Finally, we can get the graph file ```final-mapreduce-new```.

The order of running the jar package is ```intragraph.jar->intergraph.jar->formatgraph.jar->inlineNew.jar->final.jar```.
