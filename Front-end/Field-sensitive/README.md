# Generate graph for field-sensitive analysis

Overall description:

1. ```FileTest-hdfs2.0.3``` generates intragraph of ```hdfs```
2. ```FileTest-mapreduce2.7.5``` generates intragraph of ```mapreduce```
3. ```GraphInline``` inlines the intragraphs

Detailed description can be found in README in each directory.

Tips:
1. ```soot``` was installed as a plugin of Eclipse. Its version was ca.mcgill.sable.soot.lib_2.5.2。
2. The ```.class``` files of ```hdfs``` can be found in ```hdfs.zip```.
3. The ```.class``` files of ```mapreduce``` can be found in ```mapreduce.zip```.
```hadoop.zip``` contains the library class files when analyzing ```mapreduce``` source code.
4. The callgraphs (calling relationship between functions) can be found in ```callGraph-hdfs.txt``` and ```callGraph-mapreduce.txt```.
5. The java version was jdk_1.7.0_80