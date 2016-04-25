hadoop = hadoop

bases = LeftoverMapper LeftoverReducer NodeInputFormat Node NodeOrDouble NodeOutputFormat NodeRecordReader NodeRecordWriter TrustMapper TrustReducer PageRank
classDir = classes
sourceDir = src
HADOOP_PREFIX=/opt/
javaFiles = src/*.java
classFiles = classes/*.class

CLASS_PATH=$(HADOOP_PREFIX)hadoop-2.6.0/share/hadoop/common/*:$(HADOOP_PREFIX)hadoop-2.6.0/share/hadoop/yarn/lib/*:$(HADOOP_PREFIX)hadoop-2.6.0/share/hadoop/mapreduce/lib/*:$(HADOOP_PREFIX)hadoop-2.6.0/share/hadoop/mapreduce/*:./
j = -classpath $(CLASS_PATH) #Xlint:deprecation


default : $(javaFiles)
	mkdir $(classDir); javac $j -d $(classDir) $(javaFiles); jar cvf PageRank.jar $(classDir);echo $(classFiles); $(hadoop) jar PageRank.jar PageRank

clean : 
	rm -r $(classDir); rm -r stage*; rm PageRank.jar;



