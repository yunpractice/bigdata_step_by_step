使用的hadoop版本2.8.5

# 需要的jar库文件

$HADOOP_HOME/share/hadoop/common/hadoop-common-2.8.5.jar
$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.8.5.jar
$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar

# 编译设置的参数

-classpath，设置源代码里使用的各种类库所在的路径，多个路径用":"隔开。
-d，设置编译后的 class 文件保存的路径。

src/*.java，待编译的源文件。

# 编译指令：

javac -d bin -classpath xxx src/*.java

# 打包指令：

cd bin
jar -cvf WordCount.jar .

# 将数据放入hdfs

hadoop fs -put input /hbase

# 执行

hadoop jar WordCount.jar WordCount /hbase/input /hbase/output
