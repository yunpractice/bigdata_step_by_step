## 工作流程

1、Hadoop读取输入，以换行符为分隔符将其拆分为行，然后以每行作为参数运行map函数

2、map函数对行进行标记化处理，为每个标记（单词）输出一个键值对(word,1)

3、Hadoop收集所有的(word,1)对，按word对其进行排序，按每个不同的键对输出的值进行分组，以键及该键的所有值为参数对每个不同的键调用一次reduce

4、reduce函数使用这些值对每个单词出现的次数进行计数，并将结果以键值对的形式输出

5、Hadoop将最终的输出结果写到输出目录中

## 使用的hadoop版本2.8.5

## 需要的jar库文件

$HADOOP_HOME/share/hadoop/common/hadoop-common-2.8.5.jar

$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.8.5.jar

$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar

## 编译设置的参数

-classpath，设置源代码里使用的各种类库所在的路径，多个路径用":"隔开。

-d，设置编译后的 class 文件保存的路径。

src/*.java，待编译的源文件。

## 编译指令：

javac -d bin -classpath xxx src/*.java

## 打包指令：

cd bin

jar -cvf WordCount.jar .

## 将数据放入hdfs

hadoop fs -put input /hbase

## 执行

hadoop jar WordCount.jar WordCount /hbase/input /hbase/output




