1. flink jobmanager报错

   ```tex
   Caused by: java.lang.NoSuchMethodError: scala.Product.$init$(Lscala/Product;)V
           at org.example.bean.SensorReading.<init>(SensorReading.scala:3) ~[?:?]
           at org.example.source.SensorSource.$anonfun$run$3(SensorSource.scala:21) ~[?:?]
           at org.example.source.SensorSource.$anonfun$run$3$adapted(SensorSource.scala:21) ~[?:?]
           at scala.collection.Iterator$class.foreach(Iterator.scala:891) ~[flink-dist_2.11-1.11.0.jar:1.11.0]
           at scala.collection.AbstractIterator.foreach(Iterator.scala:1334) ~[flink-dist_2.11-1.11.0.jar:1.11.0]
           at scala.collection.IterableLike$class.foreach(IterableLike.scala:72) ~[flink-dist_2.11-1.11.0.jar:1.11.0]
           at scala.collection.AbstractIterable.foreach(Iterable.scala:54) ~[flink-dist_2.11-1.11.0.jar:1.11.0]
           at org.example.source.SensorSource.run(SensorSource.scala:21) ~[?:?]
           at org.apache.flink.streaming.api.operators.StreamSource.run(StreamSource.java:100) ~[flink-dist_2.11-1.11.0.jar:1.11.0]
           at org.apache.flink.streaming.api.operators.StreamSource.run(StreamSource.java:63) ~[flink-dist_2.11-1.11.0.jar:1.11.0]
   ```

   原因分析：

   ​	flink打包使用的scala版本是2.12.18，但是flink-1.11.0的lib目录下的jar包的scala版本是2.11，scala版本不匹配。

   解决方案：

   ​	将maven依赖中的scala版本修改为2.11

2. link wordcount error: No implicits found for parameter evidence$11: TypeInformation[S...

```
import org.apache.flink.streaming.api.scala._
```