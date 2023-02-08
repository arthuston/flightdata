# FoodEnforcementReport

Food Enforcement Report take-home for Invitae Interview.

## Overview

[Data_Engineering_Homework_Assignment_(6).pdf](./Data_Engineering_Homework_Assignment_(6).pdf)

## Instructions

This project is created in IntelliJ Idea on Windows using the Scala Plugin.

These steps describe running the program using IntelliJ Idea on Windows using the Scala Plugin. The program can also be run from the command-line.

Note: This program uses Spark, winutils.exe must be downloaded to run on Windows and HADOOP_HOME must be set.

1. Download this project from http://github.com/arthuston
2. [download IntelliJ](https://www.jetbrains.com/idea/download) (Community Edition is fine).
3. Download [spark winutils.exe program](https://github.com/steveloughran/winutils)
4. Launch IntelliJ and install the Scala plugin as described [here](https://www.jetbrains.com/help/idea/discover-intellij-idea-for-scala.html)
5. Set the HADOOP_HOME environment variable to one of the hadoop directories from the previous step, for example hadoop-3.0.0
6. Launch IntelliJ and load the project.
7. Select Run -> 'FoodEnhancementReport'
8. The program runs in the IntelliJ console, for example:

```
"C:\Program Files\Java\jdk1.8.0_2[README.md](README.md)81\bin\java.exe" "-javaagent:C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2022.3.1\lib\idea_rt.jar=63462:C:\Program Files\JetBrains\IntelliJ IDEA Community Edition 2022.3.1\bin" -Dfile.encoding=UTF-8 -classpath "C:\Program Files\Java\jdk1.8.0_281\jre\lib\charsets.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\deploy.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\access-bridge-64.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\cldrdata.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\dnsns.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\jaccess.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\jfxrt.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\localedata.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\nashorn.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\sunec.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\sunjce_provider.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\sunmscapi.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\sunpkcs11.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\zipfs.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\javaws.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\jce.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\jfr.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\jfxswt.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\jsse.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\management-agent.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\plugin.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\resources.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\rt.jar;C:\Users\art\OneDrive\Documents\@Career\@JobHunt\Companies\Invitae\Project\FoodEnforcementReport\target\scala-2.12\classes;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\clearspring\analytics\stream\2.9.6\stream-2.9.6.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\esotericsoftware\kryo-shaded\4.0.2\kryo-shaded-4.0.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\esotericsoftware\minlog\1.3.0\minlog-1.3.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\core\jackson-annotations\2.12.3\jackson-annotations-2.12.3.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\core\jackson-core\2.12.3\jackson-core-2.12.3.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\core\jackson-databind\2.12.3\jackson-databind-2.12.3.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\datatype\jackson-datatype-jdk8\2.11.4\jackson-datatype-jdk8-2.11.4.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\datatype\jackson-datatype-jsr310\2.11.4\jackson-datatype-jsr310-2.11.4.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\fasterxml\jackson\module\jackson-module-scala_2.12\2.12.3\jackson-module-scala_2.12-2.12.3.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\github\luben\zstd-jni\1.5.0-4\zstd-jni-1.5.0-4.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\github\nscala-time\nscala-time_2.12\2.32.0\nscala-time_2.12-2.32.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\code\findbugs\jsr305\3.0.2\jsr305-3.0.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\code\gson\gson\2.8.6\gson-2.8.6.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\crypto\tink\tink\1.6.0\tink-1.6.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\flatbuffers\flatbuffers-java\1.9.0\flatbuffers-java-1.9.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\guava\guava\16.0.1\guava-16.0.1.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\google\protobuf\protobuf-java\3.14.0\protobuf-java-3.14.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\lihaoyi\geny_2.12\1.0.0\geny_2.12-1.0.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\lihaoyi\requests_2.12\0.8.0\requests_2.12-0.8.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\ning\compress-lzf\1.0.3\compress-lzf-1.0.3.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\thoughtworks\paranamer\paranamer\2.8\paranamer-2.8.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\twitter\chill-java\0.10.0\chill-java-0.10.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\twitter\chill_2.12\0.10.0\chill_2.12-0.10.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\typesafe\play\play-functional_2.12\2.9.4\play-functional_2.12-2.9.4.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\typesafe\play\play-json_2.12\2.9.4\play-json_2.12-2.9.4.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\univocity\univocity-parsers\2.9.1\univocity-parsers-2.9.1.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\commons-codec\commons-codec\1.15\commons-codec-1.15.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\commons-collections\commons-collections\3.2.2\commons-collections-3.2.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\commons-io\commons-io\2.8.0\commons-io-2.8.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\commons-lang\commons-lang\2.6\commons-lang-2.6.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\commons-logging\commons-logging\1.1.3\commons-logging-1.1.3.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\commons-net\commons-net\3.1\commons-net-3.1.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\airlift\aircompressor\0.21\aircompressor-0.21.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\dropwizard\metrics\metrics-core\4.2.0\metrics-core-4.2.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\dropwizard\metrics\metrics-graphite\4.2.0\metrics-graphite-4.2.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\dropwizard\metrics\metrics-jmx\4.2.0\metrics-jmx-4.2.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\dropwizard\metrics\metrics-json\4.2.0\metrics-json-4.2.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\dropwizard\metrics\metrics-jvm\4.2.0\metrics-jvm-4.2.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-all\4.1.68.Final\netty-all-4.1.68.Final.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-buffer\4.1.50.Final\netty-buffer-4.1.50.Final.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-codec\4.1.50.Final\netty-codec-4.1.50.Final.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-common\4.1.50.Final\netty-common-4.1.50.Final.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-handler\4.1.50.Final\netty-handler-4.1.50.Final.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-resolver\4.1.50.Final\netty-resolver-4.1.50.Final.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-transport-native-epoll\4.1.50.Final\netty-transport-native-epoll-4.1.50.Final.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-transport-native-unix-common\4.1.50.Final\netty-transport-native-unix-common-4.1.50.Final.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\io\netty\netty-transport\4.1.50.Final\netty-transport-4.1.50.Final.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\jakarta\annotation\jakarta.annotation-api\1.3.5\jakarta.annotation-api-1.3.5.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\jakarta\servlet\jakarta.servlet-api\4.0.3\jakarta.servlet-api-4.0.3.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\jakarta\validation\jakarta.validation-api\2.0.2\jakarta.validation-api-2.0.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\jakarta\ws\rs\jakarta.ws.rs-api\2.1.6\jakarta.ws.rs-api-2.1.6.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\javax\activation\activation\1.1.1\activation-1.1.1.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\javax\annotation\javax.annotation-api\1.3.2\javax.annotation-api-1.3.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\javax\xml\bind\jaxb-api\2.2.11\jaxb-api-2.2.11.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\joda-time\joda-time\2.11.0\joda-time-2.11.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\log4j\log4j\1.2.17\log4j-1.2.17.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\net\razorvine\pyrolite\4.30\pyrolite-4.30.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\net\sf\py4j\py4j\0.10.9.5\py4j-0.10.9.5.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\antlr\antlr4-runtime\4.8\antlr4-runtime-4.8.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\arrow\arrow-format\2.0.0\arrow-format-2.0.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\arrow\arrow-memory-core\2.0.0\arrow-memory-core-2.0.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\arrow\arrow-memory-netty\2.0.0\arrow-memory-netty-2.0.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\arrow\arrow-vector\2.0.0\arrow-vector-2.0.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\avro\avro-ipc\1.10.2\avro-ipc-1.10.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\avro\avro-mapred\1.10.2\avro-mapred-1.10.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\avro\avro\1.10.2\avro-1.10.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-compress\1.20\commons-compress-1.20.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-crypto\1.1.0\commons-crypto-1.1.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-lang3\3.12.0\commons-lang3-3.12.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-math3\3.4.1\commons-math3-3.4.1.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\commons\commons-text\1.6\commons-text-1.6.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\curator\curator-client\2.13.0\curator-client-2.13.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\curator\curator-framework\2.13.0\curator-framework-2.13.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\curator\curator-recipes\2.13.0\curator-recipes-2.13.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-client-api\3.3.1\hadoop-client-api-3.3.1.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hadoop\hadoop-client-runtime\3.3.1\hadoop-client-runtime-3.3.1.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\hive\hive-storage-api\2.7.2\hive-storage-api-2.7.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\htrace\htrace-core4\4.1.0-incubating\htrace-core4-4.1.0-incubating.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\ivy\ivy\2.5.0\ivy-2.5.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\orc\orc-core\1.6.14\orc-core-1.6.14.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\orc\orc-mapreduce\1.6.14\orc-mapreduce-1.6.14.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\orc\orc-shims\1.6.14\orc-shims-1.6.14.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-column\1.12.2\parquet-column-1.12.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-common\1.12.2\parquet-common-1.12.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-encoding\1.12.2\parquet-encoding-1.12.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-format-structures\1.12.2\parquet-format-structures-1.12.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-hadoop\1.12.2\parquet-hadoop-1.12.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\parquet\parquet-jackson\1.12.2\parquet-jackson-1.12.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-catalyst_2.12\3.2.2\spark-catalyst_2.12-3.2.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-core_2.12\3.2.2\spark-core_2.12-3.2.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-kvstore_2.12\3.2.2\spark-kvstore_2.12-3.2.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-launcher_2.12\3.2.2\spark-launcher_2.12-3.2.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-network-common_2.12\3.2.2\spark-network-common_2.12-3.2.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-network-shuffle_2.12\3.2.2\spark-network-shuffle_2.12-3.2.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-sketch_2.12\3.2.2\spark-sketch_2.12-3.2.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-sql_2.12\3.2.2\spark-sql_2.12-3.2.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-tags_2.12\3.2.2\spark-tags_2.12-3.2.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\spark\spark-unsafe_2.12\3.2.2\spark-unsafe_2.12-3.2.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\xbean\xbean-asm9-shaded\4.20\xbean-asm9-shaded-4.20.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\yetus\audience-annotations\0.12.0\audience-annotations-0.12.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\zookeeper\zookeeper-jute\3.6.2\zookeeper-jute-3.6.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\apache\zookeeper\zookeeper\3.6.2\zookeeper-3.6.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\codehaus\janino\commons-compiler\3.0.16\commons-compiler-3.0.16.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\codehaus\janino\janino\3.0.16\janino-3.0.16.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\fusesource\leveldbjni\leveldbjni-all\1.8\leveldbjni-all-1.8.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\external\aopalliance-repackaged\2.6.1\aopalliance-repackaged-2.6.1.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\external\jakarta.inject\2.6.1\jakarta.inject-2.6.1.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\hk2-api\2.6.1\hk2-api-2.6.1.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\hk2-locator\2.6.1\hk2-locator-2.6.1.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\hk2-utils\2.6.1\hk2-utils-2.6.1.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\hk2\osgi-resource-locator\1.0.3\osgi-resource-locator-1.0.3.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\containers\jersey-container-servlet-core\2.34\jersey-container-servlet-core-2.34.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\containers\jersey-container-servlet\2.34\jersey-container-servlet-2.34.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\core\jersey-client\2.34\jersey-client-2.34.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\core\jersey-common\2.34\jersey-common-2.34.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\core\jersey-server\2.34\jersey-server-2.34.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\glassfish\jersey\inject\jersey-hk2\2.34\jersey-hk2-2.34.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\javassist\javassist\3.25.0-GA\javassist-3.25.0-GA.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\jetbrains\annotations\17.0.0\annotations-17.0.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\joda\joda-convert\2.2.2\joda-convert-2.2.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\json4s\json4s-ast_2.12\3.7.0-M11\json4s-ast_2.12-3.7.0-M11.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\json4s\json4s-core_2.12\3.7.0-M11\json4s-core_2.12-3.7.0-M11.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\json4s\json4s-jackson_2.12\3.7.0-M11\json4s-jackson_2.12-3.7.0-M11.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\json4s\json4s-scalap_2.12\3.7.0-M11\json4s-scalap_2.12-3.7.0-M11.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\lz4\lz4-java\1.7.1\lz4-java-1.7.1.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\objenesis\objenesis\2.5.1\objenesis-2.5.1.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\roaringbitmap\RoaringBitmap\0.9.0\RoaringBitmap-0.9.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\roaringbitmap\shims\0.9.0\shims-0.9.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\rocksdb\rocksdbjni\6.20.3\rocksdbjni-6.20.3.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\modules\scala-parser-combinators_2.12\1.1.2\scala-parser-combinators_2.12-1.1.2.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\modules\scala-xml_2.12\1.2.0\scala-xml_2.12-1.2.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-library\2.12.10\scala-library-2.12.10.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\scala-lang\scala-reflect\2.12.10\scala-reflect-2.12.10.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\slf4j\jcl-over-slf4j\1.7.30\jcl-over-slf4j-1.7.30.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\slf4j\jul-to-slf4j\1.7.30\jul-to-slf4j-1.7.30.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\slf4j\slf4j-api\1.7.30\slf4j-api-1.7.30.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\slf4j\slf4j-log4j12\1.7.30\slf4j-log4j12-1.7.30.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\spark-project\spark\unused\1.0.0\unused-1.0.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\threeten\threeten-extra\1.5.0\threeten-extra-1.5.0.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\tukaani\xz\1.8\xz-1.8.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\org\xerial\snappy\snappy-java\1.1.8.4\snappy-java-1.1.8.4.jar;C:\Users\art\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\oro\oro\2.0.8\oro-2.0.8.jar" com.arthuston.invitae.project.food.enforcement.FoodEnforcementReport
19:10:42 WARN  NativeCodeLoader:60 - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
19:11:00 WARN  ProcfsMetricsGetter:69 - Exception when trying to compute pagesize, as a result reporting of ProcessTree metrics is stopped
19:11:00 WARN  ProcfsMetricsGetter:69 - Exception when trying to compute pagesize, as a result reporting of ProcessTree metrics is stopped
1. Top States with Class III Hazard
   State CA/Status Terminated: 211
   State TX/Status Terminated: 132
   State WA/Status Terminated: 120
   State IA/Status Terminated: 117
   State MA/Status Terminated: 86
   State NY/Status Terminated: 76
   State NJ/Status Terminated: 53
   State OR/Status Terminated: 44
   State UT/Status Terminated: 37
   State PR/Status Terminated: 34
---
2. Average reports per month in 2016
   251 reports
---
3. Top States for 2017
   IL: 468
   CA: 291
   FL: 245
   NY: 227
   NH: 210
   PA: 188
   WA: 156
   TX: 143
   MA: 134
   OH: 123
---
4. Top Years
   highest year is 2017 with 3203 reports
   lowest year is 2023 with 111 reports

Process finished with exit code 0
```

# Design

* [FoodEnforcementAPI.scala](./src/main/scala/com/arthuston/invitae/project/food/enforcement/FoodEnforcementAPI.scala) is a thin wrapper around the FDA Food Enforcement API that provides an API to read all the data by paging through multiple calls to the API.
* [FoodEnforcementData.scala](./src/main/scala/com/arthuston/invitae/project/food/enforcement/FoodEnforcementData.scala) contains case classes to unmarshall the FDA Food Enforcement API.
* [FoodEnforcementCalculator.scala](./src/main/scala/com/arthuston/invitae/project/food/enforcement/FoodEnforcementCalculator.scala) uses Scala Spark to munge data output for each of the calculations.
* [FoodEnforcementReport.scala](./src/main/scala/com/arthuston/invitae/project/food/enforcement/FoodEnforcementReport.scala) is the main program.

# Processing

1. FoodEnforcementReport creates a SparkSession
2. After creating the SparkSession, FoodEnforcementReport reads all the data available in the API using FoodEnforcementAPI.get into a FoodEnforcementData case class which contains the meta-data and results. As noted below, this requires multiple calls to the API which is restricted to 1000 records per call. As noted below, adding query parameters would eliminate the need to read all the data.
3. After reading the data, the FoodEnforcementReport loads the Seq into a Dataset for processing.
4. FoodEnforcementReport runs each spark calculation in FoodEnforcementCalculator and outputs the result to the console.
5. FoodEnforcementReport ends the spark session.

# Limitations

* The high year total will print only one value instead of detecting years with the same number of records.
* The low year total will print only one value instead of detecting years with the same number of records.
* The low year total will not print a year unless it has at least one report in it. To fix this, the program should be enhanced to find the minimum and maximum year in the data, and use one of the years with zero entries in the range as the lowest year using a convention for picking the year (e.g. the oldest year or the most recent year).

# Enhancements

Enhance FoodEnforcementAPI to fully support the APi, or pass query parameters directly, and pre-filter results before using Spark to do any data munging to improve performance.

# Questions or Comments?

Contact [art@arthuston.com](mailto:art@arthuston.com) or call 978 460-5002 with any questions or comments.
