rm ~/tcc/dataset/out
rm ~/tcc/dataset/out1
rm ~/tcc/dataset/out2
rm ~/tcc/dataset/count

mvn clean install -Pbuild-jar -DskipTests
#bash /home/omcarvalho/devops/flink/flink-0.9.1/build-target/bin/flink run target/energy_stream-0.1.jar -p 1
bash /home/omcarvalho/devops/flink/flink/build-target/bin/flink run target/energy_stream-0.1.jar -p 1
