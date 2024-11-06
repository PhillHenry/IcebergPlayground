# IcebergPlayground
Katas for Apache Iceberg etc

# Creating the logs
You'll need to install `ansi2html`. On Ubuntu, you'd do it with something like:

`sudo apt install colorized-logs`

and run with the scenario_docs profile, eg:

`mvn clean install -Pscenario_docs`

See the docs [here](https://phillhenry.github.io/IcebergPlayground/index.html).

# See the BDDs

You can see the output of the BDDs [here](https://iceberg.thebigdata.space/)

# Individual tests

They can be run with something like 

`docker stop $(docker ps | grep polaris | awk '{print $1}') ; mvn  -Dtest=ConcurrentWriteSpec test`