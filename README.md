# IcebergPlayground
Katas for Apache Iceberg etc

This project assumes we're running a Unix-like OS. 
I'll make it Windows friendly if there is a demand. 

# Running

The BDDs require Docker images for Kafka and Polaris. 
Because Polaris writes to a mounted volume, you'll need to pass it your user ID and GID.
So run all the tests with:

`mvn clean install -Ddocker.uid=$(id -u) -Ddocker.gid=$(id -g)`

# Creating the Documentation
You'll need to install `ansi2html`. On Ubuntu, you'd do it with something like:

`sudo apt install colorized-logs`

and run with the scenario_docs profile, eg:

`mvn clean install -Pscenario_docs -Ddocker.uid=$(id -u) -Ddocker.gid=$(id -g)`

See the docs [here](https://phillhenry.github.io/IcebergPlayground/index.html).

# See the BDDs

You can see the output of the BDDs [here](https://iceberg.thebigdata.space/)

# Individual tests

They can be run with something like 

`docker stop $(docker ps | grep polaris | awk '{print $1}') ; mvn  -Dtest=ConcurrentWriteSpec test  -Ddocker.uid=$(id -u) -Ddocker.gid=$(id -g)`

# Running Polaris

The BDDs extensively use Polaris. 
To run the container outside of the build process, execute:

`docker run -d -eRUN_USER_ID=$(id -u) -eRUN_GROUP_ID=$(id -g) -p8181:8181 -v/tmp:/tmp ph1ll1phenry/polaris_for_bdd:latest`

This is the recommended way if you want to run a test in your IDE.
