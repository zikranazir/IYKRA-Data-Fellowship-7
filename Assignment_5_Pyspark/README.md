Make sure you have Java 8 or higher installed on your computer. Of course, you will also need Python (I recommend > Python 3.5 from Anaconda).

Now visit the Spark downloads page. Select the latest Spark release, a prebuilt package for Hadoop, and download it directly.

If you want Hive support or more fancy stuff you will have to build your spark distribution by your own -> Build Spark.

Unzip it and move it to your /opt folder:

`$ tar -xzf spark-2.3.0-bin-hadoop2.7.tgz
$ mv spark-2.3.0-bin-hadoop2.7 /opt/spark-2.3.0`

Create a symbolic link (this will let you have multiple spark versions):

`$ ln -s /opt/spark-2.3.0 /opt/spark̀`

Finally, tell your bash (or zsh, etc.) where to find spark. To do so, configure your $PATH variables by adding the following lines in your ~/.bashrc (or ~/.zshrc) file:

`export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH`

Now to run PySpark in Jupyter you’ll need to update the PySpark driver environment variables. Just add these lines to your ~/.bashrc (or ~/.zshrc) file:

`export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='notebook'`

Restart (our just source) your terminal and launch PySpark:

`$ pyspark`

## Running PySpark on your favorite IDE

Sometimes you need a full IDE to create more complex code, and PySpark isn’t on sys.path by default, but that doesn’t mean it can’t be used as a regular library. You can address this by adding PySpark to sys.path at runtime. The package findspark does that for you.

To install findspark just type:

`$ pip install findspark`

And then on your IDE (I use PyCharm) to initialize PySpark, just call:

`import findspark
findspark.init()
import pyspark
sc = pyspark.SparkContext(appName="myAppName")`
