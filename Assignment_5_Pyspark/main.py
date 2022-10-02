import findspark
findspark.init()

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('myapp') \
    .getOrCreate()

def main():

    print(f'Spark Version:, {spark.version}')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()


