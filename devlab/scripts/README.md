## Start our Lakehouse Tiering tasks.

The following 2 scripts will start our Apache Flink Jobs responsible for our data tiering from our Apache Fluss (incubating) tables into our Lakehouse target tier.

I've chosen to use Apache Paimon as my Lakehouse target for this Blog, storage is provided using a MinIO S3 container.

The Apache Flink, Fluss tiering jobs are executed using:

(from within <project root>/devlab directory):

- `make jm`
- `cd /scripts`
- execute `tier0.sh`

