import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class Transform(beam.DoFn):
    def process(self, element):
        yield element

def run():
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadFromBQ" >> beam.io.ReadFromBigQuery(
                table="project:dataset.table"
            )
            | "Transform" >> beam.ParDo(Transform())
            | "WriteToMongo" >> beam.io.WriteToMongoDB(
                uri="mongodb+srv://<username>:<password>@cluster.mongodb.net",
                db="test_db",
                collection="test_collection"
            )
        )

if __name__ == "__main__":
    run()
