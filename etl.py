from datetime import timedelta
from prefect.engine.executors import DaskExecutor
from prefect import Flow
from prefect.schedules import IntervalSchedule
from tasks import *


schedule = IntervalSchedule(interval=timedelta(hours=24))

bucket_name = "etl-demo-fractal"
source_file_name = "./enade2019.csv"
destination_blob_name = "enade2019-blob"

def main():
  with Flow('enade-flow', schedule) as flow:
    path = get_raw_data()

    filters = apply_filters(path)
    estcivil = transform_estcivil(filters)
    cor = transform_cor(filters)
    escopai = transform_escopai(filters)
    escomae = transform_escomae(filters)
    renda = transform_renda(filters)

    joined_data = join_data(filters, estcivil, cor, escopai, escomae, renda)
    write = write_csv(joined_data)
    end = upload_blob(bucket_name, source_file_name, destination_blob_name)

    flow.set_dependencies(
    task=end,
    upstream_tasks=[write])

  flow.register(project_name='Enade', idempotency_key=flow.serialized_hash())


if __name__ == "__main__":
  main()