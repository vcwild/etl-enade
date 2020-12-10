from datetime import timedelta
from prefect.tasks.gcp.storage import GCSUpload
from prefect.engine.executors import DaskExecutor
from prefect import Flow
from prefect.schedules import IntervalSchedule
from prefect.environments.storage import GCS
from tasks import *

# file url
url = "http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip"
file = "microdados_enade_2019.zip"

# GCP Settings
# storage = GCS(bucket="etl-demo-fractal", secrets=["GCP_CREDENTIALS"])
bucket_name = "etl-demo-fractal"
source_file_name = "./enade2019.csv"
destination_blob_name = "enade2019-blob"

# task objects
schedule = IntervalSchedule(interval=timedelta(hours=24))
download = ShellTask(
  name="curl_download", 
  max_retries=2, 
  retry_delay=timedelta(seconds=10)
)
upload = GCSUpload(
  bucket=bucket_name, 
  blob=destination_blob_name, 
  create_bucket=False, 
  max_retries=2, 
  retry_delay=timedelta(seconds=10), 
  task_run_name='upload_gcp',
  name='gcp_upload'
)

remove = ShellTask(
  name="shell_remove",
)

with Flow('enade-flow', schedule=schedule) as flow:
  command = curl_cmd(url, file)
  curl = download(command=command)
  path = unzip(file)
  filters = apply_filters(path)
  estcivil = transform_estcivil(filters)
  cor = transform_cor(filters)
  escopai = transform_escopai(filters)
  escomae = transform_escomae(filters)
  renda = transform_renda(filters)

  joined_data = join_data(filters, estcivil, cor, escopai, escomae, renda)
  write = write_csv(joined_data)
  # end = upload_blob(bucket_name, source_file_name, destination_blob_name)
  end = upload.run(source_file_name, content_encoding='utf-8')
  rm_command = del_cmd(source_file_name)
  delete = remove(command=rm_command)

  flow.set_dependencies(
  task=path,
  upstream_tasks=[curl])

  flow.set_dependencies(
  task=end,
  upstream_tasks=[write],
  downstream_tasks=[rm_command])

flow.register(project_name='Enade', idempotency_key=flow.serialized_hash())
