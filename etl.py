from datetime import timedelta
from prefect import Flow
from prefect.schedules import IntervalSchedule
from tasks import *


schedule = IntervalSchedule(interval=timedelta(minutes=5))

def main():
  with Flow('enade-flow', schedule) as flow:
    path = get_raw_data()
    filters = apply_filters(path)
    estcivil = clean_estcivil(filters)
    cor = clean_cor(filters)
    escopai = clean_escopai(filters)
    escomae = clean_escomae(filters)
    renda = clean_renda(filters)

    joined_data = join_data(filters, estcivil, cor, escopai, escomae, renda)
    end = write_csv(joined_data)


  flow.register(project_name='Enade', idempotency_key=flow.serialized_hash())


if __name__ == "__main__":
  main()