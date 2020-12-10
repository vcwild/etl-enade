from prefect import task
import prefect
import pandas as pd
import os
from os import system
from google.cloud import storage
from zipfile import ZipFile
from prefect.engine.signals import SKIP
from prefect.tasks.shell import ShellTask


@task
def curl_cmd(url: str, fname: str) -> str:
    """
    The curl command we wish to execute.
    """
    if os.path.exists(fname):
        raise SKIP("Image data file already exists.")

    return "curl -fL -o {fname} {url}".format(fname=fname, url=url)


@task
def unzip(fname: str):
  """Extract all the contents of zip file in current directory

  Returns:
      path: path of the unzipped dataset
  """
  with ZipFile(fname, 'r') as zipObj:
   zipObj.extract('microdados_enade_2019/2019/3.DADOS/microdados_enade_2019.txt')
  
  path = "./microdados_enade_2019/2019/3.DADOS/"

  return path


@task
def apply_filters(path: str):
  """Read the dataset and apply hardcoded

  Args:
      path (str): path of the original dataset

  Returns:
      pd.Dataframe: returns the filtered dataframe
  """
  cols = [
    'CO_GRUPO', 'TP_SEXO', 'NU_IDADE', 'NT_GER', 
    'NT_FG', 'NT_CE', 'QE_I01', 'QE_I02', 
    'QE_I03', 'QE_I04', 'QE_I05', 'QE_I08'
  ]
  enade = pd.read_csv(path + 'microdados_enade_2019.txt', sep=';', decimal=',', usecols=cols)
  enade = enade.loc[
    (enade.NU_IDADE > 20) &
    (enade.NU_IDADE < 40) &
    (enade.NT_GER > 0)
  ]

  return enade


@task
def transform_estcivil(df):
  """Cleanses the `estcivil` feature

  Args:
      df (pd.DataFrame): the subset feature to be cleaned

  Returns:
      pd.Series: returns the cleansed series
  """
  subset = df[['QE_I01']]
  subset['estcivil'] = subset.QE_I01.replace({
    'A': 'Solteiro',
    'B': 'Casado',
    'C': 'Separado',
    'D': 'Viúvo',
    'E': 'Outro'
  })

  return subset[['estcivil']]


@task
def transform_cor(df):
  """Cleanses the `cor` feature

  Args:
      df (pd.DataFrame): the subset feature to be cleaned

  Returns:
      pd.Series: returns the cleansed series
  """
  subset = df[['QE_I02']]
  subset['cor'] = subset.QE_I02.replace({
    'A': 'Branca',
    'B': 'Preta',
    'C': 'Amarela',
    'D': 'Parda',
    'E': 'Indígena',
    'F': "",
    ' ': ""
  })

  return subset[['cor']]


@task
def transform_escopai(df):
  """Cleanses the `escopai` feature

  Args:
      df (pd.DataFrame): the subset feature to be cleaned

  Returns:
      pd.Series: returns the cleansed series
  """
  subset = df[['QE_I04']]
  subset['escopai'] = subset.QE_I04.replace({
    'A': 0,
    'B': 1,
    'C': 2,
    'D': 3,
    'E': 4,
    'F': 5
  })

  return subset[['escopai']]


@task
def transform_escomae(df):
  """Cleanses the `escomae` feature

  Args:
      df (pd.DataFrame): the subset feature to be cleaned

  Returns:
      pd.Series: returns the cleansed series
  """
  subset = df[['QE_I05']]
  subset['escomae'] = subset.QE_I05.replace({
    'A': 0,
    'B': 1,
    'C': 2,
    'D': 3,
    'E': 4,
    'F': 5
  })

  return subset[['escomae']]


@task
def transform_renda(df):
  """Cleanses the `estcivil` feature

  Args:
      df (pd.DataFrame): the subset feature to be cleaned

  Returns:
      pd.Series: returns the cleansed series
  """
  subset = df[['QE_I08']]
  subset['renda'] = subset.QE_I08.replace({
    'A': 0,
    'B': 1,
    'C': 2,
    'D': 3,
    'E': 4,
    'F': 5,
    'G': 6
  })

  return subset[['renda']]


@task
def join_data(df, estcivil, cor, escopai, escomae, renda):
  """Joins the entire cleaned dataframe

  Args:
      df (pd.DataFrame): the source dataframe
      estcivil, cor, escopai, escomae, renda (pd.Series): a cleaned subset of the original dataframe


  Returns:
      pd.DataFrame: a merged dataframe of the entire cleaned project
  """
  tidy = pd.concat([
    df, estcivil, cor, escopai, escomae, renda
  ], axis=1)

  tidy = tidy[[
    'CO_GRUPO', 'TP_SEXO', 'NT_GER', 'NT_FG', 
    'NT_CE', 'NU_IDADE', 'estcivil', 'cor', 
    'escopai', 'escomae', 'renda'
  ]]

  logger = prefect.context.get('logger')
  logger.info(tidy.head(5).to_json())

  tidy = tidy.head(1000)

  return tidy


@task
def write_csv(df):
  """Write the cache df to a csv file and remove the source directory"""
  df.to_csv('enade2019.csv', index=False)
  system('rm ./microdados_enade_2019 -rf && rm microdados_enade_2019.zip')


@task
def upload_blob(bucket_name, source_file_name, destination_blob_name):
  """Uploads a file to the bucket."""
  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(destination_blob_name)

  blob.upload_from_filename(source_file_name)

  logger = prefect.context.get('logger')
  logger.info("File {} uploaded to {}.".format(source_file_name, destination_blob_name))

  system('rm enade2019.csv -rf')


@task
def del_cmd(fname: str) -> str:
  """
  The shell command we wish to execute.
  """
  if not os.path.exists(fname):
    raise SKIP("Image data file does not exist.")

  return "rm {fname}".format(fname=fname)