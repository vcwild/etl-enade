from prefect import task
import prefect
import pandas as pd
from os import system


@task
def get_raw_data():
  """Get the raw data from the defined url, unzip and define the project path

  Returns:
      path: path of the unzipped dataset
  """
  system('wget http://download.inep.gov.br/microdados/Enade_Microdados/microdados_enade_2019.zip --no-check-certificate && \
    unzip microdados_enade_2019.zip && \
    rm microdados_enade_2019.zip')
  path = "./microdados_enade_2019/2019/3.DADOS/"

  return path


@task
def apply_filters(path):
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
def clean_estcivil(df):
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
def clean_cor(df):
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
def clean_escopai(df):
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
def clean_escomae(df):
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
def clean_renda(df):
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

  return tidy


@task
def write_csv(df):
  """Write the cache df to a csv file and remove the source material"""
  df.to_csv('enade2019.csv', index=False)
  system('rm ./microdados_enade_2019 -rf')