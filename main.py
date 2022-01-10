import threading 
import pandas as pd
import time


# 0. Lire le fichier et le stocker dans un dataframe pandas 
url = 'https://raw.githubusercontent.com/diina-gh/owid-covid/main/owid-covid-data.csv'
df = pd.read_csv(url)

def part1():
  # 1. Afficher Le nombre d’enregistrements :
  print('2. Le nombre d’enregistrements: ',len(df.index))

  # 2. Afficher le schéma :
  print("le schéma ", pd.io.json.build_table_schema(df))

  # 3. Afficher Le nombre de pays différents
  print('3. Le nombre de pays différents: ',df['location'].nunique())

  # 4. Afficher Le nombre de différents continents
  print('4. Le nombre de continents différents: ',df['continent'].nunique())


# II. Analyse et exploration des données

def part2():
  # 1. Pour chaque pays, comment trouver le nombre total de cas ?
  print('II. Analyse et exploration des données')
  print('1. le nombre total de cas par pays:')
  print(df.groupby(['location']).sum()['total_cases'])

def part3():
  # 2. Pour chaque pays, comment trouver le plus grand nombre de nouveaux cas ?
  print('2. le plus grand nombre de nouveaux cas par pays:')
  grouped_df = df.groupby(['location'])['new_cases'].max()
  print(grouped_df)

def part4(country):
    result = df[(df.location == country) & (df.date == "2020-07-31")]
    print("Nombre de cas au 31 juillet 2020 pour " + country + " :" )
    print( result[['location', 'total_cases']])

def part5():
  # 4.  Pour chaque pays on souhaite également avoir la date pour laquelle, le plus grand nombre de nouveaux cas a été enregistré.
  print('4. La date pour laquelle, le plus grand nombre de nouveaux cas a été enregistré par pays: ')
  grouped_df2 = df.groupby(['location'])['new_cases'].max()
  grouped_df2 = grouped_df2.reset_index()
  print( grouped_df2[['location', 'new_cases']])

def part6():
  # 5. Quels sont les dix pays qui ont enregistré le plus grand ratio de cas (nombre total de cas  / population au 30 juin 2020) ?
  print('5. les dix pays qui ont enregistré le plus grand ratio de cas (nombre total de cas  / population au 30 juin 2020): ')
  df['ratio'] = df['total_cases'] / df['population']
  grouped_df3 = df[(df.date == "2020-06-30")]
  grouped_df3 = df.groupby(['location'])['ratio'].max()
  grouped_df3 = grouped_df3.reset_index()
  grouped_df3 = grouped_df3.sort_values(by='ratio', ascending=False)
  grouped_df3 = grouped_df3.head(10)
  print( grouped_df3[['location', 'ratio']])

def part7():
  # 6. Quels sont les dix pays qui ont enregistré le plus grand ratio de morts (nombre total de morts/population au 31 juillet 2020) ?
  print('6. les dix pays qui ont enregistré le plus grand ratio de morts (nombre total de morts/population au 31 juillet 2020): ')
  df['ratio2'] = df['total_deaths'] / df['population']
  grouped_df3 = df[(df.date == "2020-07-31")]
  grouped_df3 = df.groupby(['location'])['ratio2'].max()
  grouped_df3 = grouped_df3.reset_index()
  grouped_df3 = grouped_df3.sort_values(by='ratio2', ascending=False)
  grouped_df3 = grouped_df3.head(10)
  print( grouped_df3[['location', 'ratio2']])


def thread1():
  part1()
  part2()
  part3()
  part5()
  part6()
  part7()

thread1()


time.sleep(5)
print("Nombre de cas au 31 juillet 2020 par pays" )
df2 = df['location'].unique()

for i in range(len(df2)):
    t = threading.Thread(target=part4, args=(df2[i],))
    t.start()
    t.join()


# 7  Pensez-vous que ce facteur (calculé au 6.) est lié au hospital_beds_per_thousand, ou au population Density? pourquoi?
# Ce facteur est lié à la fois aux attributs hospital_beds_per_thousand et opulation Density. Car plus ces derniers son grand, plus le ratio
# est important.
