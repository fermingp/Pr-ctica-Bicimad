'''
Práctica 4:
    
    Carlos Díez y Fermín González

'''

from pyspark import SparkContext
sc = SparkContext()

import json
import pandas as pd 
import matplotlib.pyplot as plt
import numpy as np

rdd_base_ene18 = sc.textFile('201801.json')
rdd_base_ene19 = sc.textFile('201901.json')

rdd_base_feb18 = sc.textFile('201802.json')
rdd_base_feb19 = sc.textFile('201902.json')

rdd_base_mar18 = sc.textFile('201803.json')
rdd_base_mar19 = sc.textFile('201903.json')

rdd_base_abr18 = sc.textFile('201804.json')
rdd_base_abr19 = sc.textFile('201904.json')

rdd_base_may18 = sc.textFile('201805.json')
rdd_base_may19 = sc.textFile('201905.json')

rdd_base_jun18 = sc.textFile('201806.json')
rdd_base_jun19 = sc.textFile('201906.json')

def mapper(line):
    info = {}
    data = json.loads(line)
    info['user'] = data['user_day_code']
    info['user_type'] = int(data['user_type'])
    info['ageRange'] = int(data['ageRange'])
    info['start'] = data['idunplug_station']
    info['end'] = data['idplug_station']
    info['travel_time'] = int(data['travel_time'])
    info['zip_code'] = data['zip_code']
    return info

estaciones_mad_central = [123, 57, 3, 12, 4, 5, 8, 14, 13, 15, 11, 54, 7, 10, 116, 59, 16, 17, 55, 6, 26, 94, 19, 58, 18, 21, 2, 22, 23, 30, 24, 36, 25, 56, 28, 20, 86, 29, 1, 31, 9, 35, 32, 33, 34, 40, 52, 27, 41, 37, 38, 39, 45, 44, 43, 42, 67, 81, 50, 51, 53, 46, 47, 48, 49]

rdds_18 = [rdd_base_ene18, rdd_base_feb18, rdd_base_mar18, rdd_base_abr18, rdd_base_may18, rdd_base_jun18]
rdds_19 = [rdd_base_ene19, rdd_base_feb19, rdd_base_mar19, rdd_base_abr19, rdd_base_may19, rdd_base_jun19]



'''
GRAFICA VIAJES INICIADOS
'''

res_18 = []
res_19 = []
for i in rdds_18:
    n_viajes = i.map(mapper).filter(lambda x: x['start'] in estaciones_mad_central).count()
    res_18.append(n_viajes)

for i in rdds_19:
    n_viajes = i.map(mapper).filter(lambda x: x['start'] in estaciones_mad_central).count()
    res_19.append(n_viajes)   

    
data = pd.DataFrame({'año2018' : res_18,
                     'año2019': res_19},
                    index=('Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio'))

n = len(data.index)
x = np.arange(n)
width = 0.25
plt.bar(x - width, data.año2018, width=width, label='2018')
plt.bar(x, data.año2019, width=width, label='2019')
plt.xticks(x, data.index)
plt.legend(loc='best')
plt.title('Viajes iniciados 2018 vs 2019')
plt.show()



'''
GRAFICA VIAJES FINALIZADOS
'''

res_18 = []
res_19 = []
for i in rdds_18:
    n_viajes = i.map(mapper).filter(lambda x: x['end'] in estaciones_mad_central).count()
    res_18.append(n_viajes)

for i in rdds_19:
    n_viajes = i.map(mapper).filter(lambda x: x['end'] in estaciones_mad_central).count()
    res_19.append(n_viajes)   

    
data = pd.DataFrame({'año2018' : res_18,
                     'año2019': res_19},
                    index=('Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio'))

n = len(data.index)
x = np.arange(n)
width = 0.25
plt.bar(x - width, data.año2018, width=width, label='2018')
plt.bar(x, data.año2019, width=width, label='2019')
plt.xticks(x, data.index)
plt.legend(loc='best')
plt.title('Viajes finalizados 2018 vs 2019')
plt.show()


'''

GRAFICA RANGO EDAD

'''


niños_ene19 = rdd_base_ene19.map(mapper).filter(lambda x: x['ageRange'] in [1,2]).count()
jovenes_ene19 = rdd_base_ene19.map(mapper).filter(lambda x: x['ageRange'] in [3,4]).count()
mayores_ene19 = rdd_base_ene19.map(mapper).filter(lambda x: x['ageRange'] in [5,6]).count()

niños_feb19 = rdd_base_feb19.map(mapper).filter(lambda x: x['ageRange'] in [1,2]).count()
jovenes_feb19 = rdd_base_feb19.map(mapper).filter(lambda x: x['ageRange'] in [3,4]).count()
mayores_feb19 = rdd_base_feb19.map(mapper).filter(lambda x: x['ageRange'] in [5,6]).count()

niños_mar19 = rdd_base_mar19.map(mapper).filter(lambda x: x['ageRange'] in [1,2]).count()
jovenes_mar19 = rdd_base_mar19.map(mapper).filter(lambda x: x['ageRange'] in [3,4]).count()
mayores_mar19 = rdd_base_mar19.map(mapper).filter(lambda x: x['ageRange'] in [5,6]).count()

niños_abr19 = rdd_base_abr19.map(mapper).filter(lambda x: x['ageRange'] in [1,2]).count()
jovenes_abr19 = rdd_base_abr19.map(mapper).filter(lambda x: x['ageRange'] in [3,4]).count()
mayores_abr19 = rdd_base_abr19.map(mapper).filter(lambda x: x['ageRange'] in [5,6]).count()

niños_may19 = rdd_base_may19.map(mapper).filter(lambda x: x['ageRange'] in [1,2]).count()
jovenes_may19 = rdd_base_may19.map(mapper).filter(lambda x: x['ageRange'] in [3,4]).count()
mayores_may19 = rdd_base_may19.map(mapper).filter(lambda x: x['ageRange'] in [5,6]).count()


niños_jun19 = rdd_base_jun19.map(mapper).filter(lambda x: x['ageRange'] in [1,2]).count()
jovenes_jun19 = rdd_base_jun19.map(mapper).filter(lambda x: x['ageRange'] in [3,4]).count()
mayores_jun19 = rdd_base_jun19.map(mapper).filter(lambda x: x['ageRange'] in [5,6]).count()


data = pd.DataFrame({'Niños' : [niños_ene19, niños_feb19, niños_mar19, niños_abr19, niños_may19, niños_jun19],
                     'Jovenes': [jovenes_ene19, jovenes_feb19, jovenes_mar19, jovenes_abr19, jovenes_may19, jovenes_jun19],
                     'Mayores': [mayores_ene19, mayores_feb19, mayores_mar19, mayores_abr19, mayores_may19, mayores_jun19]},
                    index=('Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio'))

n = len(data.index)
x = np.arange(n)
width = 0.25
plt.bar(x - width, data.Niños, width=width, label='Niños (0-18)')
plt.bar(x, data.Jovenes, width=width, label='Jóvenes (19-40)')
plt.bar(x + width, data.Mayores, width=width, label='Mayores (+41)')
plt.xticks(x, data.index)
plt.legend(loc='best')
plt.title('Grupos de edad - 2019')
plt.show()



'''
CICLOS
'''

def count_estaciones(values):
    '''
    Función que cuenta el número de viajes que se inician en cada estación.
    
    Para contar el número de viajes que se inician en cada estación, 
    contamos cada viaje con un 1 para luego agrupar todos los viajes 
    que se han iniciado en la misma estación.
    
    
    Por último, ordenamos de menor a mayor segun su el número de viajes.
    
    Devuelve una lista cuyos elementos son tuplas formadas por:
        (id_unplug_station, número de viajes)
    '''
    values = values.map(lambda x: (x['start'],1)).groupByKey().mapValues(len).collect()
    values = sorted([(x[1],x[0]) for x in values])
    values = [(x[1],x[0]) for x in values]
    return values

estaciones = count_estaciones(rdd_base_ene19.map(mapper).filter(lambda x: x['start'] in estaciones_mad_central))
estacion_fav = estaciones[-1]
estacion_peor = estaciones[0]


print(estacion_fav)
print(estacion_peor)


def es_ciclo(data):
    result = False
    for i in range(len(data[1])):
        j = i + 1
        while j<len(data[1]) and not result:
            result = data[1][i][0]==data[1][j][1]
            j+=1
    return result


cic_18 = []
cic_19 = []

for i in rdds_18:
    data_reps = i.map(mapper).filter(lambda x: x['start'] in estaciones_mad_central).map(lambda x: (x['user'],[x['start'], x['end']])).groupByKey().mapValues(tuple)\
    .filter(lambda x: len(x[1])>1)
    ciclos = data_reps.filter(lambda x: es_ciclo(x)).count()
    cic_18.append(ciclos)

for i in rdds_19:
    data_reps = i.map(mapper).filter(lambda x: x['start'] in estaciones_mad_central).map(lambda x: (x['user'],[x['start'], x['end']])).groupByKey().mapValues(tuple)\
    .filter(lambda x: len(x[1])>1)
    ciclos = data_reps.filter(lambda x: es_ciclo(x)).count()
    cic_19.append(ciclos)
    
data = pd.DataFrame({'año2018' : cic_18,
                     'año2019': cic_19},
                    index=('Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio'))

n = len(data.index)
x = np.arange(n)
width = 0.25
plt.bar(x - width, data.año2018, width=width, label='2018')
plt.bar(x, data.año2019, width=width, label='2019')
plt.xticks(x, data.index)
plt.legend(loc='best')
plt.title('Número de ciclos')
plt.show() 



'''
GRAFICA ABONADOS
'''

res_18 = []
res_19 = []

for i in rdds_18:
    abonados = i.map(mapper).filter(lambda x: x['start'] in estaciones_mad_central or x['end'] in estaciones_mad_central).filter(lambda x: x['user_type']==1).count()
    res_18.append(abonados)

for i in rdds_19:
    abonados = i.map(mapper).filter(lambda x: x['start'] in estaciones_mad_central or x['end'] in estaciones_mad_central).filter(lambda x: x['user_type']==1).count()
    res_19.append(abonados)

data = pd.DataFrame({'año2018' : res_18,
                     'año2019': res_19},
                    index=('Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio'))

n = len(data.index)
x = np.arange(n)
width = 0.25
plt.bar(x - width, data.año2018, width=width, label='2018')
plt.bar(x, data.año2019, width=width, label='2019')
plt.xticks(x, data.index)
plt.legend(loc='best')
plt.title('Abonados')
plt.show()

