import requests
import pandas as pd
from tqdm import tqdm
import numpy as np
from datetime import date
import yaml
import sqlalchemy as sql

def get_config():
    with open('wug.yaml', 'r') as c:
        config = yaml.safe_load(c)
    return config

config = get_config()
centraldb = sql.create_engine('mssql+pyodbc://{user}:{password}@{sql_server}/{sql_dbname}?driver={sql_driver}'.format_map(config))
engine = centraldb.engine
st_ch_total = pd.read_sql('SELECT * FROM wug.state_change', engine)
st_ch = st_ch_total[st_ch_total['inicio'] < st_ch_total[st_ch_total['fin'] == st_ch_total['fin'].max()]['inicio'].max()]

def segmentacion_mensual(df_eventos):
    # Creamos un dataframe vacío para ir agregando los segmentos de cada evento
    df_salida = pd.DataFrame(columns=df_eventos.columns)

    # Iteramos sobre los eventos
    pbar = tqdm(total=len(df_eventos))
    for index, evento in df_eventos.iterrows():

        # Obtenemos la fecha de inicio y fin del evento
        inicio = evento['startTimeUtc']
        inicio = pd.to_datetime(inicio, format='%Y-%m-%d %H:%M:%S')
        inicio_t = inicio
        fin = evento['endTimeUtc']
        fin = pd.to_datetime(fin, format='%Y-%m-%d %H:%M:%S')

        # Creamos un dataframe vacío para ir agregando los segmentos mensuales del evento
        df_segmentos = pd.DataFrame(columns=df_eventos.columns)

        cc = 0

        # Mientras la fecha de inicio sea anterior a la fecha de fin del evento
        while inicio < fin:
            # Creamos un segmento con la información del evento
            segmento = evento.copy()
            segmento['inicio'] = inicio_t
            segmento['fin'] = fin

            # Obtenemos la fecha de fin del mes correspondiente al inicio
            fin_mes = pd.to_datetime(inicio, format='%Y-%m-%d %H:%M:%S').replace(day=1) + pd.offsets.MonthEnd(1)
            fin_mes = fin_mes.replace(hour=23, minute=59, second=59)

            # Si la fecha de fin del mes es posterior a la fecha de fin del evento
            # ajustamos la fecha de fin del mes al fin del evento
            # Asignamos las fechas de inicio y fin correspondientes
            segmento['startTimeUtc'] = inicio
            if fin_mes > fin:
                segmento['endTimeUtc'] = fin
            else:
                cc = 1
                segmento['endTimeUtc'] = fin_mes

            segmento['corrección'] = cc

            # Agregamos el segmento al dataframe de segmentos
            df_segmentos = pd.concat([df_segmentos, segmento.to_frame().T])

            # Actualizamos la fecha de inicio al inicio del mes siguiente
            inicio = fin_mes + pd.offsets.Second(1)

        # Concatenamos los segmentos del evento al dataframe de salida
        df_salida = pd.concat([df_salida, df_segmentos])
        pbar.update()

    # Reseteamos los índices del dataframe de salida
    df_salida = df_salida.reset_index(drop=True)
    pbar.close()

    return df_salida

def get_token(username, password):
    #Obtengo token de acceso
    site = "/api/v1/token"
    url = wug_url+site

    data = {
        "grant_type": "password",
        "username": username,
        "password": password
    }

    response = requests.post(url, data=data, verify=False)

    if response.status_code != 200:
        print(f"Error: {response.status_code} - {response.text}")

    return response.json()["access_token"]

global wug_url 
wug_url = "http://wug.eana.local:9644"

usuario = '{user}'.format_map(config)
contrasenia = '{pass}'.format_map(config)

#Obtengo listado de dipositivos

token = get_token(usuario, contrasenia)
print('Token obtenido')
columns = ["hostName", "networkAddress", "bestState", "worstState", "name", "id"]
devices = pd.DataFrame(columns=columns)
site = "/api/v1/device-groups/0/devices"
query = "?pageId="
pId = 0
url = wug_url + site + query + str(pId)
size = 1
headers = {
    "Accept": "text/json",
    "Authorization": "Bearer " + token
}

while(size > 0):
    response = requests.get(url, headers=headers, verify=False)

    size = response.json()["paging"]["size"]
    pId += size
    url = wug_url + site + query + str(pId)

    # check if the request was successful
    if response.status_code == 200:
        tt = pd.DataFrame.from_dict(response.json()['data']['devices'])
        devices = pd.concat([devices, tt])
    else:
        # print the error message
        print(response.json()["error_description"])
        break
print('Se obtuvieron {} dispositivos'.format(len(devices)))
devices.reset_index(inplace=True, drop=True)

device_sql = pd.read_sql('SELECT * FROM wug.devices', engine)
#Obtengo grupos a los que pertenece cada dispositivo
if len(device_sql) < len(devices):
    devices['groups'] = "" 
    pbar = tqdm(total=len(devices))
    token = get_token(usuario, contrasenia)
    headers = {
        "Accept": "text/json",
        "Authorization": "Bearer " + token
    }
    for i, dev in devices.iterrows():
        deviceID = dev['id']
        site = "/api/v1/devices/{}/config/template".format(deviceID)
        url = wug_url + site

        response = requests.get(url, headers=headers, verify=False)

        if response.status_code == 200:
            devices.loc[i, 'groups'] = str(pd.DataFrame.from_dict(response.json()['data']['templates'])['groups'][0])
            devices.loc[i, 'primary_role'] = str(pd.DataFrame.from_dict(response.json()['data']['templates'])['primaryRole'][0])
        else:
            try:
                print(response)
                error = response.json()['error']['code']
                if error == 'TokenExpired':
                    token = get_token(usuario, contrasenia)
                    headers = {
                        "Accept": "text/json",
                        "Authorization": "Bearer " + token
                    }
                    print('Token renovado')
                    
                    response = requests.get(url, headers=headers, verify=False)

                    if response.status_code == 200:
                        devices.loc[i, 'groups'] = str(pd.DataFrame.from_dict(response.json()['data']['templates'])['groups'][0])
                        devices.loc[i, 'primary_role'] = str(pd.DataFrame.from_dict(response.json()['data']['templates'])['primaryRole'][0])
                else:
                    print(response)
                    break
            except:
                print(response)
                break

        pbar.update(1)
    pbar.close()
    rr = devices.copy()
    rr['groups'] = rr['groups'].str.replace('{\'parents\': [\'My Network\'], \'name\': ', "")
    rr['groups'] = rr['groups'].str.replace('}, {\'parents\': [\'My Network\', ', ",")
    rr['groups'] = rr['groups'].str.replace('], \'name\': ', ",")
    rr['groups'] = rr['groups'].str.replace('}, ', ",")
    devices['groups'] = rr['groups'].str.replace('}]', "")

    devices.to_sql(name='devices', schema='wug', index=False, if_exists='replace', con=engine)

#Obtengo cambios de estado de todos los routers
routers = devices[devices['name'].str.contains('@')]['id'].values

state_change = pd.DataFrame()
pbar = tqdm(total=len(routers))
token = get_token(usuario, contrasenia)
headers = {
    "Accept": "text/json",
    "Authorization": "Bearer " + token
}
for rout in routers:
    deviceID = rout
    site = "/api/v1/devices/{}/reports/state-change".format(deviceID)
    start_date = pd.to_datetime(st_ch['inicio']).max().strftime('%Y-%m-%d')
    end_date =  date.today()
    pID = 0
    query = "?range=custom&rangeStartUtc={}&rangeEndUtc={}&pageId=".format(start_date, end_date)
    url = wug_url + site + query + str(pID)

    while(True):
        response = requests.get(url, headers=headers, verify=False)

        if response.status_code == 200:
            tt = pd.DataFrame.from_dict(response.json()['data'])
            tt['deviceID'] = deviceID
            state_change = pd.concat([state_change, tt])
        else:
            try:
                print(response)
                error = response.json()['error']['code']
                if error == 'TokenExpired':
                    token = get_token(usuario, contrasenia)
                    headers = {
                        "Accept": "text/json",
                        "Authorization": "Bearer " + token
                    }
                    print('Token renovado')
                    
                    response = requests.get(url, headers=headers, verify=False)

                    if response.status_code == 200:
                        tt = pd.DataFrame.from_dict(response.json()['data'])
                        tt['deviceID'] = deviceID
                        state_change = pd.concat([state_change, tt])
                else:
                    print(response)
                    break
            except:
                print(response)
                break
        
        try:
            pID = response.json()["paging"]["nextPageId"]
        except:
            break
        url = wug_url + site + query + str(pID)
    pbar.update(1)
pbar.close()
print('Se obtuvieron {} registros'.format(len(state_change)))
state_change.reset_index(inplace=True, drop=True)

state_change['servicio'] = state_change.apply(lambda x:'MPLS' if (('MPLS' in x['monitorTypeName']) & ('BGP' in x['monitorTypeName'])) else (
                                                'VSAT' if (('VSAT' in x['monitorTypeName']) & ('TUNNEL' in x['monitorTypeName'])) else (
                                                    'Energía' if 'Power supply' in x['monitorTypeName'] else(
                                                        'Router' if 'Ping' in x['monitorTypeName'] else 'Otro'
                                                    )
                                                )
                                            ), axis=1)

sitio = pd.read_excel('disp_monitor.xlsx')
sitio['len'] = sitio.apply(lambda x: len(x['Cod']), axis=1)
sitio.sort_values(by=['len', 'Cod'], ascending=[False, True], inplace=True)
sitio.reset_index(inplace=True, drop=True)

state_change['sitio'] = np.nan
state_change['deviceName'] = state_change['deviceName'].astype('str')

for sit in sitio['Cod']:
    state_change.loc[state_change['deviceName'].str.contains(sit), 'sitio'] = sitio[sitio['Cod'] == sit]['Sitio'].values[0]

state_change['startTimeUtc'] = state_change['startTimeUtc'].replace({'\.[\d]+Z$': 'Z'}, regex=True)
state_change['startTimeUtc'] = pd.to_datetime(state_change['startTimeUtc'])

state_change['endTimeUtc'] = state_change['endTimeUtc'].replace({'\.[\d]+Z$': 'Z'}, regex=True)
state_change['endTimeUtc'] = pd.to_datetime(state_change['endTimeUtc'])

state_change['corte_energia'] = 0

print(len(state_change), len(st_ch))
state_change = pd.concat([st_ch, state_change])

state_change['startTimeUtc'] = state_change['startTimeUtc'].replace({'\.[\d]+Z$': 'Z'}, regex=True)
state_change['startTimeUtc'] = pd.to_datetime(state_change['startTimeUtc'], utc=True)

state_change['endTimeUtc'] = state_change['endTimeUtc'].replace({'\.[\d]+Z$': 'Z'}, regex=True)
state_change['endTimeUtc'] = pd.to_datetime(state_change['endTimeUtc'], utc=True)

state_change['startTimeUtc'] = state_change['startTimeUtc'].dt.tz_convert(None)
state_change['startTimeUtc'] = state_change['startTimeUtc'].astype('datetime64[ms]')

state_change['endTimeUtc'] = state_change['endTimeUtc'].dt.tz_convert(None)
state_change['endTimeUtc'] = state_change['endTimeUtc'].astype('datetime64[ms]')

mensual = state_change[state_change['startTimeUtc'].dt.month == state_change['endTimeUtc'].dt.month]
mensual['inicio'] = mensual['startTimeUtc']
mensual['fin'] = mensual['endTimeUtc']
mensuales = state_change[state_change['startTimeUtc'].dt.month != state_change['endTimeUtc'].dt.month]

print(len(mensual), len(mensuales))
if len(mensuales) > 0:
    mensual_seg = segmentacion_mensual(mensuales)
    mensual = pd.concat([mensual, mensual_seg])
print(len(mensual))
mensual.drop_duplicates(inplace=True)

mensual['corrección'] = mensual['corrección'].fillna(0)

mensual.to_sql(name='state_change', schema='wug', index=False, if_exists='replace', con=engine)

st_ch_general = pd.read_sql('SELECT * FROM wug.state_change_general', engine)
st_ch_g = st_ch_general[st_ch_general['inicio'] < st_ch_general[st_ch_general['fin'] == st_ch_general['fin'].max()]['inicio'].max()]

st_ch_g['startTimeUtc'] = st_ch_g['startTimeUtc'].replace({'\.[\d]+Z$': 'Z'}, regex=True)
st_ch_g['startTimeUtc'] = pd.to_datetime(st_ch_g['startTimeUtc'])

st_ch_g['endTimeUtc'] = st_ch_g['endTimeUtc'].replace({'\.[\d]+Z$': 'Z'}, regex=True)
st_ch_g['endTimeUtc'] = pd.to_datetime(st_ch_g['endTimeUtc'])

all_dev = devices['id'].values
state_change_general = pd.DataFrame()
pbar = tqdm(total=len(all_dev))
token = get_token(usuario, contrasenia)
headers = {
    "Accept": "text/json",
    "Authorization": "Bearer " + token
}
for dev in all_dev:
    deviceID = dev
    site = "/api/v1/devices/{}/reports/state-change".format(deviceID)
    start_date = pd.to_datetime(st_ch_g['inicio']).max().strftime('%Y-%m-%d')
    end_date = date.today()
    pID = 0
    query = "?range=custom&rangeStartUtc={}&rangeEndUtc={}&pageId=".format(start_date, end_date)
    url = wug_url + site + query + str(pID)

    while(True):
        response = requests.get(url, headers=headers, verify=False)

        if response.status_code == 200:
            tt = pd.DataFrame.from_dict(response.json()['data'])
            tt['deviceID'] = deviceID
            state_change_general = pd.concat([state_change_general, tt])
        else:
            try:
                print(response)
                error = response.json()['error']['code']
                if error == 'TokenExpired':
                    token = get_token(contrasenia, contrasenia)
                    headers = {
                        "Accept": "text/json",
                        "Authorization": "Bearer " + token
                    }
                    print('Token renovado')

                    response = requests.get(url, headers=headers, verify=False)

                    if response.status_code == 200:
                        tt = pd.DataFrame.from_dict(response.json()['data'])
                        tt['deviceID'] = deviceID
                        state_change_general = pd.concat([state_change_general, tt])
                else:
                    print(response)
                    break
            except:
                print(response)
                break
        
        try:
            pID = response.json()["paging"]["nextPageId"]
        except:
            break
        url = wug_url + site + query + str(pID)
    pbar.update(1)
pbar.close()
print('Se obtuvieron {} registros'.format(len(state_change_general)))
state_change_general.reset_index(inplace=True, drop=True)

state_change_general['servicio'] = state_change_general.apply(lambda x:'MPLS' if (('MPLS' in x['monitorTypeName']) & ('BGP' in x['monitorTypeName'])) else (
                                                'VSAT' if (('VSAT' in x['monitorTypeName']) & ('TUNNEL' in x['monitorTypeName'])) else (
                                                    'Energía' if 'Power supply' in x['monitorTypeName'] else(
                                                        'Router' if 'Ping' in x['monitorTypeName'] else 'Otro'
                                                    )
                                                )
                                            ), axis=1)

print(len(state_change_general), len(st_ch_g))
state_change_general = pd.concat([st_ch_g, state_change_general])

state_change_general['startTimeUtc'] = state_change_general['startTimeUtc'].replace({'\.[\d]+Z$': 'Z'}, regex=True)
state_change_general['startTimeUtc'] = pd.to_datetime(state_change_general['startTimeUtc'], utc=True)
state_change_general['startTimeUtc'] = state_change_general['startTimeUtc'].dt.tz_convert(None)
state_change_general['startTimeUtc'] = state_change_general['startTimeUtc'].astype('datetime64[ms]')

state_change_general['endTimeUtc'] = state_change_general['endTimeUtc'].replace({'\.[\d]+Z$': 'Z'}, regex=True)
state_change_general['endTimeUtc'] = pd.to_datetime(state_change_general['endTimeUtc'], utc=True)
state_change_general['endTimeUtc'] = state_change_general['endTimeUtc'].dt.tz_convert(None)
state_change_general['endTimeUtc'] = state_change_general['endTimeUtc'].astype('datetime64[ms]')

mensual_general = state_change_general[state_change_general['startTimeUtc'].dt.month == state_change_general['endTimeUtc'].dt.month]
mensual_general['inicio'] = mensual_general['startTimeUtc']
mensual_general['fin'] = mensual_general['endTimeUtc']
mensuales = state_change_general[state_change_general['startTimeUtc'].dt.month != state_change_general['endTimeUtc'].dt.month]
print(len(mensual_general), len(mensuales))
if len(mensuales) > 0:
    mensual_seg = segmentacion_mensual(mensuales)
    mensual_general = pd.concat([mensual_general, mensual_seg])
print(len(mensual_general))
mensual_general.drop_duplicates(inplace=True)

mensual_general['corrección'] = mensual_general['corrección'].fillna(0)

mensual_general.to_sql(name='state_change_general', schema='wug', index=False, if_exists='replace', con=engine)