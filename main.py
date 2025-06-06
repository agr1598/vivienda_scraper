# AUTOR: ADRIAN GONZALEZ RETAMOSA
# FECHA: 2024/09/18
# DESCRIPCION: SCRIPT DE WEB SCRAPING PARA OBTENER DATOS DE PISOS EN COMUNIDAD DE MADRID
#              Y SUBIR AUTOMÁTICAMENTE CADA EXCEL A GOOGLE DRIVE.
# -----------------------------------------------------------------------------------------------------------------------

# ─── LIBRERIAS ──────────────────────────────────────────────────────────────────────────────────────────────────────────
import os
import pandas as pd
import numpy as np
import time
import math
from datetime import datetime

# Selenium
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# Google Drive
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Geckodriver autoinstaller
import geckodriver_autoinstaller
geckodriver_autoinstaller.install()

# Variables globales
path_output = os.path.join(os.getcwd(), "output")
dia = '20250603'
n_municipios = ['leganes']

def configuracion_inicial():
    pass

def iniciar_navegador(options):
    driver = webdriver.Firefox(options=options)
    return driver

def configurar_selenium():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    return options

def obtener_links_por_municipio(driver, municipios, path_output, dia):
    carpeta = path_output
    archivo = f"{municipios}_{dia}_links.xlsx"
    if os.path.exists(os.path.join(carpeta, archivo)):
        print(f"Hay Links previos de {municipios}, cargando CSV…")
        df_links = pd.read_excel(os.path.join(carpeta, archivo))
        archivo2 = f"{municipios}_{dia}_bu.xlsx"
        if os.path.exists(os.path.join(carpeta, archivo2)):
            print(f"Hay respaldo parcial para {municipios}, filtrando links ya procesados…")
            df_aux = pd.read_excel(os.path.join(carpeta, archivo2))
            df_aux['id_a'] = 'hechos'
            df_aux = df_aux[['id', 'id_a']]
            df_links = df_links.merge(df_aux, how='left', on='id')
            df_links = df_links[df_links['id_a'].isna()]
            return df_links.id.unique()
        else:
            return df_links.id.unique()
    else:
        print(f"Desde 0 – obteniendo links de {municipios}")
        lista_url = f'https://www.pisos.com/venta/pisos-{municipios}'
        driver.get(lista_url)
        try:
            WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.ID, "didomi-notice-agree-button"))
            ).click()
        except (NoSuchElementException, TimeoutException):
            print("Botón de cookies no encontrado o no es necesario.")
        resultados_element = driver.find_element(
            By.XPATH, '//div[@class="grid__title"]//span[contains(text(), "resultados")]'
        )
        resultados_texto = resultados_element.text.replace(' resultados', '').replace('.', '')
        maxi = math.floor((int(resultados_texto) / 30) + 1)
        links = []
        for n_pag in range(1, maxi + 1):
            print(f"Pag {n_pag} de {maxi}")
            url_pagina = f'https://www.pisos.com/venta/pisos-{municipios}/{n_pag}/'
            driver.get(url_pagina)
            time.sleep(1)
            elementos_con_href = driver.find_elements(
                By.XPATH, '//*[contains(@href, "/comprar/")]'
            )
            hrefs = [elem.get_attribute('href') for elem in elementos_con_href]
            links.extend(hrefs)
        df_links = pd.DataFrame({'id': links})
        df_links.drop_duplicates(inplace=True)
        os.makedirs(carpeta, exist_ok=True)
        df_links.to_excel(os.path.join(carpeta, archivo), index=False)
        return df_links.id.unique()

def extraer_informacion_piso(driver, link):
    driver.get(link)
    time.sleep(1)
    try:
        WebDriverWait(driver, 0.1).until(
            EC.element_to_be_clickable((By.ID, "didomi-notice-agree-button"))
        ).click()
    except (NoSuchElementException, TimeoutException):
        pass
    try:
        precio_element = driver.find_element(By.CLASS_NAME, 'price__value')
        precio_text = precio_element.text.strip()
    except:
        print(f"No se encontró precio para {link}")
        return None
    titulo = driver.find_element(By.TAG_NAME, 'h1').text.strip()
    posicion = driver.find_element(By.CSS_SELECTOR, '.details__block > p').text.strip()
    try:
        tipo_operacion = driver.find_element(
            By.CSS_SELECTOR,
            '.features-summary__item.features-summary__item--featured > span'
        ).text.strip()
    except:
        tipo_operacion = None
    habs = baños = texto_m2 = planta = e_m2 = None
    for feature in driver.find_elements(By.CLASS_NAME, 'features-summary__item'):
        text = feature.text.strip()
        if 'hab' in text:
            habs = text
        elif 'baño' in text:
            baños = text
        elif 'm²' in text and '/' not in text:
            texto_m2 = text
        elif 'planta' in text:
            planta = text
        elif '/m²' in text:
            e_m2 = text
    additional_info = "///".join(
        label.text.strip() + ': ' + value.text.strip()
        for label, value in zip(
            driver.find_elements(By.CLASS_NAME, 'features__label'),
            driver.find_elements(By.CLASS_NAME, 'features__value')
        )
    )
    descripcion = driver.find_element(By.CLASS_NAME, 'description__content').text.strip()
    return {
        'precio': precio_text,
        'titulo': titulo,
        'posicion': posicion,
        'tipo': tipo_operacion,
        'm2': texto_m2,
        'habitaciones': habs,
        'baños': baños,
        'planta': planta,
        'p_m2': e_m2,
        'Equipamiento': additional_info,
        'descripcion': descripcion,
        'id': link
    }

def guardar_respaldo(df, carpeta, archivo):
    ruta_archivo = os.path.join(carpeta, archivo)
    if os.path.exists(ruta_archivo):
        df_bu = pd.read_excel(ruta_archivo)
        df_bu = pd.concat([df_bu, df], axis=0)
        df_bu.drop_duplicates(inplace=True)
        df_bu.to_excel(ruta_archivo, index=False)
    else:
        df.to_excel(ruta_archivo, index=False)

def subir_a_drive(ruta_local, nombre_archivo):
    creds_json = os.environ["GOOGLE_CREDENTIALS_JSON"]
    with open("temp_gdrive_credentials.json", "w") as temp:
        temp.write(creds_json)
    SCOPES = ["https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file("temp_gdrive_credentials.json", scopes=SCOPES)
    drive_service = build("drive", "v3", credentials=creds)
    carpeta_id = os.environ["GOOGLE_DRIVE_FOLDER_ID"]
    file_metadata = {"name": nombre_archivo, "parents": [carpeta_id]}
    media = MediaFileUpload(
        ruta_local,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        resumable=True
    )
    uploaded_file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()
    print(f"✔ Subido a Google Drive: {nombre_archivo} (ID: {uploaded_file.get('id')})")
    os.remove("temp_gdrive_credentials.json")

def procesar_municipios(n_municipios, dia, path_output, options):
    for municipio in n_municipios:
        driver = iniciar_navegador(options)
        links = obtener_links_por_municipio(driver, municipio, path_output, dia)
        driver.quit()
        df = pd.DataFrame()
        driver = iniciar_navegador(options)
        for idx, link in enumerate(links):
            print(f'--- Piso en {municipio}: {idx+1} de {len(links)}')
            info_piso = extraer_informacion_piso(driver, link)
            if info_piso:
                df = pd.concat([df, pd.DataFrame([info_piso])], axis=0)
            if (idx + 1) % 200 == 0:
                archivo_bu = f"{municipio}_{dia}_bu.xlsx"
                guardar_respaldo(df, path_output, archivo_bu)
                df = pd.DataFrame()
        os.makedirs(path_output, exist_ok=True)
        archivo_final = f"{municipio}_{dia}.xlsx"
        ruta_local = os.path.join(path_output, archivo_final)
        df.to_excel(ruta_local, index=False)
        subir_a_drive(ruta_local, archivo_final)
        df_links = pd.read_excel(os.path.join(path_output, f"{municipio}_{dia}_links.xlsx"))
        ids_faltantes = set(df_links['id']) - set(df['id'])
        if len(ids_faltantes) > 3:
            print(f"Faltan {len(ids_faltantes)} links en el Excel final de {municipio}:")
            print(ids_faltantes)
        else:
            print(f"Todos los links están presentes en el Excel final de {municipio}.")
        driver.quit()

if __name__ == "__main__":
    configuracion_inicial()
    options = configurar_selenium()
    procesar_municipios(n_municipios, dia, path_output, options)

