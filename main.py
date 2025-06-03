
# AUTOR: ADRIAN GONZALEZ RETAMOSA
# FECHA: 2024/09/18
# DESCRIPCION: SCRIPT DE WEB SCRAPING A PARA OBTENER DATOS DE PISOS PUESTOS A LA VENTA EN LA COMUNIDAD DE MADRID.
#-----------------------------------------------------------------------------------------------------------------------

# LIBRERIAS
import pandas as pd
import numpy as np
import os
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
import time
import math
from datetime import datetime
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# VARIABLES GLOBALES
GECKODRIVER_PATH = "/usr/local/bin/geckodriver"
path_output = os.path.join(os.getcwd(), "output")
dia = '20250602'
n_municipios=['leganes', 'getafe', 'mostoles', 'pozuelo_de_alarcon', 'madrid_suroeste_arroyomolinos', 'alcorcon', 'humanes_de_madrid', 'rivas_vaciamadrid', 'fuenlabrada', 'madrid_norte', 'madrid_sur', 'corredor_del_henares', 'madrid_noroeste', 'madrid_sureste', 'madrid_suroeste', 'arganzuela', 'madrid_capital_barajas', 'madrid_capital_carabanchel', 'madrid_capital_centro', 'madrid_capital_chamartin', 'chamberi_distrito', 'ciudad_lineal', 'fuencarral_el_pardo', 'hortaleza', 'latina', 'moncloa_aravaca', 'moratalaz', 'puente_de_vallecas', 'madrid_capital_retiro', 'madrid_capital_salamanca', 'madrid_capital_san_blas', 'tetuan', 'madrid_capital_usera', 'madrid_capital_vicalvaro', 'villa_de_vallecas', 'villaverde_distrito'] # 21



# FUNCIONES
# Configuración del entorno
def configuracion_inicial():
    entorno_deseado = 'env_vivienda'
    env_name = os.environ.get('CONDA_DEFAULT_ENV')
    if env_name != entorno_deseado:
        raise EnvironmentError(f"Entorno no correcto: {env_name}")
    print(f"Conda environment name: {env_name}")
    
## Configuración del navegador
def iniciar_navegador(options):
    # El geckodriver ya está en /usr/local/bin/geckodriver
    serv_object = Service(executable_path=GECKODRIVER_PATH)
    driver = webdriver.Firefox(service=serv_object, options=options)
    return driver

## Configuración de opciones de Selenium
def configurar_selenium():
    options = Options()
    options.add_argument('--headless')  # Modo sin interfaz gráfica
    return options

## Obtener los links de los municipios
def obtener_links_por_municipio(driver, municipios, path_output, dia):
    carpeta = rf"{path_output}"
    archivo = f"{municipios}_{dia}_links.xlsx"
    
    if os.path.exists(os.path.join(carpeta, archivo)):
        print('Hay Links')
        df_links = pd.read_excel(rf'{path_output}/{municipios}_{dia}_links.xlsx')
        archivo2 = f"{municipios}_{dia}_bu.xlsx"
        if os.path.exists(os.path.join(carpeta, archivo2)):
            print('Hay BU')
            df_aux = pd.read_excel(rf'{path_output}/{municipios}_{dia}_bu.xlsx')
            df_aux['id_a'] = 'hechos'
            df_aux = df_aux[['id', 'id_a']]
            df_links = df_links.merge(df_aux, how='left', on='id')
            df_links = df_links[df_links['id_a'].isna()]
            return df_links.id.unique()
        else:
            return df_links.id.unique()
    
    else:
        print(f'Desde 0 - {municipios}')
        lista_url = f'https://www.pisos.com/venta/pisos-{municipios}'
        driver.get(lista_url)
        
        # Aceptar cookies (esperando hasta 10 segundos)
        try:
            WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.ID, "didomi-notice-agree-button"))).click()
        except (NoSuchElementException, TimeoutException):
            print("El botón de cookies no se encontró o no es necesario.")
        
        # Obtener el número de resultados
        resultados_element = driver.find_element(By.XPATH, '//div[@class="grid__title"]//span[contains(text(), "resultados")]')
        resultados_texto = resultados_element.text.replace(' resultados', '').replace('.', '')
        maxi = math.floor((int(resultados_texto) / 30) + 1)
        
        links = []
        for n_pag in range(1, maxi+1):
            print(f'Pag {n_pag} de {maxi}')
            lista_url = f'https://www.pisos.com/venta/pisos-{municipios}/{n_pag}/'
            driver.get(lista_url)
            time.sleep(1)
            elementos_con_href_comprar = driver.find_elements(By.XPATH, '//*[contains(@href, "/comprar/")]')
            hrefs_comprar = [elemento.get_attribute('href') for elemento in elementos_con_href_comprar]
            links.extend(hrefs_comprar)
        
        df_links = pd.DataFrame({'id': links})
        df_links.drop_duplicates(inplace=True)
        df_links.to_excel(rf'{path_output}\{municipios}_{dia}_links.xlsx')
        return df_links.id.unique()

## Extraer información de los pisos
def extraer_informacion_piso(driver, link):
    driver.get(link)
    time.sleep(1)

    # Aceptar cookies si es necesario
    try:
        WebDriverWait(driver, 0.1).until(EC.element_to_be_clickable((By.ID, "didomi-notice-agree-button"))).click()
    except (NoSuchElementException, TimeoutException):
        pass  # Si no aparece el botón, continuar sin problemas.

    try:
        precio_element = driver.find_element(By.CLASS_NAME, 'price__value')
        precio_text = precio_element.text.strip()
    except:
        print(f"No se pudo encontrar el precio para este enlace: {link}")
        return None

    titulo = driver.find_element(By.TAG_NAME, 'h1').text.strip()
    posicion = driver.find_element(By.CSS_SELECTOR, '.details__block > p').text.strip()

    try:
        tipo_operacion = driver.find_element(By.CSS_SELECTOR, '.features-summary__item.features-summary__item--featured > span').text.strip()
    except:
        tipo_operacion = None

    habs, baños, texto_m2, planta, e_m2 = [None] * 5
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
        for label, value in zip(driver.find_elements(By.CLASS_NAME, 'features__label'),
                                driver.find_elements(By.CLASS_NAME, 'features__value'))
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

## Guardar respaldos periódicos
def guardar_respaldo(df, carpeta, archivo):
    if os.path.exists(os.path.join(carpeta, archivo)):
        df_bu = pd.read_excel(rf'{carpeta}\{archivo}')
        df_bu = pd.concat([df_bu, df], axis=0)
        df_bu.drop_duplicates(inplace=True)
        df_bu.to_excel(rf'{carpeta}\{archivo}', index=False)
    else:
        df.to_excel(rf'{carpeta}\{archivo}', index=False)

## Procesamiento de los municipios
def procesar_municipios(n_municipios, dia, path_driver, path_output, options):
    for municipio in n_municipios:
        driver = driver = iniciar_navegador(options)
        links = obtener_links_por_municipio(driver, municipio, path_output, dia)
        driver.quit()

        df = pd.DataFrame()
        driver = driver = iniciar_navegador(options)
        
        contador_bu = 0
        len_links = len(links)
        for idx, link in enumerate(links):
            print(f'--- Piso en {municipio}: {idx+1} de {len_links}')
            info_piso = extraer_informacion_piso(driver, link)
            if info_piso:
                df = pd.concat([df, pd.DataFrame([info_piso])], axis=0)
            
            # Guardar respaldo cada 200 registros
            if (idx + 1) % 200 == 0:
                contador_bu += 200
                archivo = f"{municipio}_{dia}_bu.xlsx"
                guardar_respaldo(df, path_output, archivo)
                df = pd.DataFrame()  # Reiniciar DataFrame para el próximo batch

        # Guardar archivo final, combinando con el respaldo si existe
        archivo_final = f"{municipio}_{dia}.xlsx"
        archivo_respaldo = f"{municipio}_{dia}_bu.xlsx"
        if os.path.exists(os.path.join(path_output, archivo_respaldo)):
            df_respaldo = pd.read_excel(rf'{path_output}\{archivo_respaldo}')
            df = pd.concat([df_respaldo, df], axis=0)
            df.drop_duplicates(inplace=True)
        
        df.to_excel(rf'{path_output}\{archivo_final}', index=False)

        # Verificar que todos los links están en el archivo final
        df_links = pd.read_excel(rf'{path_output}\{municipio}_{dia}_links.xlsx')
        ids_faltantes = set(df_links['id']) - set(df['id'])
        if len(ids_faltantes) > 3:
            print(f"Faltan {len(ids_faltantes)} links en el archivo final:")
            print(ids_faltantes)
            #raise ValueError("No se encontraron todos los links en el archivo final.")
        else:
            print(f"Todos los links están presentes en el archivo final para {municipio}.")
        
        driver.quit()


# PROGRAMA PRINCIPAL
if __name__ == "__main__":

    configuracion_inicial()
    options = configurar_selenium()
    procesar_municipios(n_municipios, dia, path_driver, path_output, options)





