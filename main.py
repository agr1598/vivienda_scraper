# AUTOR: ADRIAN GONZALEZ RETAMOSA
# FECHA: 2024/09/18
# DESCRIPCION: SCRIPT DE WEB SCRAPING PARA OBTENER DATOS DE PISOS EN COMUNIDAD DE MADRID
#              Y SUBIR AUTOMÁTICAMENTE CADA EXCEL A GOOGLE DRIVE.
# -----------------------------------------------------------------------------------------------------------------------

# ─── LIBRERIAS ──────────────────────────────────────────────────────────────────────────────────────────────────────────
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

# ─── IMPORTACIONES PARA GOOGLE DRIVE ───────────────────────────────────────────────────────────────────────────────────
# Estas tres librerías son las que acabas de añadir en requirements.txt
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ─── VARIABLES GLOBALES ───────────────────────────────────────────────────────────────────────────────────────────────
GECKODRIVER_PATH = "/usr/local/bin/geckodriver"  
# Ruta de geckodriver en el contenedor de Render (start.sh lo instala ahí)

# La carpeta "output" se crea dinámicamente en el contenedor de Render
path_output = os.path.join(os.getcwd(), "output")

dia = '20250603'
n_municipios = ['leganes']
# Si quieres procesar todos: descomenta la línea siguiente y comenta la anterior
# n_municipios = ['leganes', 'getafe', 'mostoles', 'pozuelo_de_alarcon', 'madrid_suroeste_arroyomolinos', 'alcorcon', 'humanes_de_madrid', 'rivas_vaciamadrid', 'fuenlabrada', 'madrid_norte', 'madrid_sur', 'corredor_del_henares', 'madrid_noroeste', 'madrid_sureste', 'madrid_suroeste', 'arganzuela', 'madrid_capital_barajas', 'madrid_capital_carabanchel', 'madrid_capital_centro', 'madrid_capital_chamartin', 'chamberi_distrito', 'ciudad_lineal', 'fuencarral_el_pardo', 'hortaleza', 'latina', 'moncloa_aravaca', 'moratalaz', 'puente_de_vallecas', 'madrid_capital_retiro', 'madrid_capital_salamanca', 'madrid_capital_san_blas', 'tetuan', 'madrid_capital_usera', 'madrid_capital_vicalvaro', 'villa_de_vallecas', 'villaverde_distrito']

# ─── FUNCIONES ───────────────────────────────────────────────────────────────────────────────────────────────────────

def configuracion_inicial():
    """
    En el entorno de Render no usamos Conda. Vamos a comentar o eliminar
    cualquier verificación de entorno Conda que hubiera. Dejar vacío o pass.
    """
    pass


def iniciar_navegador(options):
    """
    Configura y devuelve un webdriver de Firefox en modo headless,
    usando geckodriver instalado en /usr/local/bin/geckodriver.
    """
    serv_object = Service(executable_path=GECKODRIVER_PATH)
    driver = webdriver.Firefox(service=serv_object, options=options)
    return driver


def configurar_selenium():
    """
    Devuelve un objeto Options() configurado en modo headless
    (sin interfaz gráfica) para usar en entornos como Render.
    """
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    return options


def obtener_links_por_municipio(driver, municipios, path_output, dia):
    """
    Si existe el archivo {municipios}_{dia}_links.xlsx en output/, lo usa.
    Si no, arranca de cero, recorre todas las páginas y extrae los links.
    Al final, guarda o devuelve la lista de links únicos.
    """
    carpeta = path_output
    archivo = f"{municipios}_{dia}_links.xlsx"

    if os.path.exists(os.path.join(carpeta, archivo)):
        print('Hay Links previos: cargando desde Excel')
        df_links = pd.read_excel(os.path.join(carpeta, archivo))
        archivo2 = f"{municipios}_{dia}_bu.xlsx"
        if os.path.exists(os.path.join(carpeta, archivo2)):
            print('Hay respaldo parcial (BU): filtrando links ya procesados')
            df_aux = pd.read_excel(os.path.join(carpeta, archivo2))
            df_aux['id_a'] = 'hechos'
            df_aux = df_aux[['id', 'id_a']]
            df_links = df_links.merge(df_aux, how='left', on='id')
            df_links = df_links[df_links['id_a'].isna()]
            return df_links.id.unique()
        else:
            return df_links.id.unique()

    else:
        print(f'Desde 0 - obteniendo links de {municipios}')
        lista_url = f'https://www.pisos.com/venta/pisos-{municipios}'
        driver.get(lista_url)

        # Aceptar cookies si aparece el botón
        try:
            WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.ID, "didomi-notice-agree-button"))
            ).click()
        except (NoSuchElementException, TimeoutException):
            print("Botón de cookies no encontrado o no es necesario.")

        # Número de resultados para calcular el total de páginas
        resultados_element = driver.find_element(
            By.XPATH,
            '//div[@class="grid__title"]//span[contains(text(), "resultados")]'
        )
        resultados_texto = resultados_element.text.replace(' resultados', '').replace('.', '')
        maxi = math.floor((int(resultados_texto) / 30) + 1)

        links = []
        for n_pag in range(1, maxi + 1):
            print(f'Pag {n_pag} de {maxi}')
            lista_url = f'https://www.pisos.com/venta/pisos-{municipios}/{n_pag}/'
            driver.get(lista_url)
            time.sleep(1)
            elementos_con_href_comprar = driver.find_elements(
                By.XPATH, '//*[contains(@href, "/comprar/")]'
            )
            hrefs_comprar = [elem.get_attribute('href') for elem in elementos_con_href_comprar]
            links.extend(hrefs_comprar)

        df_links = pd.DataFrame({'id': links})
        df_links.drop_duplicates(inplace=True)
        os.makedirs(carpeta, exist_ok=True)
        df_links.to_excel(os.path.join(carpeta, archivo), index=False)
        return df_links.id.unique()


def extraer_informacion_piso(driver, link):
    """
    Dada una URL de piso, entra en la página y extrae:
    precio, título, posición, tipo, m2, habitaciones, baños, planta, p_m2,
    equipamiento (texto concatenado), descripción y URL (id).
    Devuelve un dict o None si no encuentra precio.
    """
    driver.get(link)
    time.sleep(1)

    # Aceptar cookies si aparece
    try:
        WebDriverWait(driver, 0.1).until(
            EC.element_to_be_clickable((By.ID, "didomi-notice-agree-button"))
        ).click()
    except (NoSuchElementException, TimeoutException):
        pass

    # Precio (si no se encuentra, devolvemos None)
    try:
        precio_element = driver.find_element(By.CLASS_NAME, 'price__value')
        precio_text = precio_element.text.strip()
    except:
        print(f"No se pudo encontrar el precio para este enlace: {link}")
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

    # Inicializamos variables a None
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
    """
    Si ya existe el archivo de respaldo en carpeta,
    lo carga, concatena con el df nuevo, quita duplicados y guarda.
    Si no existe, guarda el df como nuevo Excel.
    """
    ruta_archivo = os.path.join(carpeta, archivo)
    if os.path.exists(ruta_archivo):
        df_bu = pd.read_excel(ruta_archivo)
        df_bu = pd.concat([df_bu, df], axis=0)
        df_bu.drop_duplicates(inplace=True)
        df_bu.to_excel(ruta_archivo, index=False)
    else:
        df.to_excel(ruta_archivo, index=False)


def subir_a_drive(ruta_local, nombre_archivo):
    """
    1) Lee la variable de entorno GOOGLE_CREDENTIALS_JSON y la escribe en temp_gdrive_credentials.json.
    2) Usa esas credenciales para crear un cliente de Drive v3.
    3) Sube el fichero 'ruta_local' a la carpeta de Drive cuyo ID está en GOOGLE_DRIVE_FOLDER_ID.
    """
    # 1) Guardamos JSON temporal
    creds_json = os.environ["GOOGLE_CREDENTIALS_JSON"]
    with open("temp_gdrive_credentials.json", "w") as temp:
        temp.write(creds_json)

    # 2) Creamos credenciales con el archivo temporal y el scope necesario
    SCOPES = ["https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file("temp_gdrive_credentials.json", scopes=SCOPES)

    # 3) Construimos el cliente de la API de Drive v3
    drive_service = build("drive", "v3", credentials=creds)

    # 4) Metadatos del archivo: nombre y carpeta destino
    carpeta_id = os.environ["GOOGLE_DRIVE_FOLDER_ID"]
    file_metadata = {
        "name": nombre_archivo,
        "parents": [carpeta_id]
    }

    # 5) Creamos el objeto MediaFileUpload para subir el Excel
    media = MediaFileUpload(
        ruta_local,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        resumable=True
    )

    # 6) Ejecutamos la petición de subida
    uploaded_file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()

    print(f"✔ Subido a Google Drive: {nombre_archivo} (ID: {uploaded_file.get('id')})")

    # 7) Eliminamos el JSON temporal por seguridad
    os.remove("temp_gdrive_credentials.json")


def procesar_municipios(n_municipios, dia, path_output, options):
    """
    Para cada municipio:
      1) Inicia navegador.
      2) Obtiene lista de links (o carga previos).
      3) Recorre cada link, extrae info y concatena en un DataFrame.
      4) Cada 200 registros guarda respaldo (opcional).
      5) Al final, concatena con respaldo si existe y crea el Excel final.
      6) LLAMA A subir_a_drive(ruta_local, archivo_final) para subir ese .xlsx a Drive.
      7) Verifica qué links faltan e imprime mensaje.
      8) Cierra el navegador.
    """
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

            # Guardar respaldo cada 200 registros
            if (idx + 1) % 200 == 0:
                archivo_bu = f"{municipio}_{dia}_bu.xlsx"
                guardar_respaldo(df, path_output, archivo_bu)
                df = pd.DataFrame()

        # 5) GUARDAR ARCHIVO FINAL COMBINANDO CON RESPALDO SI EXISTE
        os.makedirs(path_output, exist_ok=True)
        archivo_final = f"{municipio}_{dia}.xlsx"
        ruta_local = os.path.join(path_output, archivo_final)
        df.to_excel(ruta_local, index=False)

        # ─── LLAMADA CLAVE: SUBIR A GOOGLE DRIVE ─────────────────
        subir_a_drive(ruta_local, archivo_final)

        # 6) Verificar que todos los links estén en el Excel final
        df_links = pd.read_excel(os.path.join(path_output, f"{municipio}_{dia}_links.xlsx"))
        ids_faltantes = set(df_links['id']) - set(df['id'])
        if len(ids_faltantes) > 3:
            print(f"Faltan {len(ids_faltantes)} links en el archivo final para {municipio}:")
            print(ids_faltantes)
        else:
            print(f"Todos los links están presentes en el archivo final para {municipio}.")

        driver.quit()


# ─── PROGRAMA PRINCIPAL ──────────────────────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    configuracion_inicial()               # Ya no verifica Conda, simplemente pass
    options = configurar_selenium()       # Firefox headless
    procesar_municipios(n_municipios, dia, path_output, options)
