import logging
import io
import os
import azure.functions as func
import datetime
import json
import requests
from dotenv import load_dotenv
import pandas as pd
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DOMAIN = os.getenv("DOMAIN")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")
STORA_ACCOUNT_NAME = os.getenv("STORA_ACCOUNT_NAME")

@app.queue_trigger(
    arg_name="azqueue"
    , queue_name="requests"
    , connection="QueueAzureWebJobsStorage"
)
def QueueTriggerPokeReport(azqueue: func.QueueMessage):
    body = azqueue.get_body().decode('utf-8')
    record = json.loads(body)
    id = record[0]["id"]

    try:
        update_request(id, "inprogress")
        request_info = get_request(id)
        pokemon_basic_list = get_pokemons(request_info[0]["type"])
        
        detailed_pokemon_list = get_detailed_pokemon_info(pokemon_basic_list)
        
        # Generar CSV con toda la información 
        pokemon_bytes = generate_detailed_csv_to_blob(detailed_pokemon_list)
        blob_name = f"poke_report_{id}.csv"
        upload_csv_to_blob(blob_name=blob_name, csv_data=pokemon_bytes)
        logger.info(f"Archivo {blob_name} se subio con exito")

        url_completa = f"https://{STORA_ACCOUNT_NAME}.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{blob_name}"
        update_request(id, "completed", url_completa)
    except Exception as e:
        logger.error(f"Error procesando la solicitud: {e}")
        update_request(id, "failed")
        raise

def update_request(id: int, status: str, url: str = None) -> dict:
    payload = {
        "status": status
        , "id": id
    }
    if url:
        payload["url"] = url

    reponse = requests.put(f"{DOMAIN}/api/request", json=payload)
    return reponse.json()

def get_request(id: int) -> dict:
    reponse = requests.get(f"{DOMAIN}/api/request/{id}")
    return reponse.json()

def get_pokemons(type: str) -> dict:
    pokeapi_url = f"https://pokeapi.co/api/v2/type/{type}"
    response = requests.get(pokeapi_url, timeout=3000)
    data = response.json()
    pokemon_entries = data.get("pokemon", [])
    return [p["pokemon"] for p in pokemon_entries]

def get_pokemon_details(pokemon_url: str) -> dict:
    """Obtiene detalles detallados de un Pokémon específico"""
    try:
        response = requests.get(pokemon_url, timeout=3000)
        if response.status_code == 200:
            pokemon_data = response.json()
            
            stats = {}
            for stat in pokemon_data.get("stats", []):
                stat_name = stat["stat"]["name"]
                stat_value = stat["base_stat"]
                stats[stat_name] = stat_value
            
            abilities = [ability["ability"]["name"] for ability in pokemon_data.get("abilities", [])]
            abilities_str = ", ".join(abilities) if abilities else ""
            
            pokemon_details = {
                "name": pokemon_data["name"],
                "url": pokemon_url,
                "hp": stats.get("hp", 0),
                "attack": stats.get("attack", 0),
                "defense": stats.get("defense", 0),
                "special-attack": stats.get("special-attack", 0),
                "special-defense": stats.get("special-defense", 0),
                "speed": stats.get("speed", 0),
                "abilities": abilities_str
            }
            
            return pokemon_details
        else:
            logger.warning(f"Error obteniendo detalles para {pokemon_url}: {response.status_code}")
            return None
    except Exception as e:
        logger.warning(f"Error en solicitud para {pokemon_url}: {e}")
        return None

def get_detailed_pokemon_info(pokemon_list: list) -> list:
    """Obtiene información detallada para todos los Pokémon secuencialmente"""
    detailed_pokemons = []
    total_pokemons = len(pokemon_list)
    
    logger.info(f"Obteniendo detalles para {total_pokemons} Pokémon...")
    
    for i, pokemon in enumerate(pokemon_list):
        if (i + 1) % 5 == 0:
            logger.info(f"Procesando Pokémon {i + 1}/{total_pokemons}...")
        
        pokemon_detail = get_pokemon_details(pokemon["url"])
        if pokemon_detail:
            detailed_pokemons.append(pokemon_detail)
    
    logger.info(f"Completada la obtención de detalles para {len(detailed_pokemons)}/{total_pokemons} Pokémon")
    return detailed_pokemons

def generate_detailed_csv_to_blob(detailed_pokemon_list: list) -> bytes:
    """Genera un CSV con información detallada de los Pokémon"""
    df = pd.DataFrame(detailed_pokemon_list)
    
    output = io.StringIO()
    df.to_csv(output, index=False, encoding='utf-8')
    csv_bytes = output.getvalue().encode('utf-8')
    output.close()
    return csv_bytes

def upload_csv_to_blob(blob_name: str, csv_data: bytes):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=blob_name)
        blob_client.upload_blob(csv_data, overwrite=True)
    except Exception as e:
        logger.error(f"Error al subir el archivo {e}")
        raise