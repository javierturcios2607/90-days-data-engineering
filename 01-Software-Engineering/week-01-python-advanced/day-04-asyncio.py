import asyncio
import aiohttp
import time
import logging

# Configuración de logging para monitoreo de procesos ETL
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Parámetros de control de flujo
BASE_URL = "https://pokeapi.co/api/v2/pokemon/"
POKEMON_IDS = range(1, 51)
# El semáforo evita el baneo de IP y la saturación del socket local
MAX_CONCURRENT_REQUESTS = 10 
TIMEOUT_SECONDS = 10

async def fetch_entity_data(session, entity_id, semaphore):
    """
    Realiza una petición GET asíncrona a un endpoint específico.
    
    Args:
        session (aiohttp.ClientSession): Sesión persistente para reutilización de conexiones TCP.
        entity_id (int): Identificador del recurso a consultar.
        semaphore (asyncio.Semaphore): Mecanismo de control para limitar la concurrencia.
        
    Returns:
        dict: Datos del recurso en formato JSON o None si ocurre un error persistente.
    """
    async with semaphore:
        url = f"{BASE_URL}{entity_id}"
        try:
            # Implementación de timeout para evitar tareas colgadas (Zombie tasks)
            async with session.get(url, timeout=TIMEOUT_SECONDS) as response:
                if response.status == 200:
                    data = await response.json()
                    logging.info(f"Recurso {entity_id} procesado exitosamente: {data.get('name')}")
                    return data
                
                logging.warning(f"Error en recurso {entity_id}: Status {response.status}")
                return None
                
        except asyncio.TimeoutError:
            logging.error(f"Timeout excedido para el recurso {entity_id}")
        except aiohttp.ClientError as e:
            logging.error(f"Error de conexión en recurso {entity_id}: {str(e)}")
        except Exception as e:
            logging.error(f"Error inesperado en recurso {entity_id}: {str(e)}")
        return None

async def run_extraction_pipeline():
    """
    Orquestador del pipeline asíncrono. 
    Implementa el patrón de diseño 'Fan-out/Fan-in' para procesamiento masivo de I/O.
    """
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    # ClientSession se define fuera del loop de extracción para aprovechar el Connection Pooling
    async with aiohttp.ClientSession() as session:
        # Generación de corrutinas (Lazy evaluation)
        tasks = [
            fetch_entity_data(session, p_id, semaphore) 
            for p_id in POKEMON_IDS
        ]
        
        start_time = time.perf_counter()
        
        # Ejecución concurrente y recolección de resultados
        # return_exceptions=True evita que un fallo individual detenga todo el pipeline
        results = await asyncio.gather(*tasks, return_exceptions=True)

        end_time = time.perf_counter()
        
        # Filtrado y validación de resultados finales
        valid_results = [r for r in results if isinstance(r, dict)]
        
        logging.info("--- Resumen de Ejecución ---")
        logging.info(f"Tiempo total: {end_time - start_time:.4f} segundos")
        logging.info(f"Registros exitosos: {len(valid_results)}/{len(POKEMON_IDS)}")

if __name__ == "__main__":
    try:
        # Punto de entrada para el Event Loop de asyncio
        asyncio.run(run_extraction_pipeline())
    except KeyboardInterrupt:
        logging.info("Proceso interrumpido por el usuario.")

"""
Pipeline de extracción asíncrona optimizado para cargas de trabajo I/O Bound mediante el patrón Fan-out/Fan-in. 
El diseño implementa persistencia de conexiones mediante 'aiohttp.ClientSession' para minimizar la latencia de 
handshakes TCP/SSL, junto con un semáforo de concurrencia que actúa como mecanismo de 'backpressure' para evitar 
la saturación de recursos y el bloqueo por parte del servidor. A diferencia de tareas CPU Bound, este enfoque 
maximiza el rendimiento del Event Loop al delegar los tiempos de espera de red, garantizando resiliencia 
mediante una gestión estricta de timeouts y logging estructurado para entornos de producción.
"""