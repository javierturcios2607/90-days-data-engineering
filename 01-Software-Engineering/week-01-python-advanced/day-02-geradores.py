'''
Crea un sistema simple de conexión a una base de datos simulada. Debes implementar:
Un mecanismo que reintente la conexión hasta 3 veces si falla, y que además mida cuánto tiempo tardó la ejecución total.
Un manejador que abra un archivo de logs, escriba el resultado de la conexión y garantice que el archivo se cierre correctamente, incluso si el programa se rompe a la mitad.
Toma una lista de diccionarios (con nombres y saldos) y obtén solo los nombres de los usuarios con saldo positivo, transformando sus nombres a mayúsculas.
'''



import time
import random
from functools import wraps

# ==========================================
# 1. DECORADORES (@timeit y @retry)
# ==========================================

def timeit(func):
    """Decorador para medir el tiempo total de ejecución."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        inicio = time.time()
        resultado = func(*args, **kwargs)
        fin = time.time()
        print(f"[Monitoreo] La función '{func.__name__}' tardó {fin - inicio:.4f} segundos.")
        return resultado
    return wrapper

def retry(max_intentos=3, delay=1):
    """Decorador para reintentar la ejecución en caso de fallo."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for intento in range(1, max_intentos + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"[Advertencia] Intento {intento}/{max_intentos} fallido: {e}")
                    if intento == max_intentos:
                        print(f"[Error Crítico] La función '{func.__name__}' falló tras {max_intentos} intentos.")
                        raise
                    time.sleep(delay)
        return wrapper
    return decorator

# ==========================================
# 2. CONTEXT MANAGER (Manejo de Logs)
# ==========================================

class ManejadorLogs:
    """Context Manager basado en clases para garantizar el cierre del archivo de logs."""
    def __init__(self, ruta_archivo):
        self.ruta_archivo = ruta_archivo

    def __enter__(self):
        # Se abre el archivo en modo append ('a')
        self.archivo = open(self.ruta_archivo, 'a')
        self.archivo.write(f"\n--- Inicio de conexión: {time.ctime()} ---\n")
        return self.archivo

    def __exit__(self, exc_type, exc_value, traceback):
        # Si ocurre una excepción dentro del bloque 'with', se captura aquí
        if exc_type:
            self.archivo.write(f"Estado: ERROR - {exc_value}\n")
        else:
            self.archivo.write("Estado: ÉXITO\n")
        
        self.archivo.close()
        # Retornar False permite que la excepción (si la hay) siga su curso y no sea silenciada
        return False

# ==========================================
# SIMULACIÓN DEL SISTEMA ETL
# ==========================================

@timeit
@retry(max_intentos=3, delay=2)
def conectar_bd_simulada():
    """Simula una conexión a base de datos que falla aleatoriamente."""
    print("Intentando establecer conexión a la base de datos...")
    
    # Simulamos un 60% de probabilidad de fallo en la red
    if random.random() < 0.6:
        raise ConnectionError("Timeout de red al conectar al host.")
    
    print("¡Conexión establecida con éxito!")
    # Simulamos el tiempo de consulta
    time.sleep(1)
    return True

def ejecutar_pipeline():
    # Usamos el Context Manager para asegurar que el log se escriba pase lo que pase
    with ManejadorLogs("etl_conexiones.log") as log:
        try:
            conectar_bd_simulada()
            log.write("Detalle: Los datos fueron extraídos correctamente.\n")
        except ConnectionError:
            print("El pipeline se detuvo debido a un fallo persistente de conexión.")
            # El context manager registrará el error automáticamente antes de salir

# ==========================================
# 3. PROGRAMACIÓN FUNCIONAL (map, filter)
# ==========================================

usuarios = [
    {"nombre": "javier", "saldo": 1500.50},
    {"nombre": "ana", "saldo": -300.00},
    {"nombre": "carlos", "saldo": 0.00},
    {"nombre": "marta", "saldo": 450.75}
]

# Paso A: Filtrar usuarios con saldo positivo
usuarios_positivos = filter(lambda u: u["saldo"] > 0, usuarios)

# Paso B: Extraer los nombres y transformarlos a mayúsculas
nombres_mayusculas = map(lambda u: u["nombre"].upper(), usuarios_positivos)

# Convertir el iterador resultante a lista para su uso final
resultado_nombres = list(nombres_mayusculas)

if __name__ == "__main__":
    print("--- INICIANDO EXTRACCIÓN ---")
    ejecutar_pipeline()
    
    print("\n--- INICIANDO TRANSFORMACIÓN ---")
    print(f"Usuarios originales: {len(usuarios)}")
    print(f"Nombres de usuarios con saldo positivo: {resultado_nombres}")