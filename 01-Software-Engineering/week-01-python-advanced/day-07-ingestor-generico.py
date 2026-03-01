"""
Dia 07: Proyecto Semanal - Ingestor Generico Asincrono
Plan de Aprendizaje de 90 Dias - Data Engineering

Descripcion:
Este script implementa un pipeline de extraccion, validacion y carga (ETL ligero).
Extrae datos de una API REST de forma asincrona, los valida contra un contrato estricto
y los guarda en el sistema de archivos local.
"""

import asyncio
import aiohttp
import json
import time
from functools import wraps
from typing import List, Dict, Any, Generator, Type
from pydantic import BaseModel, Field, EmailStr, ValidationError

# ==========================================
# 1. DECORADORES (Tema del Martes)
# ==========================================
# Usamos un decorador para medir el tiempo de ejecucion de funciones asincronas.
# Esto cumple con el principio de responsabilidad unica: la funcion hace su trabajo,
# y el decorador se encarga de la telemetria/auditoria.

def medir_tiempo_ejecucion(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        inicio = time.time()
        resultado = await func(*args, **kwargs)
        fin = time.time()
        print(f"[Telemetria] La funcion '{func.__name__}' tardo {fin - inicio:.4f} segundos.")
        return resultado
    return wrapper

# ==========================================
# 2. CONTRATO DE DATOS (Tema del Viernes)
# ==========================================
# Pydantic asegura la calidad del dato antes de aterrizarlo.

class UsuarioAPI(BaseModel):
    id_usuario: int = Field(alias="id") # Mapeamos 'id' del JSON a 'id_usuario'
    nombre: str = Field(alias="name")
    email: EmailStr
    
    class Config:
        populate_by_name = True

# ==========================================
# 3. MIXINS Y POO (Tema del Miercoles)
# ==========================================
# Un Mixin aporta funcionalidad especifica a una clase sin usar herencia profunda.
# Extraemos la logica de guardado de archivos para que el ingestor no este acoplado
# a un solo metodo de almacenamiento.

class AlmacenamientoLocalMixin:
    def guardar_json_local(self, datos: List[Dict[str, Any]], ruta_archivo: str) -> None:
        """Guarda una lista de diccionarios en un archivo JSON local."""
        # Context Manager (Tema del Martes): 'with' asegura que el archivo
        # se cierre correctamente asi ocurra un error a la mitad de la escritura.
        with open(ruta_archivo, 'w', encoding='utf-8') as archivo:
            json.dump(datos, archivo, indent=4, ensure_ascii=False)
        print(f"[I/O] Datos guardados exitosamente en: {ruta_archivo}")

# ==========================================
# 4. CLASE PRINCIPAL: INGESTOR (POO - Miercoles)
# ==========================================

class IngestorGenerico(AlmacenamientoLocalMixin):
    """
    Clase principal que orquesta la extraccion asincrona y la validacion.
    Hereda del Mixin para obtener la capacidad de guardar en local.
    """
    
    def __init__(self, url: str, modelo_pydantic: Type[BaseModel]):
        self.url = url
        self.modelo_pydantic = modelo_pydantic

    # Asyncio (Tema del Jueves) + Decorador (Martes)
    @medir_tiempo_ejecucion
    async def extraer_datos(self) -> List[Dict[str, Any]]:
        """
        Realiza la peticion HTTP no bloqueante.
        Ideal para operaciones I/O bound (esperar a que la red responda).
        """
        print(f"[Red] Iniciando extraccion asincrona desde: {self.url}")
        
        # Context Manager asincrono para la sesion HTTP
        async with aiohttp.ClientSession() as sesion:
            async with sesion.get(self.url) as respuesta:
                respuesta.raise_for_status() # Falla rapido si el HTTP status no es 200
                datos_crudos = await respuesta.json()
                return datos_crudos

    # Generadores (Tema del Lunes)
    def validar_en_flujo(self, datos_crudos: List[Dict[str, Any]]) -> Generator[Dict[str, Any], None, None]:
        """
        Usa yield para retornar un registro valido a la vez. 
        Si procesaramos 1 millon de registros, esto evita cargar dos copias 
        de la lista entera en la memoria RAM simultaneamente.
        """
        print("[Procesamiento] Iniciando validacion de calidad de datos...")
        for registro in datos_crudos:
            try:
                # Validamos e instanciamos el modelo
                modelo_valido = self.modelo_pydantic(**registro)
                # Retornamos el diccionario limpio y mapeado
                yield modelo_valido.model_dump(by_alias=False)
            except ValidationError as e:
                # En un entorno real, esto iria a una Dead Letter Queue
                print(f"[Error de Calidad] Registro descartado. Motivo: {e.errors()[0]['msg']}")

    # Clean Code (Tema del Sabado): Orquestador simple y legible
    async def ejecutar_pipeline(self, ruta_destino: str):
        """
        Metodo principal que coordina los pasos del ETL.
        Se lee secuencialmente, ocultando la complejidad tecnica interna.
        """
        # 1. Extraccion (I/O Bound)
        datos_crudos = await self.extraer_datos()
        
        # 2. Transformacion/Validacion (CPU Bound ligero)
        # Consumimos el generador y lo convertimos a lista para guardarlo
        datos_limpios = list(self.validar_en_flujo(datos_crudos))
        
        # 3. Carga (I/O Bound)
        self.guardar_json_local(datos_limpios, ruta_destino)
        print(f"[Exito] Pipeline finalizado. {len(datos_limpios)} registros procesados.")

# ==========================================
# EJECUCION (Punto de entrada)
# ==========================================

async def main():
    # Usamos una API publica de pruebas (JSONPlaceholder)
    URL_API = "https://jsonplaceholder.typicode.com/users"
    ARCHIVO_SALIDA = "usuarios_validados.json"
    
    # Instanciamos la clase pasandole la URL y el Modelo de Contrato
    ingestor = IngestorGenerico(url=URL_API, modelo_pydantic=UsuarioAPI)
    
    # Ejecutamos el pipeline asincrono
    await ingestor.ejecutar_pipeline(ruta_destino=ARCHIVO_SALIDA)

if __name__ == "__main__":
    # Event loop principal de asyncio (Jueves)
    asyncio.run(main())