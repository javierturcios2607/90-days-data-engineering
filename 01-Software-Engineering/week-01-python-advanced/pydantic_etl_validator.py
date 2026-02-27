"""
Módulo de Validación de Datos con Pydantic para Pipelines ETL
Parte del Plan de Aprendizaje de 90 Días - Data Engineering

Descripción:
Este script demuestra cómo utilizar Pydantic V2 como una barrera de calidad de datos
(Data Quality Gate) antes de ingerir información en un Data Warehouse. 
Implementa validaciones de tipos estrictos, coerción de datos, asignación de valores 
por defecto y validaciones de reglas de negocio personalizadas.

Dependencias necesarias:
pip install pydantic pydantic[email]
"""

from typing import Optional, List, Dict, Any
from datetime import date
from pydantic import BaseModel, Field, EmailStr, field_validator, ValidationError
import json

# ==========================================
# 1. DEFINICIÓN DEL CONTRATO DE DATOS
# ==========================================

class TransaccionVentas(BaseModel):
    """
    Modelo de datos que define la estructura esperada y las reglas de validación
    para los registros de transacciones de ventas entrantes.
    Al heredar de BaseModel, Pydantic asume el control de la instanciación.
    """
    
    # Validaciones de tipo base:
    # Pydantic intentará realizar un "casting" seguro. Si recibe un string "123",
    # lo convertirá automáticamente a entero. Si recibe "abc", lanzará un error.
    transaccion_id: int = Field(
        ..., 
        description="Identificador único de la transacción. Obligatorio."
    )
    
    # Validación de dominios específicos:
    # EmailStr valida internamente que la cadena cumpla con el estándar RFC 5322.
    # Evita que tengamos que escribir y mantener expresiones regulares complejas.
    email_cliente: EmailStr = Field(
        ..., 
        description="Correo electrónico del cliente. Debe tener un formato válido."
    )
    
    # Monto de la transacción. Usamos Field para agregar reglas de validación en línea.
    # ge=0.01 significa "greater than or equal to 0.01" (no se permiten montos negativos o en cero).
    monto_usd: float = Field(
        ..., 
        ge=0.01, 
        description="Monto de la venta en dólares. Debe ser mayor a cero."
    )
    
    # Coerción de fechas:
    # Acepta strings en formato ISO (YYYY-MM-DD) y los convierte a objetos datetime.date.
    fecha_transaccion: date = Field(
        ..., 
        description="Fecha en la que ocurrió la transacción."
    )
    
    # Manejo de valores nulos y por defecto:
    # Optional[str] indica que el campo puede venir como null (None en Python).
    # Al asignarle un valor, le decimos a Pydantic qué poner si la llave no existe en el JSON.
    metodo_pago: str = Field(
        default="NO_ESPECIFICADO", 
        description="Método utilizado para pagar."
    )
    notas_adicionales: Optional[str] = Field(
        default=None, 
        description="Comentarios opcionales de la venta."
    )

    # ==========================================
    # 2. VALIDACIONES DE NEGOCIO PERSONALIZADAS
    # ==========================================

    @field_validator('fecha_transaccion')
    @classmethod
    def validar_fecha_no_futura(cls, valor_fecha: date) -> date:
        """
        Validador personalizado que asegura que las transacciones no tengan 
        fechas en el futuro. Esto previene errores de sincronización de relojes 
        en los sistemas de origen.
        """
        if valor_fecha > date.today():
            raise ValueError('Data Quality Error: La fecha de la transacción no puede ser posterior al día de hoy.')
        return valor_fecha
    
    @field_validator('metodo_pago')
    @classmethod
    def estandarizar_metodo_pago(cls, valor_metodo: str) -> str:
        """
        Validador que actúa como transformador ligero (Data Cleansing).
        Asegura que todos los métodos de pago entren al Data Warehouse 
        en mayúsculas y sin espacios en blanco accidentales.
        """
        return valor_metodo.strip().upper()


# ==========================================
# 3. SIMULACIÓN DE FLUJO ETL
# ==========================================

def procesar_lote_extraccion(datos_crudos: List[Dict[str, Any]]) -> tuple:
    """
    Simula la fase de Transformación de un pipeline ETL.
    Itera sobre los datos crudos, aplicando el modelo de Pydantic para separar
    los registros limpios de los registros anómalos (Dead Letter Queue).
    
    Args:
        datos_crudos: Lista de diccionarios que representan el JSON de origen.
        
    Returns:
        Una tupla que contiene dos listas: (registros_validos, registros_cuarentena)
    """
    
    registros_validos = []
    registros_cuarentena = []

    for registro in datos_crudos:
        try:
            # Intentamos instanciar el modelo desempaquetando el diccionario.
            # Aquí es donde Pydantic aplica todas las reglas definidas arriba.
            registro_limpio = TransaccionVentas(**registro)
            
            # Si pasa la validación, extraemos los datos estandarizados a un diccionario
            # model_dump() es el estándar en Pydantic V2 (reemplaza a dict())
            registros_validos.append(registro_limpio.model_dump())
            
        except ValidationError as error_pydantic:
            # Capturamos la excepción de validación sin romper la ejecución del bucle.
            # Estructuramos un payload de error para auditoría.
            error_estructurado = {
                "payload_original": json.dumps(registro),
                "motivos_rechazo": error_pydantic.errors(),
                "falla_en_pipeline": "Pydantic Data Quality Gate"
            }
            registros_cuarentena.append(error_estructurado)

    return registros_validos, registros_cuarentena


# ==========================================
# 4. EJECUCIÓN PRINCIPAL Y PRUEBAS
# ==========================================

if __name__ == "__main__":
    
    # Simulamos un payload (JSON) extraído de una API o un Bucket,
    # con diferentes tipos de errores comunes en la vida real.
    lote_json_sucio = [
        # Registro 1: Correcto. ID viene como string pero Pydantic lo hará entero.
        {
            "transaccion_id": "1001",
            "email_cliente": "usuario1@empresa.com",
            "monto_usd": 150.50,
            "fecha_transaccion": "2023-10-15",
            "metodo_pago": "   tarjeta_credito  " # Espacios y minúsculas
        },
        # Registro 2: Error. El email no tiene formato válido.
        {
            "transaccion_id": 1002,
            "email_cliente": "usuario2_sin_arroba.com",
            "monto_usd": 50.00,
            "fecha_transaccion": "2023-10-15"
        },
        # Registro 3: Error múltiple. Monto negativo y fecha en el futuro (asumiendo hoy < 2099).
        {
            "transaccion_id": 1003,
            "email_cliente": "usuario3@empresa.com",
            "monto_usd": -10.00,
            "fecha_transaccion": "2099-12-31"
        },
        # Registro 4: Correcto parcial. Faltan campos opcionales, Pydantic asignará defaults.
        {
            "transaccion_id": 1004,
            "email_cliente": "usuario4@empresa.com",
            "monto_usd": 99.99,
            "fecha_transaccion": "2023-10-16"
        }
    ]

    print("Iniciando procesamiento de lote de datos...")
    print("-" * 50)
    
    datos_buenos, datos_malos = procesar_lote_extraccion(lote_json_sucio)
    
    print(f"Total de registros procesados: {len(lote_json_sucio)}")
    print(f"Registros válidos (Listos para Data Warehouse): {len(datos_buenos)}")
    print(f"Registros en cuarentena (Enviados a DLQ): {len(datos_malos)}")
    print("-" * 50)
    
    print("\nEjemplo de registro válido transformado:")
    # Nota como el ID es un entero, la fecha un objeto y el método de pago está en mayúsculas.
    print(datos_buenos[0])
    
    print("\nEjemplo de registro en cuarentena detallado:")
    # Mostramos por qué falló el registro con el correo malo.
    print(json.dumps(datos_malos[0], indent=2))