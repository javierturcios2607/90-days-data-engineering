"""
Dia 06: Principios de Clean Code aplicados a Ingenieria de Datos
Plan de Aprendizaje de 90 Dias

Descripcion:
Este modulo demuestra la refactorizacion de un script ETL basico.
Compara un enfoque monolitico y dificil de mantener (Anti-patron) 
con un enfoque estructurado, modular y tipado (Clean Code).
"""

from typing import List, Dict, Any

# ==========================================
# EL ANTI-PATRON (Codigo Junior / Sucio)
# ==========================================
# Problemas:
# 1. Nombres de variables incomprensibles (d, res, st, amt).
# 2. La funcion hace multiples cosas (filtra, calcula, transforma).
# 3. No hay Type Hints ni docstrings.
# 4. Uso de "Magic Numbers" (0.13) sin explicacion.

def process(data):
    res = []
    for d in data:
        if d.get('st') == 'A' and d.get('amt', 0) > 0:
            tax = d['amt'] * 0.13
            total = d['amt'] + tax
            d['tax'] = round(tax, 2)
            d['total'] = round(total, 2)
            res.append(d)
    return res


# ==========================================
# EL ENFOQUE SENIOR (Clean Code)
# ==========================================
# Soluciones aplicadas:
# 1. Nombres que revelan intencion.
# 2. Single Responsibility Principle (SRP): Funciones pequenas y enfocadas.
# 3. Type Hints para predecir entradas y salidas.
# 4. Constantes para evitar numeros magicos.

TASA_IVA_SV = 0.13  # Constante global clara en lugar de un numero magico

def filtrar_transacciones_validas(transacciones: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filtra las transacciones activas con montos mayores a cero."""
    return [
        trx for trx in transacciones 
        if trx.get('estado') == 'ACTIVO' and trx.get('monto_usd', 0) > 0
    ]

def calcular_impuestos_y_totales(transaccion: Dict[str, Any]) -> Dict[str, Any]:
    """Calcula el IVA y el total, retornando una nueva transaccion enriquecida."""
    # Evitamos mutar el diccionario original (inmutabilidad) creando una copia
    trx_enriquecida = transaccion.copy()
    
    monto_base = trx_enriquecida['monto_usd']
    impuesto = monto_base * TASA_IVA_SV
    
    trx_enriquecida['impuesto_usd'] = round(impuesto, 2)
    trx_enriquecida['total_usd'] = round(monto_base + impuesto, 2)
    
    return trx_enriquecida

def procesar_pipeline_ventas(datos_crudos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Funcion orquestadora. Define el flujo de datos paso a paso.
    Se lee como un libro, no hace falta descifrar que hace internamente.
    """
    transacciones_validas = filtrar_transacciones_validas(datos_crudos)
    
    datos_procesados = [
        calcular_impuestos_y_totales(trx) 
        for trx in transacciones_validas
    ]
    
    return datos_procesados


# ==========================================
# EJECUCION Y PRUEBA
# ==========================================

if __name__ == "__main__":
    # Simulacion de datos extraidos de un sistema fuente (origen)
    lote_ventas_crudas = [
        {"id": 1, "estado": "ACTIVO", "monto_usd": 100.00},
        {"id": 2, "estado": "INACTIVO", "monto_usd": 50.00}, # Sera filtrado
        {"id": 3, "estado": "ACTIVO", "monto_usd": -10.00},  # Sera filtrado (monto invalido)
        {"id": 4, "estado": "ACTIVO", "monto_usd": 250.50}
    ]

    print("--- Procesando con Anti-patron ---")
    # Adaptamos el payload sucio para que el script viejo lo entienda
    datos_viejos = [{"st": "A", "amt": 100}, {"st": "I", "amt": 50}]
    print(process(datos_viejos))

    print("\n--- Procesando con Clean Code ---")
    resultados_limpios = procesar_pipeline_ventas(lote_ventas_crudas)
    for registro in resultados_limpios:
        print(registro)