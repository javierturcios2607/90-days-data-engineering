

def filtrar_errores_log(ruta_archivo):
    """
    Lee un archivo de texto masivo línea por línea y filtra aquellas que contienen la palabra 'ERROR'.
    
    Utiliza un generador (yield) para mantener un bajo consumo de memoria (O(1) space complexity),
    evitando cargar el archivo completo en la RAM.
    
    Args:
        ruta_archivo (str): Ruta local o absoluta del archivo a procesar.
        
    Yields:
        str: La línea completa que contiene el patrón de error.
    """
    # Usamos utf-8 para asegurar la compatibilidad con caracteres especiales (ñ, tildes)
    with open(ruta_archivo, 'r', encoding='utf-8') as archivo:
        for linea in archivo:
            # Búsqueda de patrón simple. Para casos complejos, evaluar Regex.
            if 'ERROR' in linea:
                yield linea.strip() # .strip() limpia saltos de línea innecesarios

    
archivo_entrada='dataset_masivo_logs.csv'
archivo_salida='solo_errores.csv'

with open(archivo_salida, 'w', encoding='utf-8') as archivo_destino:
    print(f"Buscando errores en {archivo_entrada}...")

    for linea_con_errores in filtrar_errores_log(archivo_entrada):
        archivo_destino.write(linea_con_errores +'\n')
print(f"¡Proceso terminado! Los errores se han extraído a {archivo_salida}.")
