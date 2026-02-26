from abc import ABC, abstractmethod

# 1. El Mixin (Inyección de dependencias mediante herencia múltiple)
# Es una clase pequeña, diseñada para añadir una funcionalidad muy específica.
class AlertMixin:
    def send_alert(self, message: str):
        # Aquí conectarías con la API de Slack, SendGrid (Email), PagerDuty, etc.
        print(f"[ALERTA CRÍTICA] 🚨 Notificando al equipo: {message}")


# 2. La Clase Base Abstracta (El Molde)
class BaseETL(ABC):
    
    def run_pipeline(self):#self es la forma en que un objeto se refiere a sí mismo.
        """Método orquestador (Patrón Template Method). Define el flujo estándar."""
        print("--- Iniciando Pipeline ---")
        try:
            raw_data = self.extract()
            clean_data = self.transform(raw_data)
            self.load(clean_data)
            print("--- Pipeline Finalizado con Éxito ---\n")
        except Exception as e:
            # Comprobamos si la clase tiene la capacidad de enviar alertas (via Mixin)
            if hasattr(self, 'send_alert'):#hasattr es una función integrada de Python que significa "has attribute" (¿tiene el atributo?). Se usa para verificar si un objeto tiene un método o una variable específica.
                self.send_alert(f"Fallo en la ejecución: {e}\n")
            else:
                print(f"[ERROR SILENCIOSO] Falló el pipeline y no hay alerta configurada: {e}\n")

    # Definimos los "contratos". Cualquier clase hija ESTÁ OBLIGADA a escribir estos métodos.
    @abstractmethod
    def extract(self):
        pass #Como en Python los bloques de código se definen por sangría, no puedes dejar un método vacío. pass es decirle a Python: "No hagas nada, solo estoy ocupando el espacio para que no de error".

    @abstractmethod
    def transform(self, data):
        pass

    @abstractmethod
    def load(self, data):
        pass


# 3. Clases Específicas (Herencia Múltiple y Polimorfismo)

# Hereda primero de AlertMixin (para inyectar la alerta) y luego de BaseETL (para la estructura)
class ApiETL(AlertMixin, BaseETL):
    def __init__(self, endpoint):
        self.endpoint = endpoint

    def extract(self):
        print(f"Extrayendo datos vía GET desde la API: {self.endpoint}")
        # Simulamos un error de red para ver el Mixin en acción
        raise ConnectionError("Timeout al intentar conectar con el endpoint.")
        return {"raw_json": "data"}

    def transform(self, data):
        print("Aplanando JSON de la API...")
        return data

    def load(self, data):
        print("Insertando datos de la API en la base de datos...")




class DbETL(AlertMixin, BaseETL):
    def __init__(self, table_name):
        self.table_name = table_name

    def extract(self):
        print(f"Ejecutando SELECT * FROM {self.table_name}")
        return ["registro_1", "registro_2"]

    def transform(self, data):
        print("Limpiando nulos y estandarizando tipos de datos...")
        return [registro.upper() for registro in data]

    def load(self, data):
        print("Haciendo UPSERT de los datos transformados en BigQuery...")


"""
LECCIÓN DE INTROSPECCIÓN PARA ETL:
Uso de la tríada de inspección de objetos:
    - hasattr: Verifica existencia (Pregunta).
    - getattr: Recupera el miembro (Dámelo).
    - setattr: Asigna o sobrescribe (Ponle esto).
"""