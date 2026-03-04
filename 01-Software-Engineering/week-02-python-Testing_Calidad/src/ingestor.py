import requests


class ClimaAPIIngestor:
    def __init__(self, api_url: str):
        self.api_url = api_url

    def fetch_data(self) -> dict:
        """Obtiene datos de una API externa con manejo de errores."""
        try:
            response = requests.get(self.api_url, timeout=10)
            response.raise_for_status()  # Lanza excepción si hay error 4xx o 5xx
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error de red: {e}")
            return {}

    def transform_data(self, data: dict) -> list:
        """Lógica simple para limpiar los datos (ejemplo)."""
        if not data or "items" not in data:
            return []
        # Convierte a mayúsculas cada item en la lista
        return [item.upper() for item in data.get("items", [])]
