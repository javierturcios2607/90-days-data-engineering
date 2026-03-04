import pytest
from unittest.mock import patch, MagicMock
from src.ingestor import ClimaAPIIngestor


def test_fetch_data_success():
    # Simulamos una respuesta exitosa de la API
    mock_response = {"items": ["lunes", "martes"]}

    # Usamos patch para interceptar la llamada a requests.get
    with patch("src.ingestor.requests.get") as mock_get:
        # Configuramos el mock para que devuelva un objeto con .json() y .status_code
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response

        ingestor = ClimaAPIIngestor("http://api.falsa.com")
        resultado = ingestor.fetch_data()

        assert resultado == mock_response
        mock_get.assert_called_once_with("http://api.falsa.com", timeout=10)


def test_transform_data():
    ingestor = ClimaAPIIngestor("http://api.falsa.com")
    datos_entrada = {"items": ["sol", "nube"]}

    resultado = ingestor.transform_data(datos_entrada)

    assert resultado == ["SOL", "NUBE"]
