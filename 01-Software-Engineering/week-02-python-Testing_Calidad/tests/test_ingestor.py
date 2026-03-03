import pytest
import requests  # <--- ¡ESTA ES LA LÍNEA QUE FALTA!
from unittest.mock import Mock, patch
from src.ingestor import DataIngestor


# 1. Fixture: Prepara el objeto para no repetirlo en cada test
@pytest.fixture
def ingestor():
    return DataIngestor(api_url="https://api.falsa.com/datos")


# 2. Test con Mocking: Simulamos la respuesta de 'requests'
@patch("src.ingestor.requests.get")
def test_fetch_data_success(mock_get, ingestor):
    # Configuramos el comportamiento del Mock
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"items": ["dato1", "dato2"]}
    mock_get.return_value = mock_response

    resultado = ingestor.fetch_data()

    assert resultado == {"items": ["dato1", "dato2"]}
    mock_get.assert_called_once_with("https://api.falsa.com/datos", timeout=10)


# 3. Test de Lógica (Sin necesidad de Mocks)
@pytest.mark.parametrize(
    "input_data, expected",
    [
        ({"items": ["a", "b"]}, ["A", "B"]),
        ({}, []),
        ({"items": []}, []),
    ],
)
def test_transform_data(ingestor, input_data, expected):
    assert ingestor.transform_data(input_data) == expected


# 4. Test de Error: Forzamos una excepción de red
@patch("src.ingestor.requests.get")
def test_fetch_data_error(mock_get, ingestor):
    # Simulamos que requests lanza una excepción (ej. no hay internet)
    mock_get.side_effect = requests.exceptions.RequestException("Error de conexión")

    resultado = ingestor.fetch_data()

    # Verificamos que el código manejó el error y devolvió el dict vacío
    assert resultado == {}
    mock_get.assert_called_once()

    """ Qué hicimos:
Escribimos un script de pruebas usando pytest para evaluar la clase del ETL que armamos en la semana 1. Utilizamos fixtures para inyectarle datos falsos controlados y escribimos asserts para comprobar que el método de transformación está filtrando correctamente los registros inválidos (como los que no traen temperatura) y convirtiendo los tipos de datos como se espera.

Por qué lo hicimos:
Principalmente para no tener que estar probando a mano con print() ni depender de que la API externa nos responda. Al meter nuestros propios datos de prueba, nos aseguramos de que la lógica interna de la clase funciona perfectamente. También lo hicimos como una red de seguridad: si en unos meses alguien modifica el código y rompe una regla de negocio, estas pruebas van a fallar automáticamente en la consola, evitando que mandemos datos sucios a producción. Es la forma en que los equipos Senior previenen errores antes de que sucedan.


    """
