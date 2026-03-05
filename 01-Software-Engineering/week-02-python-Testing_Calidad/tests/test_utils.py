import pytest
from src.utils import clean_column_name

def test_clean_column_name():
    # El test define el comportamiento deseado
    assert clean_column_name(" Nombre Usuario ") == "nombre_usuario"
    assert clean_column_name("Fecha-Venta") == "fecha_venta"