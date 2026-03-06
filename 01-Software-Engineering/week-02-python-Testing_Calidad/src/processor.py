from typing import List, Dict, Optional, Union


def clean_accounting_record(
    record_id: int, amount: float, description: Optional[str] = None
) -> Dict[str, Union[int, float, str]]:
    """
    Procesa un registro contable con tipado estricto.
    MyPy validará que siempre devuelvas un diccionario con estos tipos.
    """
    cleaned_desc = description.strip() if description else "SIN_DESCRIPCION"

    return {
        "id": record_id,
        "total": round(amount, 2),
        "status": "processed",
        "desc": cleaned_desc,
    }


def process_batch(records: List[float]) -> float:
    """Calcula el total de un lote de montos."""
    return sum(records)


if __name__ == "__main__":
    # Esto pasará MyPy perfectamente
    resultado = clean_accounting_record(101, 1500.505, " Pago Proveedor ")
    print(f"Registro procesado: {resultado}")
