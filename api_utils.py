import numpy as np
import pandas as pd
from decimal import Decimal

def make_serializable(obj):
    """
    Рекурсивно обходит структуру данных и заменяет несовместимые с JSON
    значения (numpy-типы, inf, nan, Decimal) на безопасные Python-типы.
    (Этот код взят из твоего файла api_utils.py)
    """
    if isinstance(obj, dict):
        return {key: make_serializable(val) for key, val in obj.items()}
    elif isinstance(obj, list):
        return [make_serializable(item) for item in obj]
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, (np.integer, np.floating)):
        val = obj.item()
        if pd.isna(val) or np.isinf(val):
            return None
        return val
    elif isinstance(obj, float):
        if pd.isna(obj) or np.isinf(obj):
            return None
        return obj
    else:
        return obj
