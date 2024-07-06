from typing import List, Tuple
import pandas as pd
from collections import Counter

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    # leo el archivo parquet usando la columna mentionUser
    df = pd.read_parquet(file_path, columns=['mentionUser'])
    # elimino las filas que tengan valores nulos
    df = df[df["mentionUser"].isnull() == False]

    # obtengo los 10 usuarios mas mencionados
    pds = df["mentionUser"].explode().to_list()

    return Counter(pds).most_common(10)