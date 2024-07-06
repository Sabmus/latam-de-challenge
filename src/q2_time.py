from typing import List, Tuple
import emoji
import pandas as pd
from collections import Counter

def find_all_emojis(text: str) -> List[str]:
    return [e["emoji"] for e in emoji.emoji_list(text)]

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    # leo el parquet seleccionando solo "content"
    df = pd.read_parquet(file_path, columns=['content'])

    # filtro content nulos
    df = df[df["content"].isnull() == False]

    # reemplazo caracteres no ascii y devuelvo solo emojis y algunos caracteres de lenguas de la india
    # luego aplico find_all_emojis para retornar una lista de emojis
    pds = df["content"].str.replace(r'[\u0900-\u097F]+|[\x00-\x7F]+', '', regex=True).apply(find_all_emojis)

    # finalmente filtro las listas vacias y las exploto para contar los emojis
    pds = pds[pds.str.len() > 0].explode().to_list()

    return Counter(pds).most_common(10)
