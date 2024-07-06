from typing import List, Tuple
import emoji
import pandas as pd
from collections import Counter

def find_all_emojis(text: str) -> List[str]:
    return [e["emoji"] for e in emoji.emoji_list(text)]

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    df = pd.read_parquet(file_path, columns=['content'])

    df = df[df["content"].isnull() == False]
    pds = df["content"].str.replace(r'[\u0900-\u097F]+|[\x00-\x7F]+', '', regex=True).apply(find_all_emojis)

    pds = pds[pds.str.len() > 0]
    pds = pds.explode().to_list()

    return Counter(pds).most_common(10)
