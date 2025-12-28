import glob
import os
from typing import Iterable, Tuple, List

def iter_text_files(input_dir: str = "data_text") -> Iterable[Tuple[str, str]]:
    for src in ("wikipedia_en", "marinelink"):
        d = os.path.join(input_dir, src)
        for fp in glob.glob(os.path.join(d, "*.txt")):
            yield src, fp

def list_docs(input_dir: str = "data_text") -> List[Tuple[str, str]]:
    docs = list(iter_text_files(input_dir))
    docs.sort(key=lambda x: x[1])
    return docs
