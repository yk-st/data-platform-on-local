import re
import unicodedata
from typing import Mapping

# 共通の表記ゆれマップ
JITTER_MAP: Mapping[str, str] = {
    "表": "票",
    # ... 他の共通置換 ...
}

def replace_jitter(
    col_name: str,
    replace_map: Mapping[str, str] = JITTER_MAP,
    exact_match: bool = True
) -> str:
    """
    • exact_match=True のときは「列名全体が before と完全一致」する場合だけ置換
    • exact_match=False のときは部分一致で置換
    """
    for before, after in replace_map.items():
        if exact_match:
            # ^...$ で完全一致だけをマッチ
            pattern = rf"^{re.escape(before)}$"
            col_name = re.sub(pattern, after, col_name)
        else:
            # 部分一致で置換
            col_name = re.sub(re.escape(before), after, col_name)
    return col_name

def sanitize_column_name(
    col_name: str,
    extra_jitter_map: Mapping[str, str] | None = None,
    exact_match: bool = True
) -> str:
    # 1) 全角英数字→半角
    s = unicodedata.normalize("NFKC", col_name)

    # 2) 長音符を除去
    #s = re.sub(r"[ー－]", "", s)

    # 3) その他の不要記号をアンダースコアに
    s = re.sub(r"[()（）・／\-/]", "_", s)

    # 6) 連続するアンダースコアを 1 つにまとめる
    s = re.sub(r"_+", "_", s)

    # 7) 先頭・末尾のアンダースコアを削除
    s = s.strip("_")

    # 4) 個別置換（完全一致）
    if extra_jitter_map:
        s = replace_jitter(s, replace_map=extra_jitter_map, exact_match=exact_match)

    # 5) 共通置換（完全一致）
    s = replace_jitter(s, replace_map=JITTER_MAP, exact_match=True)

    return s
