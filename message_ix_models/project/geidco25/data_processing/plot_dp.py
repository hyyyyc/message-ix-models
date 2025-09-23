import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re
from typing import List, Sequence, Union, Dict

region_mapping = {
    'CHN': 'China',
    'EEU': 'Eastern Europe',
    'FSU': 'Former Soviet Union',
    'LAM': 'Latin America',
    'MEA': 'Middle East and Africa',
    'NAM': 'North America',
    'PAS': 'Pacific Asia',
    'PAO': 'Pacific OECD',
    'RCPA': 'Rest of Centrally planned Asia',
    'SAS': 'South Asia',
    'AFR': 'Subsaharan Africa',
    'WEU': 'Western Europe'
}

basin_mapping = {
    'B159|CHN': 'Yangtze',
    'B35|CHN': 'China Coast',
    'B162|CHN': 'Ziya He Interior',
    'B62|CHN': 'Huang He',
    'B38|AFR': 'Congo',
    'B96|AFR': 'Nile',
    'B96|MEA': 'Nile',
    'B53|CHN': 'Ganges Bramaputra',
    'B148|CHN': 'Tarim Interior',
    'B90|NAM': 'Mississipy',
    'B97|NAM': 'Colorado',
    'B9|LAM': 'Amazon',
    'B125|LAM': 'Sao Francisco'
}

basin_order = ['Yangtze', 'Ganges Bramaputra', 'Huang He', 'Ziya He Interior',
               'China Coast', 'Tarim Interior',
               'Congo', 'Nile',
               'Mississipy', 'Colorado',
               'Amazon', 'Sao Francisco']


def stan_data_stru(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize data structure:
    1. Standardize column names
    2. Wide to long
    3. Change data type
    4. Convert unit
    5. Standardize region name
    """
    # lowercase first letter
    new_columns = []
    for col in df.columns:
        if isinstance(col, str) and col and col[0].isupper():
            new_columns.append(col[0].lower() + col[1:])
        else:
            new_columns.append(col)
    df.columns = new_columns

    # wide to long, if necessary
    if "value" not in df.columns:
        id_list = ['model', 'scenario', 'region', 'variable', 'unit']
        # id in df
        df_cols_id = [c for c in df.columns if c in id_list]
        df_cols_yr = [x for x in df.columns if x not in df_cols_id]

        # Wide to long
        df_long = df.melt(
            id_vars=df_cols_id,
            value_vars=df_cols_yr,
            var_name='year',
            value_name='value'
        )
    else:
        df_long = df

    # confirm data type for 'year' as int
    df_long['year'] = df_long['year'].astype(int)

    # MCM/yr to km3/yr
    mask = df_long['unit'] == 'MCM/yr'
    df_long.loc[mask, 'value'] = df_long.loc[mask, 'value'] / 1000
    df_long.loc[mask, 'unit'] = 'km3/yr'

    # Standardize region name
    # Remove "R12_"
    df_long["region"] = df_long["region"].str.replace(r"^R12_", "", regex=True)

    df_long['region'] = df_long['region'].map(
        lambda x: region_mapping.get(x, x))

    return df_long


def filter_series_labels_colors(energy_df, energy_vars, labels):
    """
    return non-zero records
    series_list、labels_list
    """
    valid_idx = []
    for i, var in enumerate(energy_vars):
        vals = energy_df[var].to_numpy(
        ) if var in energy_df.columns else np.array([])
        # abs>0 -> non-zero
        if np.nansum(np.abs(vals)) > 0:
            valid_idx.append(i)

    series_list = [energy_df[energy_vars[i]].to_numpy() for i in valid_idx]
    labels_list = [labels[i] for i in valid_idx]
    return series_list, labels_list


def get_colors(cmap_name, n):
    # Prepare colors and labels
    cmap = plt.get_cmap(cmap_name)
    return [cmap(i / (n - 1)) for i in range(n)]


def fill_missing_region(df: pd.DataFrame, col: str = "Region", new_reg: str = "Missing region") -> pd.DataFrame:
    """
    Fill missing region in gei reporting
    """
    df[col] = df[col].fillna(new_reg)
    return df


def add_noGEI_vars(
    df: pd.DataFrame,
    techs: List[str] = None,
    base_prefix: str = "Secondary Energy|Electricity",
    variable_col: str = "Variable",
    id_cols: Sequence[str] = ("Model", "Region", "Scenario", "Unit"),
    gei_suffix_map: Dict[str, List[str]] = None,
) -> pd.DataFrame:
    """
    按 (Model, Region, Scenario, Unit) 对齐，对每个技术添加 noGEI 行：
      {base_prefix}|{tech}|noGEI = {base_prefix}|{tech} - {base_prefix}|{tech}|<GEI后缀...>

    参数
    ----
    df : 原始数据（含年份列、variable、以及对齐键列）
    techs : 要处理的技术列表，默认 ["Hydro","Wind","Solar"]
    base_prefix : 变量前缀，默认 "Secondary Energy|Electricity"
    variable_col : 变量列名，默认 "variable"
    id_cols : 对齐键列，默认 ("Model","Region","Scenario","Unit")
    gei_suffix_map : 为每个技术指定 GEI 的“后缀路径”，例如：
        {
          "Hydro": ["GEI"],
          "Wind":  ["GEI"],
          "Solar": ["PV","GEI"]   # => ...|Solar|PV|GEI
        }

    返回
    ----
    新的 DataFrame，包含追加的 *|noGEI 行
    """
    if techs is None:
        techs = ["Hydro", "Wind", "Solar"]
    if gei_suffix_map is None:
        gei_suffix_map = {"Hydro": ["GEI"],
                          "Wind": ["GEI"],
                          "Solar": ["PV", "GEI"]}

    # 年份列（4位数字，int 或 str 均可）
    year_cols: List[Union[str, int]] = [
        c for c in df.columns if re.fullmatch(r"\d{4}", str(c))]
    if not year_cols:
        raise ValueError("未找到年份列（需要形如 2010, 2015, ... 的4位数字列名）。")

    # 基础检查
    missing_id = [c for c in id_cols if c not in df.columns]
    if missing_id:
        raise KeyError(f"缺少对齐键列: {missing_id}")
    if variable_col not in df.columns:
        raise KeyError(f"找不到变量列 `{variable_col}`")

    new_rows = []

    for tech in techs:
        base_var = f"{base_prefix}|{tech}"
        gei_suffix = gei_suffix_map.get(tech, ["GEI"])
        gei_var = f"{base_var}|{'|'.join(gei_suffix)}"
        no_var = f"{base_prefix}|{tech}|noGEI"

        base_df = df[df[variable_col] == base_var].copy()
        if base_df.empty:
            # 没有基础变量则跳过该技术
            continue

        gei_df = df[df[variable_col] == gei_var][[*id_cols, *year_cols]].copy()
        gei_df = gei_df.rename(columns={y: f"{y}_GEI" for y in year_cols})

        # 按 id_cols 左连接，确保逐 Model/Region/Scenario/Unit 匹配
        merged = base_df.merge(gei_df, on=list(id_cols), how="left")

        # 逐年相减，缺失按 0
        for y in year_cols:
            merged[y] = pd.to_numeric(merged[y], errors="coerce").fillna(0)
            gy = f"{y}_GEI"
            merged[gy] = pd.to_numeric(
                merged.get(gy), errors="coerce").fillna(0)
            merged[y] = merged[y] - merged[gy]

        merged[variable_col] = no_var
        merged = merged.drop(
            columns=[f"{y}_GEI" for y in year_cols if f"{y}_GEI" in merged.columns])

        # 列顺序与原 df 一致
        merged = merged[df.columns]
        new_rows.append(merged)

    if not new_rows:
        return df.copy()

    return pd.concat([df] + new_rows, ignore_index=True)


def clean_diff_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # remove A and B column
    cols_to_drop = [col for col in df.columns if col.endswith(("A", "B"))]
    df.drop(columns=cols_to_drop, inplace=True)

    # rename diff column
    new_columns = {col: col.replace("_diff", "")
                   for col in df.columns if col.endswith("diff")}
    df.rename(columns=new_columns, inplace=True)

    return df


def get_gradient_colors_water(cmap_name, n, highlight_index=None, highlight_color=None):
    '''
    Prepare color gradients with special color for water extraction and withdrawal.
    If highlight_index and highlight_color are given, insert highlight_color
    at the specified index and shift the rest of the colors.
    '''
    cmap = plt.get_cmap(cmap_name)
    colors = [cmap(0.3 + 0.7 * i / (n - 1)) for i in range(n)]

    if highlight_index is not None and highlight_color is not None:
        # insert highlight_color, shift the later color down the sequence
        colors.insert(highlight_index, highlight_color)

    return colors


def add_industry_water(df: pd.DataFrame) -> pd.DataFrame:
    # Add up variables
    ind_vars = [
        "Water Withdrawal|Industrial Water|Unconnected Eff",
        "Water Withdrawal|Industrial Water|Unconnected",
    ]
    df_ind = df[df["variable"].isin(ind_vars)].copy()

    if not df_ind.empty:
        # group by except variable, value
        group_cols = [c for c in df_ind.columns if c not in [
            "variable", "value"]]
        df_ind_sum = (
            df_ind.groupby(group_cols, as_index=False, sort=False)["value"]
            .sum()
            .assign(variable="Water Withdrawal|Industrial Water")
        )
        # substitute
        df = pd.concat(
            [df[~df["variable"].isin(ind_vars)], df_ind_sum],
            ignore_index=True
        )
    return df
