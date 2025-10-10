import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

from message_ix_models.util import package_data_path
from message_ix_models.project.geidco25.data_processing.plot_dp import (
    basin_mapping, basin_order)

# ---- Read data -----
model = "MixG_GEIDCO5_SSP2_v6.1"
scen_a = "Base_RCP7_noint_noIBWT_t2"
scen_b = "Base_RCP7_noint_IBWT_t2"

data_a = (
    package_data_path().parents[0]
    / f"reporting_output/{model}_{scen_a}_nexus.csv"
)
data_b = (
    package_data_path().parents[0]
    / f"reporting_output/{model}_{scen_b}_nexus.csv"
)

df_a = pd.read_csv(data_a)
df_b = pd.read_csv(data_b)

df_con = pd.concat([df_a, df_b], ignore_index=True)

# ----- Configuration -----
plt.rcParams.update({
    'font.size': 20,
    'axes.titlesize': 16,
    'axes.labelsize': 16,
    'xtick.labelsize': 14,
    'ytick.labelsize': 14,
    'legend.fontsize': 16,
    'figure.titlesize': 18
})

vars = [
    "Water Withdrawal",
    "Water Resource|Surface Water"
]

scenario_dict = {
    scen_a: 'noIBWT',
    scen_b: 'IBWT'
}

route_target_dict = {
    'route1': 'B35|CHN',
    'route2': 'B162|CHN',
    'route3': 'B35|CHN',
    'route4': 'B162|CHN',
    'route5': 'B162|CHN',
    'route6': 'B62|CHN',
    'route7': 'B162|CHN',
    'route8': 'B62|CHN',
    'route10': 'B159|CHN',
    'route11': 'B62|CHN',
    'route12': 'B62|CHN',
    'route13': 'B148|CHN',
    'route14': 'B162|CHN',
    'route15': 'B96|AFR',
    'route16': 'B96|MEA',
    'route17': 'B97|NAM',
    'route18': 'B125|LAM'
}

supply_basins = ["Yangtze", "Ganges Bramaputra",
                 "Congo", "Mississippi", "Amazon"]
# supply_basins = ['B159|CHN', 'B53|CHN', 'B38|AFR', 'B90|NAM', 'B9|LAM']

# ----- Data Preprocessing -----
# Rename scenario name
df_con['scenario'] = df_con['scenario'].map(scenario_dict)

# Water Tranfer data
df_transfer = df_con[(df_con["variable"].str.startswith("Water Transfer|Interbasin Water Transfer|Existing|route") |
                     df_con["variable"].str.startswith("Water Transfer|Interbasin Water Transfer|Planned|route")) &
                     df_con['region'].str.startswith("B") &
                     (df_con['year'] >= 2030) &
                     (df_con['year'] <= 2060)]

# Split route number
df_transfer["route"] = df_transfer["variable"].str.rsplit("|", n=1).str[-1]
df_transfer['target'] = df_transfer['route'].map(route_target_dict)

# Water export
df_transfer_source = df_transfer.groupby(
    ['model', 'scenario', 'region', 'unit', 'year'], as_index=False)['value'].sum()
df_transfer_source['variable'] = 'water_export'
df_transfer_source['value'] = -df_transfer_source['value']

# Water import
df_transfer_target = df_transfer.groupby(
    ['model', 'scenario', 'target', 'unit', 'year'], as_index=False)['value'].sum().rename(columns={'target': 'region'})
df_transfer_target['variable'] = 'water_import'

# Water Transfer relevant basins
basins = list(pd.unique(df_transfer[["region", "target"]].values.ravel()))
# Filter basins
basins_country = [x for x in basins if "CHN" not in x]

# Filter by water withdrawal and water resource
df_filter = df_con[df_con['variable'].isin(vars) &
                   df_con['region'].isin(basins) &
                   (df_con['year'] >= 2030) &
                   (df_con['year'] <= 2060)]

# Concat ww, wr, wt
df_water = pd.concat([df_filter, df_transfer_source, df_transfer_target],
                     ignore_index=True)


ID_vars = ['scenario', 'region', 'year']
df_wide = (
    df_water.pivot_table(index=ID_vars, columns='variable',
                         values='value', aggfunc='sum')
    .reset_index()
)
df_wide = df_wide.fillna(0)

# ----- Calculate -----
df_wide['Water Availability'] = df_wide['Water Resource|Surface Water'] + \
    df_wide['water_export']+df_wide['water_import']

df_wide['WS'] = df_wide['Water Withdrawal'] / \
    df_wide['Water Availability']

df_ws = df_wide[ID_vars+['WS']]
df_ws_wide = (
    df_ws.pivot_table(index=['region', 'year'], columns='scenario',
                      values='WS', aggfunc='sum')
    .reset_index()
)

df_mean = df_ws_wide.groupby("region")[["IBWT", "noIBWT"]].mean().reset_index()
# Filter by basins
df_mean = df_mean[(df_mean['region'] != 'B96|AFR') &
                  df_mean['region'].isin(basins_country)]
# Rename region name
df_mean['region'] = df_mean['region'].map(basin_mapping)
# Reorder
df_mean["region"] = pd.Categorical(
    df_mean["region"], categories=basin_order, ordered=True)
df_sorted = df_mean.sort_values(by="region").reset_index(drop=True)

# ----- Plotting -----
fig, ax = plt.subplots(figsize=(10, 7))

x_region = np.arange(len(df_sorted["region"]))
width_bar = 0.35
colors = [
    "royalblue" if b in supply_basins else "lightcoral" for b in df_sorted["region"]]

ax.bar(x_region - width_bar/2,
       df_sorted["noIBWT"], width_bar, color=colors, alpha=0.5)
ax.bar(x_region + width_bar/2, df_sorted["IBWT"],
       width_bar, color=colors)

ax.set_ylabel("Annual Water Scarcity Index")
ax.set_xlabel("Basin")
# ax.set_title("Annual WSI")
ax.set_xticks(x_region)
ax.set_xticklabels(df_sorted["region"], rotation=45, ha="right")

legend = [
    Patch(facecolor="gray", alpha=0.5, label="Baseline scenario"),
    Patch(facecolor="gray", label="IBWT scenario"),
    Patch(facecolor="royalblue", label="Water-exporting basin"),
    Patch(facecolor="lightcoral", label="Water-importing basin")
]
ax.legend(handles=legend)

# Output path
output_dir = package_data_path(
).parents[0] / f"reporting_output/plot_WSI/RCP7_2060"
output_dir.mkdir(parents=True, exist_ok=True)

fig_name = "WSI_other region.png"
plt.tight_layout()
plt.savefig(os.path.join(output_dir, fig_name), dpi=300)
plt.show()
