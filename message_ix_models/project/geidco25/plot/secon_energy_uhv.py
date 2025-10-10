import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from message_ix_models.util import package_data_path
from message_ix_models.project.geidco25.data_processing.plot_dp import (
    stan_data_stru, fill_missing_region, region_mapping)

# Read data
model = "MixG_GEIDCO5_SSP2_v6.1"
scen = "Base_RCP7_int_noIBWT_t2"
data_file = (
    package_data_path().parents[0]
    / f"reporting_output/report_full_t2/{model}_{scen}.xlsx"
)
df = pd.read_excel(data_file, sheet_name="data")
# becasue some bugs when running gei reporting with nexus scenario
df = fill_missing_region(df, col="Region")

scenario = scen
# Output path
output_dir = package_data_path(
).parents[0] / f"reporting_output/plot_ibwt/{scenario}/energy_mix_2060"
output_dir.mkdir(parents=True, exist_ok=True)

# Increase default font size for all text elements
plt.rcParams.update({
    'font.size': 20,
    'axes.titlesize': 18,
    'axes.labelsize': 16,
    'xtick.labelsize': 14,
    'ytick.labelsize': 14,
    'legend.fontsize': 16,
    'figure.titlesize': 18
})

color_map = {
    'China': "#ea7ee5",
    'Eastern Europe': "#eba488",
    'Former Soviet Union': "#85a9e6",
    'Latin America': "#e6dd80",
    'Middle East and Africa': "#e791d0",
    'North America': "#78dde3",
    'Pacific Asia': "#7ae67f",
    'Pacific OECD': "#8d84e2",
    'Rest of Centrally planned Asia': "#e8bf91",
    'South Asia': "#e27888",
    'Subsaharan Africa': "#89e7a5",
    'Western Europe': "#bce777"
}

# ===== data processing =====
df_long = stan_data_stru(df)
# Filter year
df_long = df_long[(df_long['year'] >= 2030) &
                  (df_long['year'] <= 2060)]
# Filter uhv
df_uhv = df_long[df_long["variable"].str.startswith(
    "Secondary Energy|Electricity|UHV|To_")]
df_uhv['receive_region'] = df_uhv["variable"].str.extract(r"\|To_(.+)$")
df_uhv['receive_region'] = df_uhv['receive_region'].map(
    lambda x: region_mapping.get(x, x))

fig, axes = plt.subplots(2, 1, figsize=(12, 14))

# ===== ax1：Electricity Imports =====
ax = axes[0]

# 1. 按 receive_region 和 year 汇总 value
# Remove duplicate
df_uhv = df_uhv[~df_uhv["region"].isin(["World", "Missing region"])]
df_sum = df_uhv.groupby(["receive_region", "year"],
                        as_index=False)["value"].sum()
df_sum = df_sum[df_sum['value'] != 0]

# 2. 透视表：行为 year，列为 receive_region
df_pivot = df_sum.pivot(
    index="year", columns="receive_region", values="value").fillna(0)

# 3. 按年份排序（避免曲线乱跳）
df_pivot = df_pivot.sort_index()

# 4. 绘制 stackplot
years = df_pivot.index
series = df_pivot.T.values  # 每个 receive_region 的数值序列
labels = df_pivot.columns

polys = ax.stackplot(years, series, labels=labels, colors=[
                     color_map[i] for i in labels])

ax.set_xlabel("Year")
ax.set_ylabel("EJ/yr")
ax.set_title("Electricity Imports via UHV")
# Reverse legend order
handles, labels_re = ax.get_legend_handles_labels()
ax.legend(
    [handles[i] for i in reversed(range(len(handles)))],
    [labels[i] for i in reversed(range(len(labels)))],
    loc='upper right'
)

# ===== ax2：Electricity Exports =====
ax = axes[1]

# 1. 按 export_region 和 year 汇总 value
df_sum = df_uhv.groupby(["region", "year"],
                        as_index=False)["value"].sum()
df_sum = df_sum[(df_sum["value"] != 0) &
                (~df_sum["region"].isin(["World", "Missing region"]))]

# 2. 透视表：行为 year，列为 region
df_pivot = df_sum.pivot(
    index="year", columns="region", values="value").fillna(0)

# 3. 按年份排序（避免曲线乱跳）
df_pivot = df_pivot.sort_index()

# 4. 绘制 stackplot

years = df_pivot.index
series = df_pivot.T.values  # 每个 region 的数值序列
labels = df_pivot.columns

polys = ax.stackplot(years, series, labels=labels, colors=[
                     color_map[i] for i in labels])

ax.set_xlabel("Year")
ax.set_ylabel("EJ/yr")
ax.set_title("Electricity Exports via UHV")
# Reverse legend order
handles, labels_re = ax.get_legend_handles_labels()
ax.legend(
    [handles[i] for i in reversed(range(len(handles)))],
    [labels[i] for i in reversed(range(len(labels)))],
    loc='upper right'
)

# save
filename = f"Second_energy_uhv_stackplot.png"
save_path = os.path.join(output_dir, filename)
plt.savefig(save_path, dpi=300, bbox_inches='tight')
plt.show()
