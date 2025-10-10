import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

from message_ix_models.util import package_data_path
from message_ix_models.project.geidco25.data_processing.plot_dp import (
    stan_data_stru, clean_diff_df)

# Increase default font size for all text elements
plt.rcParams.update({
    'font.size': 20,
    'axes.titlesize': 18,
    'axes.labelsize': 16,
    'xtick.labelsize': 16,
    'ytick.labelsize': 16,
    'legend.fontsize': 18,
    'figure.titlesize': 18
})

# ----- Read data for irrigation -----
model = "MixG_GEIDCO5_SSP2_v6.1"
scen_a = "Base_RCP7_noint_noIBWT_t3"
scen_b = "Base_RCP7_noint_IBWT_t3"

file_noibwt = (
    package_data_path().parents[0]
    / f"reporting_output/{model}_{scen_a}_nexus.csv"
)
file_ibwt = (
    package_data_path().parents[0]
    / f"reporting_output/{model}_{scen_b}_nexus.csv"
)
df_no = pd.read_csv(file_noibwt)
df_ib = pd.read_csv(file_ibwt)

# ----- Read data for agricultural production -----
data_file = (
    package_data_path().parents[0]
    / f"reporting_output/report_diff/diff_{scen_a}_{scen_b}.xlsx"
)
df_legacy = pd.read_excel(data_file, sheet_name="differences")

# output path
output_dir = package_data_path(
).parents[0] / f"reporting_output/plot_diff/{scen_a}_{scen_b}/irrigation water withd_2060"
output_dir.mkdir(parents=True, exist_ok=True)

# ----- Configuration -----
variables_irr = [
    "Water Withdrawal|Irrigation|Cereal",
    "Water Withdrawal|Irrigation|Oil Crops",
    "Water Withdrawal|Irrigation|Sugar Crops"
]

variables_ap = [
    "Agricultural Production|Crops|Cereals",
    "Agricultural Production|Crops|Oil Crops",
    "Agricultural Production|Crops|Sugar Crops",
    "Agricultural Production|Crops|Other Crops",
    "Agricultural Production|Crops|Energy Crops"
]

color_map = {
    'Cereals': "#ddad3b",
    'Cereal': "#ddad3b",
    'Oil Crops': "#75a43a",
    'Sugar Crops': "#e4589e",
    'Other Crops': "grey",
    'Energy Crops': "#ca6627"
}

# ----- Data Processing for irrigation -----
df_no = stan_data_stru(df_no)
df_ib = stan_data_stru(df_ib)

# Filter by year
df_no = df_no[(df_no['year'] >= 2030) & (df_no['year'] <= 2060)]
df_ib = df_ib[(df_ib['year'] >= 2030) & (df_ib['year'] <= 2060)]

# Filter by region and variables
# region = ["Middle East and Africa", "Subsaharan Africa"]
# region_name = "Africa"

region = ["China"]
region_name = "China"
filt_no = df_no[(df_no["region"].isin(region)) & (
    df_no["variable"].isin(variables_irr))].copy()
filt_ib = df_ib[(df_ib["region"].isin(region)) & (
    df_ib["variable"].isin(variables_irr))].copy()

if filt_no.empty and filt_ib.empty:
    raise ValueError(
        f"No data found for region={region} and variables={variables_irr}")

# Unit
units = sorted(pd.concat([filt_no["unit"], filt_ib["unit"]], ignore_index=True)
               .dropna().astype(str).unique())
unit_irr = units[0] if len(units) == 1 else "multiple"

# Sum up
g_no = (filt_no.groupby(["year", "variable"], as_index=False)["value"].sum())
g_ib = (filt_ib.groupby(["year", "variable"], as_index=False)["value"].sum())

# Pivot
years_all = sorted(set(g_no["year"]).union(set(g_ib["year"])))
p_no = g_no.pivot(index="year", columns="variable",
                  values="value").reindex(years_all).fillna(0.0)
p_ib = g_ib.pivot(index="year", columns="variable",
                  values="value").reindex(years_all).fillna(0.0)

# Sorted by var_order
for p in (p_no, p_ib):
    for v in variables_irr:
        if v not in p.columns:
            p[v] = 0.0
    p = p
p_no = p_no[variables_irr]
p_ib = p_ib[variables_irr]

# Calculate difference
diff_irr = p_ib - p_no
diff_irr.index.name = "year"

# ----- Data Processing for Agricultural Production -----
df_legacy = clean_diff_df(df_legacy)
df_legacy_long = stan_data_stru(df_legacy)

# Filter by years
df_legacy_long = df_legacy_long[(df_legacy_long['year'] >= 2030) & (
    df_legacy_long['year'] <= 2060)]

# Filter by region and variables
df_ap = df_legacy_long[(df_legacy_long["region"].isin(region)) & (
    df_legacy_long["variable"].isin(variables_ap))].copy()

# Unit
units = sorted(df_ap['unit'].dropna().astype(str).unique())
unit_ap = units[0] if len(units) == 1 else "multiple"

# Pivot
years_all = sorted(set(df_ap["year"]))
p_ap = df_ap.pivot_table(index="year", columns="variable",
                         values="value", aggfunc="sum").reindex(years_all).fillna(0.0)

# Sorted by var_order
p_ap = p_ap[variables_ap]

# ----- Plot for Irrigation -----
years = diff_irr.index.to_numpy()
x = np.arange(len(years))

fig, axes = plt.subplots(2, 1, figsize=(9, 12), sharex=False)

ax = axes[0]
# 正负分开累积，保证正值向上堆叠、负值向下堆叠
pos_cum = np.zeros(len(years))
neg_cum = np.zeros(len(years))

handles = []
labels = []

for var in variables_irr:
    label = var.split("|")[-1]
    vals = diff_irr[var].to_numpy()

    # 对每个年份：正值从 pos_cum 开始，负值从 neg_cum 开始
    bottoms = np.where(vals >= 0, pos_cum, neg_cum)
    h = ax.bar(x, vals, bottom=bottoms, width=0.6,
               label=label, color=color_map[label])

    # 更新累积
    pos_cum = pos_cum + np.where(vals > 0, vals, 0)
    neg_cum = neg_cum + np.where(vals < 0, vals, 0)

    handles.append(h)
    labels.append(var.split("|")[-1])

# 坐标与标注
ax.set_xticks(x, [str(int(y)) if float(y).is_integer() else str(y)
              for y in years])
# ax.set_xlabel("Year")
ax.set_ylabel(f"Irrigation Water Withdrawal ({unit_irr})")
ax.set_title(f"IBWT - baseline ({region_name})")
ax.legend(bbox_to_anchor=(0.5, -0.05),
          loc="upper center",
          ncol=3,
          frameon=False)
ax.axhline(0, linewidth=1)

# ----- Plot for Agricultural Production -----
ax = axes[1]
# 正负分开累积，保证正值向上堆叠、负值向下堆叠
pos_cum = np.zeros(len(years))
neg_cum = np.zeros(len(years))

handles = []
labels = []

for var in variables_ap:
    label = var.split("|")[-1]
    vals = p_ap[var].to_numpy()

    # 对每个年份：正值从 pos_cum 开始，负值从 neg_cum 开始
    bottoms = np.where(vals >= 0, pos_cum, neg_cum)
    h = ax.bar(x, vals, bottom=bottoms, width=0.6,
               label=label, color=color_map[label])

    # 更新累积
    pos_cum = pos_cum + np.where(vals > 0, vals, 0)
    neg_cum = neg_cum + np.where(vals < 0, vals, 0)

    handles.append(h)
    labels.append(var.split("|")[-1])

# 坐标与标注
ax.set_xticks(x, [str(int(y)) if float(y).is_integer() else str(y)
              for y in years])
ax.set_xlabel("Year")
ax.set_ylabel(f"Agricultual Production ({unit_ap})")
# ax.set_title(f"IBWT - baseline ({region})")
ax.legend(bbox_to_anchor=(0.5, -0.13),
          loc="upper center",
          ncol=3,
          frameon=False)
ax.axhline(0, linewidth=1)

plt.tight_layout()
out_png = f"Irri_Agri_{region_name}_diff_ppt.png"
save_path = os.path.join(output_dir, out_png)
plt.savefig(save_path, dpi=300, bbox_inches='tight')
plt.show()
