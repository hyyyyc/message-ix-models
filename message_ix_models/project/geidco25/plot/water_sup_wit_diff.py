import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

from message_ix_models.util import package_data_path
from message_ix_models.project.geidco25.data_processing.plot_dp import (
    stan_data_stru, clean_diff_df, get_gradient_colors_water, add_industry_water, add_surface_remove_ibwt)

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

# ----- Read data for water -----
model = "MixG_GEIDCO5_SSP2_v6.1"
scen_a = "Base_RCP7_noint_noIBWT_t3"
scen_b = "Base_RCP7_noint_IBWT_t3"

data_file = (
    package_data_path().parents[0]
    / f"reporting_output/report_diff/diff_{scen_a}_nexus_{scen_b}_nexus.xlsx"
)
df = pd.read_excel(data_file, sheet_name="differences")

# output path
output_dir = package_data_path(
).parents[0] / f"reporting_output/plot_diff/{scen_a}_{scen_b}/water_balance_2060"
output_dir.mkdir(parents=True, exist_ok=True)

spatial_scale = 'region'

# ----- Configuration -----
if spatial_scale == 'region':
    supply_ibwt = [
        "Water Extraction|Surface Water Remove IBWT",
        "Water Transfer|Interbasin Water Transfer",
        "Water Extraction|Groundwater",
        "Water Waste|Reuse",
        "Water Extraction|Fossil Groundwater",
        "Water Extraction|Seawater|Desalination"
    ]

    supply_label = [
        'Surface Water',
        'IBWT',
        'Groundwater',
        'Reuse',
        'Fossil Groundwater',
        'Desalination'
    ]

    withdrawal = [
        "Water Withdrawal|Electricity|Cooling|Fresh Water",
        "Water Withdrawal|Irrigation_in",
        "Water Withdrawal|Industrial Water",
        "Water Withdrawal|Municipal Water"
    ]

    withdrawal_label = [
        'Electricity',
        'Irrigation',
        'Industrial Water',
        'Municipal Water'
    ]
if spatial_scale == 'basin':
    supply_ibwt = [
        "Water Extraction|Surface Water",
        # "Water Transfer|Interbasin Water Transfer",
        "Water Extraction|Groundwater",
        "Water Waste|Reuse",
        "Water Extraction|Fossil Groundwater",
        "Water Extraction|Seawater|Desalination"
    ]

    supply_label = [
        'Surface Water',
        # 'IBWT',
        'Groundwater',
        'Reuse',
        'Fossil Groundwater',
        'Desalination'
    ]

    withdrawal = [
        # "Water Withdrawal|Electricity|Cooling|Fresh Water",
        # "Water Withdrawal|Irrigation_in",
        "Water Withdrawal|Industrial Water",
        "Water Withdrawal|Municipal Water"
    ]

    withdrawal_label = [
        # 'Electricity',
        # 'Irrigation',
        'Industrial Water',
        'Municipal Water'
    ]

regions = ['World',
           'China',
           'Eastern Europe',
           'Former Soviet Union',
           'Latin America',
           'Middle East and Africa',
           'North America',
           'Pacific Asia',
           'Pacific OECD',
           'Rest of Centrally planned Asia',
           'South Asia',
           'Subsaharan Africa',
           'Western Europe']

# ----- Data Processing for Water Supply -----
df = clean_diff_df(df)
df_long = stan_data_stru(df)
if spatial_scale == 'region':
    df_long = add_surface_remove_ibwt(df_long)
# df_long = add_industry_water(df_long)

# Filter by years
df_long = df_long[(df_long['year'] >= 2030) & (df_long['year'] <= 2060)]

# Filter by region and variables

region = ["China"]
region_name = "China"

# region = ["Middle East and Africa", "Subsaharan Africa"]
# region_name = "Africa"

# region = ["B162|CHN"]
# region_name = "Ziya He Interior"
df_long_sup = df_long[(df_long["region"].isin(region)) & (
    df_long["variable"].isin(supply_ibwt))].copy()

# Unit
units = sorted(df_long_sup["unit"].dropna().astype(str).unique())
unit = units[0] if len(units) == 1 else "multiple"

# Pivot
years_all = sorted(set(df_long_sup["year"]))
df_sup = df_long_sup.pivot_table(index="year", columns="variable",
                                 values="value", aggfunc="sum").reindex(years_all).fillna(0.0)

# Sorted by var_order
df_sup = df_sup[supply_ibwt]

# ----- Data Processing for Water Withdrawal -----
# Filter by variables
df_long_wit = df_long[(df_long["region"].isin(region)) & (
    df_long["variable"].isin(withdrawal))].copy()

# Pivot
years_all = sorted(set(df_long_wit["year"]))
df_wit = df_long_wit.pivot_table(index="year", columns="variable",
                                 values="value", aggfunc="sum").reindex(years_all).fillna(0.0)

# Sorted by var_order
df_wit = df_wit[withdrawal]

# ----- Plot for supply -----
years = df_sup.index.to_numpy()
x = np.arange(len(years))

fig, axes = plt.subplots(2, 1, figsize=(9, 13), sharex=False)

ax = axes[0]
# 正负分开累积，保证正值向上堆叠、负值向下堆叠
pos_cum = np.zeros(len(years))
neg_cum = np.zeros(len(years))

handles = []
labels = []

# Identify index of Interbasin in supply_table columns
supply_cols = list(df_sup.columns)
highlight_idx = supply_cols.index(
    "Water Transfer|Interbasin Water Transfer")
# Use bright crimson for highlight
highlight_color = (0.988, 0.608, 0.059, 1.0)  # RGBA bright color
supply_cmap = get_gradient_colors_water('Reds', len(
    supply_cols)-1, highlight_idx, highlight_color)

for j, var in enumerate(supply_ibwt):
    vals = df_sup[var].to_numpy()

    # 对每个年份：正值从 pos_cum 开始，负值从 neg_cum 开始
    bottoms = np.where(vals >= 0, pos_cum, neg_cum)
    h = ax.bar(x, vals, bottom=bottoms, width=0.6,
               label=supply_label[j], color=supply_cmap[j])

    # 更新累积
    pos_cum = pos_cum + np.where(vals > 0, vals, 0)
    neg_cum = neg_cum + np.where(vals < 0, vals, 0)

    handles.append(h)
    labels.append(var.split("|")[-1])

# 坐标与标注
ax.set_xticks(x, [str(int(y)) if float(y).is_integer() else str(y)
              for y in years])
# ax.set_xlabel("Year")
ax.set_ylabel(f"Water Supply ({unit})")
ax.set_title(f"IBWT - baseline ({region_name})")
ax.legend(bbox_to_anchor=(0.5, -0.05),
          loc="upper center",
          ncol=2,
          frameon=False)
ax.axhline(0, linewidth=1)

# ----- Plot for Withdrawal -----
ax = axes[1]
# 正负分开累积，保证正值向上堆叠、负值向下堆叠
pos_cum = np.zeros(len(years))
neg_cum = np.zeros(len(years))

handles = []
labels = []

withdrawal_cmap = get_gradient_colors_water(
    'Blues', len(df_wit.columns))

for j, var in enumerate(withdrawal):
    vals = df_wit[var].to_numpy()

    # 对每个年份：正值从 pos_cum 开始，负值从 neg_cum 开始
    bottoms = np.where(vals >= 0, pos_cum, neg_cum)
    h = ax.bar(x, vals, bottom=bottoms, width=0.6,
               label=withdrawal_label[j], color=withdrawal_cmap[j])

    # 更新累积
    pos_cum = pos_cum + np.where(vals > 0, vals, 0)
    neg_cum = neg_cum + np.where(vals < 0, vals, 0)

    handles.append(h)
    labels.append(var.split("|")[-1])

# 坐标与标注
ax.set_xticks(x, [str(int(y)) if float(y).is_integer() else str(y)
              for y in years])
ax.set_xlabel("Year")
ax.set_ylabel(f"Water Withdrawal ({unit})")
# ax.set_title(f"IBWT - baseline ({region})")
ax.legend(bbox_to_anchor=(0.5, -0.13),
          loc="upper center",
          ncol=2,
          frameon=False)
ax.axhline(0, linewidth=1)

plt.tight_layout()
out_png = f"Water_balance_{region_name}_diff_ppt.png"
save_path = os.path.join(output_dir, out_png)
plt.savefig(save_path, dpi=300, bbox_inches='tight')
plt.show()
