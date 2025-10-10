import glob
import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import gridspec
import numpy as np

from message_ix_models.util import package_data_path
from message_ix_models.project.geidco25.data_processing.plot_dp import (
    stan_data_stru, fill_missing_region, basin_mapping, supply_basins, basin_order)

# Increase default font size for all text elements
plt.rcParams.update({
    'font.size': 11,
    'axes.titlesize': 13,
    'axes.labelsize': 11,
    'xtick.labelsize': 11,
    'ytick.labelsize': 11,
    'legend.fontsize': 13,
    'figure.titlesize': 13
})

# ----- Configuration -----
regions = [
    'World',
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
    'Western Europe'
]

scenario_dict = {
    'Base_RCP7_noint_noIBWT_t2': 'noIBWT',
    'Base_RCP7_noint_IBWT_t2': 'IBWT',
    'Base_RCP7_int_noIBWT_t2': 'GEI'
}

# ----- Data for energy price -----
file_list_ene = glob.glob(
    r"D:\IIASA\Model\message-ix-models\message_ix_models\reporting_output\report_full_t2\MixG*.xlsx")

# Read all files
dfs_ene = []
for f in file_list_ene:
    df = pd.read_excel(f, sheet_name="data")
    dfs_ene.append(df)

# Concat
if dfs_ene:
    df_ene = pd.concat(dfs_ene, ignore_index=True)
else:
    df_ene = pd.DataFrame()

# Data preprocessing
df_ene = fill_missing_region(df_ene, col="Region")
df_long_ene = stan_data_stru(df_ene)

# ----- Data for water price -----
# file_list_water = glob.glob(
#     r"D:\IIASA\Model\message-ix-models\message_ix_models\reporting_output\*t2_nexus.csv")

file_water_path = r"D:\IIASA\Model\message-ix-models\message_ix_models\reporting_output"
file_list_water = ["MixG_GEIDCO5_SSP2_v6.1_Base_RCP7_noint_noIBWT_t2_nexus.csv",
                   "MixG_GEIDCO5_SSP2_v6.1_Base_RCP7_noint_IBWT_t2_nexus.csv"]

# Read all files
dfs_water = []
for f in file_list_water:
    df = pd.read_csv(os.path.join(file_water_path, f))
    # df = pd.read_csv(f)
    dfs_water.append(df)

# Concat
if dfs_water:
    df_water = pd.concat(dfs_water, ignore_index=True)
else:
    df_water = pd.DataFrame()

# Data preprocessing
df_long_water = stan_data_stru(df_water)

# ----- Output -----
output_dir = package_data_path(
).parents[0] / f"reporting_output/plot_diff/compare_scen_t2/price_annual_2060"
output_dir.mkdir(parents=True, exist_ok=True)


def plot_by_scenario(df_long, var_name, fname_prefix=None):
    for region in regions:
        df_base = df_long[df_long['region'] == region]
        sub = df_base[df_base["variable"] == var_name].copy()
        if sub.empty:
            print(f"No variable: {var_name}")
            return

        sub = sub.dropna(subset=["value"])
        # Filter by year
        sub = sub[sub["year"] >= 2030]
        sub = sub[sub["year"] <= 2060]
        # Unit
        unit_vals = sub["unit"].dropna().astype(str).unique()
        unit_str = unit_vals[0] if len(unit_vals) > 0 else ""

        # Group by scenarios
        scenarios = sub["scenario"].dropna().unique().tolist()
        scenarios = sorted(scenarios, key=lambda x: str(x))

        plt.figure(figsize=(8, 5.2))
        used_labels = set()
        for sc in scenarios:
            ss = sub[sub["scenario"] == sc].sort_values("year")
            if ss["value"].notna().sum() == 0:
                continue

            label = str(sc)
            if label.endswith("_t2"):
                label = label[:-3]  # Remove "_tx"

            if label in used_labels:
                plt.plot(ss["year"], ss["value"], label="_nolegend_")
            else:
                plt.plot(ss["year"], ss["value"], label=label)
                used_labels.add(label)

        plt.xlabel("Year")
        plt.ylabel(f"Price ({unit_str})" if unit_str else "Value")
        plt.title(f"{var_name} ({region})")
        # Legend
        fig = plt.gcf()
        fig.tight_layout(rect=(0, 0, 0.78, 1))

        plt.legend(
            loc="upper left",
            bbox_to_anchor=(1.02, 1),
            borderaxespad=0.0,
            fontsize=12,
            frameon=False
        )

        # Save
        if fname_prefix is None:
            fname_prefix = var_name.replace(
                "|", "_").replace("/", "_").replace(" ", "")
        out_png = f"{fname_prefix}_{region}.png"
        plt.savefig(os.path.join(output_dir, out_png),
                    dpi=180, bbox_inches="tight")
        plt.show()

# ----- Plot: Compare scenarios -----
# plot_by_scenario(df_long_water, "Price|Drinking Water",
#                  f"Price_DrinkingWater")
# plot_by_scenario(df_long_ene, "Price|Secondary Energy|Electricity",
#                  f"Price_SecondaryEnergy_Electricity")


# ----- Calculate Change -----
basins = list(basin_mapping.keys())
region = 'China'
var = 'Price|Drinking Water'

df_rc = df_long_water[(df_long_water['region'].isin(basins)) &
                      (df_long_water['variable'] == var) &
                      (df_long_water['year'] >= 2030) &
                      (df_long_water['year'] <= 2060)]
df_rc['scenario'] = df_rc['scenario'].map(scenario_dict)
df_rc['region'] = df_rc['region'].map(basin_mapping)

# Average value for the Nile River Basin
df_rc_pivot = df_rc.pivot_table(index=['region', 'year'], columns='scenario',
                                values='value', aggfunc="mean").reset_index()
# Relative Change
df_rc_pivot['rc'] = ((df_rc_pivot['IBWT'] -
                     df_rc_pivot['noIBWT'])/df_rc_pivot['noIBWT'])*100
# df_rc_pivot['rc'] = ((df_rc_pivot['GEI'] -
#                      df_rc_pivot['noIBWT'])/df_rc_pivot['noIBWT'])*100

# Absolute Change
df_rc_pivot['change'] = (df_rc_pivot['IBWT'] - df_rc_pivot['noIBWT'])

# Annual average
df_annual = df_rc_pivot.groupby("region", as_index=False).mean()


def plot_rc():
    # ----- Plot: Relative Change by region (x: year) -----
    # but the time trend cannot explain anything
    basin_names = df_rc_pivot['region'].unique().tolist()
    for basin_name in basin_names:
        # color setting
        if basin_name in supply_basins:
            color_b = "royalblue"
        else:
            color_b = "lightcoral"
        df_rc_pivot_region = df_rc_pivot[df_rc_pivot['region'] == basin_name]
        plt.figure(figsize=(6, 4))
        plt.plot(df_rc_pivot_region["year"], df_rc_pivot_region["rc"],
                 marker="o", linestyle="-", linewidth=2, color=color_b)
        plt.axhline(0, color="gray", linestyle="--",
                    linewidth=1)  # 0 base line
        plt.title(f"GEI vs. Baseline ({basin_name})")
        plt.xlabel("Year")
        plt.ylabel("Price Change for Drinking Water (%)")
        # plt.grid(True, linestyle="--", alpha=0.6)
        plt.tight_layout()

        out_png = f"Relative Change_drinking water_{basin_name}.png"
        plt.savefig(os.path.join(output_dir, out_png),
                    dpi=180, bbox_inches="tight")
        plt.show()


# ----- Plot: Absolute Change (x: basins) -----
plt.rcParams.update({
    'font.size': 20,
    'axes.titlesize': 16,
    'axes.labelsize': 14,
    'xtick.labelsize': 14,
    'ytick.labelsize': 14,
    'legend.fontsize': 16,
    'figure.titlesize': 18
})

# Re-order
df_annual["region"] = pd.Categorical(
    df_annual["region"], categories=basin_order, ordered=True)

df_annual = df_annual.sort_values(by="region").reset_index(drop=True)

# Filter
df_annual = df_annual[~df_annual['region'].isin(
    ['Mississippi', 'Colorado', 'Amazon', 'Sao Francisco'])]

# ----- Plot1: double y-axis -----
fig, ax1 = plt.subplots(figsize=(10, 7))

# y1
bars = ax1.bar(df_annual['region'], df_annual['change'],
               color='skyblue', label='Absolute Change', width=0.6)
ax1.set_ylabel('US$2010/m³')
ax1.set_xlabel('Basin', fontsize=12)
ax1.tick_params(axis='x', rotation=45)
for label in ax1.get_xticklabels():
    label.set_ha("right")
ax1.axhline(y=0, color='grey', linestyle='--', linewidth=1, alpha=0.8)

# y2
ax2 = ax1.twinx()
ax2.scatter(df_annual['region'], df_annual['rc'], color='orange',
            marker='o', label='Relative Change')
ax2.set_ylabel('%')
ax2.axhline(y=0, color='grey', linestyle='--', linewidth=1, alpha=0.8)

# Legend
lines, labels = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines + lines2, labels + labels2)

plt.title('IBWT – baseline')
plt.tight_layout()
plt.show()

# ----- Plot2: broken y-axis -----
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8), sharex='all',
                               gridspec_kw={'height_ratios': [1, 3]})
fig.subplots_adjust(hspace=0.05)

# left: Absolute Change
for ax in [ax1, ax2]:
    ax.bar(df_annual['region'], df_annual['change'],
           color='skyblue', edgecolor='black', width=0.6, label='Absolute Change')

ax2.set_ylim(-6.5, 0.5)
ax1.set_ylim(20, 25)

ax2.axhline(y=0, color='grey', linestyle='--', linewidth=1, alpha=0.8)
ax2.set_ylabel('US$2010/m³')
ax2.yaxis.set_label_coords(-0.07, 0.7)

# Remove axis
ax1.spines.bottom.set_visible(False)
ax2.spines.top.set_visible(False)

ax1.tick_params(axis='x', length=0)  # remove x ticks
ax1.yaxis.set_visible(False)  # remove x ticks and nums

# x label for ax2
ax2.set_xlabel('Basin')
ax2.tick_params(axis='x', rotation=45)
for label in ax2.get_xticklabels():
    label.set_ha("right")

# right: Relative Change
ax1r = ax1.twinx()
ax1r.scatter(df_annual['region'], df_annual['rc'], color='orange',
             s=80, label='Relative Change')
ax1r.set_ylim(200, max(df_annual['rc']) * 1.1)

ax2r = ax2.twinx()
ax2r.scatter(df_annual['region'], df_annual['rc'], color='orange',
             s=80, label='Relative Change (lower)')
ax2r.set_ylim(min(df_annual['rc']) * 1.1, 7.5)  # 为了对齐y=0，需要手动调整max范围
ax2r.set_ylabel('%')
ax2r.yaxis.set_label_coords(1.07, 0.7)

ax1r.spines.bottom.set_visible(False)
ax2r.spines.top.set_visible(False)

# broken labels
d = 0.5
kwargs = dict(marker=[(-1, -d), (1, d)], markersize=10,
              linestyle="none", color='k', mec='k', mew=1, clip_on=False)
ax1.plot([0, 1], [0, 0], transform=ax1.transAxes, **kwargs)
ax2.plot([0, 1], [1, 1], transform=ax2.transAxes, **kwargs)

# Legend
lines, labels = ax1.get_legend_handles_labels()
lines2, labels2 = ax1r.get_legend_handles_labels()
ax1.legend(lines + lines2, labels + labels2)

plt.suptitle(
    'Mean Annual Difference in Drinking Water Price\n(IBWT – baseline)')
out_png = f"Change_drinking water_basins.png"
plt.savefig(os.path.join(output_dir, out_png),
            dpi=180, bbox_inches="tight")
plt.tight_layout()
plt.show()
