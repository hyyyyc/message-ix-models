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
    'font.size': 20,
    'axes.titlesize': 16,
    'axes.labelsize': 14,
    'xtick.labelsize': 14,
    'ytick.labelsize': 14,
    'legend.fontsize': 16,
    'figure.titlesize': 18
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
    'Base_RCP7_noint_noIBWT_t2': 'RCP7.0 baseline',
    'Base_RCP7_noint_IBWT_t2': 'RCP7.0 IBWT',
    'Base_RCP7_int_noIBWT_t2': 'RCP7.0 GEI',
    'Base_RCP7_int_IBWT_t2': 'RCP7.0 GEI&IBWT',
    'EN1000f_RCP26_noint_noIBWT_t2': 'Mitigation baseline',
    'EN1000f_RCP26_noint_IBWT_t2': 'Mitigation IBWT',
    'EN1000f_RCP26_int_noIBWT_t2': 'Mitigation GEI',
    'EN1000f_RCP26_int_IBWT_t2': 'Mitigation GEI&IBWT'
}

# ----- Data for water price -----
file_list_water = glob.glob(
    r"D:\IIASA\Model\message-ix-models\message_ix_models\reporting_output\*t2_nexus.csv")

# file_water_path = r"D:\IIASA\Model\message-ix-models\message_ix_models\reporting_output"
# file_list_water = ["MixG_GEIDCO5_SSP2_v6.1_Base_RCP7_noint_noIBWT_t2_nexus.csv",
#                    "MixG_GEIDCO5_SSP2_v6.1_Base_RCP7_noint_IBWT_t2_nexus.csv"]

# Read all files
dfs_water = []
for f in file_list_water:
    # df = pd.read_csv(os.path.join(file_water_path, f))
    df = pd.read_csv(f)
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


# ----- Calculate Change -----
basins = list(basin_mapping.keys())
region = 'China'
var = 'Price|Drinking Water'

df_water_filter = df_long_water[(df_long_water['region'].isin(basins)) &
                                (df_long_water['variable'] == var) &
                                (df_long_water['year'] >= 2030) &
                                (df_long_water['year'] <= 2060)]
df_water_filter['scenario'] = df_water_filter['scenario'].map(scenario_dict)
df_water_filter['region'] = df_water_filter['region'].map(basin_mapping)

# Average value for the Nile River Basin
df_water_scen = df_water_filter.pivot_table(index=['region', 'year'], columns='scenario',
                                            values='value', aggfunc="mean").reset_index()

# Annual average
df_water_annual = df_water_scen.groupby("region", as_index=False).mean()

# Re-order
df_water_annual["region"] = pd.Categorical(
    df_water_annual["region"], categories=basin_order, ordered=True)

df_water_annual = df_water_annual.sort_values(
    by="region").reset_index(drop=True)

# Filter
df_water_annual = df_water_annual[~df_water_annual['region'].isin(
    ['Mississippi', 'Colorado', 'Amazon', 'Sao Francisco'])]

# ----- Plot: Absolute Change (x: basins) -----
# ====== 2. 新的情景顺序 ======
scenario_order = [
    'RCP7.0 baseline', 'RCP7.0 IBWT', 'RCP7.0 GEI&IBWT',
    'Mitigation baseline', 'Mitigation IBWT', 'Mitigation GEI&IBWT'
]

# ====== 3. 色系与透明度设置 ======
base_colors = {
    'normal': (0.9, 0.5, 0.1),     # orange
    'mitigation': (0.2, 0.4, 0.8)  # blue
}
alphas = [1.0, 0.75, 0.5]  # 同组内透明度区分

# ====== 4. 数据准备 ======
regions = df_water_annual['region']
n_region = len(regions)
n_scenario = len(scenario_order)
x = np.arange(n_region)
bar_width = 0.12

# ====== 5. 绘图 ======
plt.figure(figsize=(13, 7))

for i, scenario in enumerate(scenario_order):
    if "Mitigation" in scenario:
        color = base_colors['mitigation']
        alpha = alphas[i - 3]
    else:
        color = base_colors['normal']
        alpha = alphas[i]

    plt.bar(x + i * bar_width,
            df_water_annual[scenario],
            width=bar_width,
            color=color,
            alpha=alpha,
            label=scenario)

# ====== 6. 美化图表 ======
plt.xticks(x + (n_scenario - 1) / 2 * bar_width, regions, rotation=20)
plt.ylabel('US$2010/m³')
plt.xlabel('Basin')
plt.title('Mean Annual Drinking Water Price\n(RCP7.0 vs. Mitigation)')

plt.legend()
plt.grid(axis='y', linestyle='--', alpha=0.5)

out_png = f"RCP_Miti_Drinking water_basins2.png"
plt.savefig(os.path.join(output_dir, out_png),
            dpi=180, bbox_inches="tight")

plt.tight_layout()
plt.show()


# fig, ax1 = plt.subplots(figsize=(10, 7))

# # y1
# bars = ax1.bar(df_annual['region'], df_annual['change'],
#                color='skyblue', label='Absolute Change', width=0.6)
# ax1.set_ylabel('US$2010/m³')
# ax1.set_xlabel('Basin', fontsize=12)
# ax1.tick_params(axis='x', rotation=45)
# for label in ax1.get_xticklabels():
#     label.set_ha("right")
# ax1.axhline(y=0, color='grey', linestyle='--', linewidth=1, alpha=0.8)

# # y2
# ax2 = ax1.twinx()
# ax2.scatter(df_annual['region'], df_annual['rc'], color='orange',
#             marker='o', label='Relative Change')
# ax2.set_ylabel('%')
# ax2.axhline(y=0, color='grey', linestyle='--', linewidth=1, alpha=0.8)

# # Legend
# lines, labels = ax1.get_legend_handles_labels()
# lines2, labels2 = ax2.get_legend_handles_labels()
# ax1.legend(lines + lines2, labels + labels2)

# plt.title('')
# plt.tight_layout()
# plt.show()

# # ----- Plot2: broken y-axis -----
# fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8), sharex='all',
#                                gridspec_kw={'height_ratios': [1, 3]})
# fig.subplots_adjust(hspace=0.05)

# # left: Absolute Change
# for ax in [ax1, ax2]:
#     ax.bar(df_annual['region'], df_annual['change'],
#            color='skyblue', edgecolor='black', width=0.6, label='Absolute Change')

# ax2.set_ylim(-6.5, 0.5)
# ax1.set_ylim(20, 25)

# ax2.axhline(y=0, color='grey', linestyle='--', linewidth=1, alpha=0.8)
# ax2.set_ylabel('US$2010/m³')
# ax2.yaxis.set_label_coords(-0.07, 0.7)

# # Remove axis
# ax1.spines.bottom.set_visible(False)
# ax2.spines.top.set_visible(False)

# ax1.tick_params(axis='x', length=0)  # remove x ticks
# ax1.yaxis.set_visible(False)  # remove x ticks and nums

# # x label for ax2
# ax2.set_xlabel('Basin')
# ax2.tick_params(axis='x', rotation=45)
# for label in ax2.get_xticklabels():
#     label.set_ha("right")

# # right: Relative Change
# ax1r = ax1.twinx()
# ax1r.scatter(df_annual['region'], df_annual['rc'], color='orange',
#              s=80, label='Relative Change')
# ax1r.set_ylim(200, max(df_annual['rc']) * 1.1)

# ax2r = ax2.twinx()
# ax2r.scatter(df_annual['region'], df_annual['rc'], color='orange',
#              s=80, label='Relative Change (lower)')
# ax2r.set_ylim(min(df_annual['rc']) * 1.1, 7.5)  # 为了对齐y=0，需要手动调整max范围
# ax2r.set_ylabel('%')
# ax2r.yaxis.set_label_coords(1.07, 0.7)

# ax1r.spines.bottom.set_visible(False)
# ax2r.spines.top.set_visible(False)

# # broken labels
# d = 0.5
# kwargs = dict(marker=[(-1, -d), (1, d)], markersize=10,
#               linestyle="none", color='k', mec='k', mew=1, clip_on=False)
# ax1.plot([0, 1], [0, 0], transform=ax1.transAxes, **kwargs)
# ax2.plot([0, 1], [1, 1], transform=ax2.transAxes, **kwargs)

# # Legend
# lines, labels = ax1.get_legend_handles_labels()
# lines2, labels2 = ax1r.get_legend_handles_labels()
# ax1.legend(lines + lines2, labels + labels2)

# plt.suptitle(
#     'Mean Annual Difference in Drinking Water Price\n(IBWT – baseline)')
# out_png = f"Change_drinking water_basins.png"
# plt.savefig(os.path.join(output_dir, out_png),
#             dpi=180, bbox_inches="tight")
# plt.tight_layout()
# plt.show()
