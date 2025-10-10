import glob
import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import gridspec
import numpy as np

from message_ix_models.util import package_data_path
from message_ix_models.project.geidco25.data_processing.plot_dp import (
    stan_data_stru, fill_missing_region, basin_mapping, supply_basins)

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
# regions = [
#     'World',
#     'China',
#     'Eastern Europe',
#     'Former Soviet Union',
#     'Latin America',
#     'Middle East and Africa',
#     'North America',
#     'Pacific Asia',
#     'Pacific OECD',
#     'Rest of Centrally planned Asia',
#     'South Asia',
#     'Subsaharan Africa',
#     'Western Europe'
# ]

region_dict = {
    'China': 'China',
    'Middle East and Africa': 'Africa',
    'Subsaharan Africa': 'Africa'
}

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

# ----- Output -----
output_dir = package_data_path(
).parents[0] / f"reporting_output/plot_diff/compare_scen_t2/price_annual_2060"
output_dir.mkdir(parents=True, exist_ok=True)


# ----- Calculate Change -----
countries = ['China', 'Africa']
var = 'Price|Secondary Energy|Electricity'

df_long_ene['region'] = df_long_ene['region'].map(region_dict)

df_ene_filter = df_long_ene[(df_long_ene['region'].isin(countries)) &
                            (df_long_ene['variable'] == var) &
                            (df_long_ene['year'] >= 2030) &
                            (df_long_ene['year'] <= 2060)]
df_ene_filter['scenario'] = df_ene_filter['scenario'].map(scenario_dict)

# Average value for Africa
df_ene_scen = df_ene_filter.pivot_table(index=['region', 'year'], columns='scenario',
                                        values='value', aggfunc="mean").reset_index()

# Annual average
df_ene_annual = df_ene_scen.groupby("region", as_index=False).mean()

# Re-order
df_ene_annual["region"] = pd.Categorical(
    df_ene_annual["region"], categories=countries, ordered=True)

df_ene_annual = df_ene_annual.sort_values(
    by="region").reset_index(drop=True)

# ----- Plot: Absolute Change (x: basins) -----
# ====== 2. 新的情景顺序 ======
scenario_order = [
    'RCP7.0 baseline', 'Mitigation baseline', 'Mitigation GEI'
]

# ====== 3. 色系与透明度设置 ======
base_colors = {
    'normal': (0.9, 0.5, 0.1),     # orange
    'mitigation': (0.2, 0.4, 0.8)  # blue
}
alphas = [1.0, 0.75, 0.5]  # 同组内透明度区分

# ====== 4. 数据准备 ======
regions = df_ene_annual['region']
n_region = len(regions)
n_scenario = len(scenario_order)
x = np.arange(n_region)
bar_width = 0.2

# ====== 5. 绘图 ======
plt.figure(figsize=(11, 7))

for i, scenario in enumerate(scenario_order):
    if "Mitigation" in scenario:
        color = base_colors['mitigation']
        alpha = alphas[i - 1]
    else:
        color = base_colors['normal']
        alpha = alphas[i]

    plt.bar(x + i * (bar_width+0.05),
            df_ene_annual[scenario],
            width=bar_width,
            color=color,
            alpha=alpha,
            label=scenario)

# ====== 6. 美化图表 ======
plt.xticks(x + (n_scenario - 1) / 2 * (bar_width + 0.05), regions, rotation=20)
plt.ylabel('US$2010/GJ')
plt.xlabel('Region')
plt.title('Mean Annual Electricity Price\n(RCP7.0 vs. Mitigation)')

plt.legend(loc="upper left",
           bbox_to_anchor=(1.02, 1),)
plt.grid(axis='y', linestyle='--', alpha=0.5)

out_png = f"RCP_Miti_electricity_basins2.png"
plt.savefig(os.path.join(output_dir, out_png),
            dpi=180, bbox_inches="tight")

plt.tight_layout()
plt.show()
