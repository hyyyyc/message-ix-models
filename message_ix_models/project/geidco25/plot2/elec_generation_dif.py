import os
import glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

from message_ix_models.util import package_data_path
from message_ix_models.project.geidco25.data_processing.plot_dp import (
    stan_data_stru, drop_all_zero_rows,
    scen_name_dict)

# ===== Read data =====
file_list = glob.glob(
    r"D:\IIASA\Model\message-ix-models\message_ix_models\reporting_output\report_full_t4\*")

# Read all files
dfs = []
for f in file_list:
    df = pd.read_excel(f, sheet_name="data")
    dfs.append(df)

# Concat
if dfs:
    df_con = pd.concat(dfs, ignore_index=True)
else:
    df_con = pd.DataFrame()

# ===== Settings =====
# Font size
plt.rcParams.update({
    "font.size": 14,
    "axes.titlesize": 16,
    "axes.labelsize": 14,
    "xtick.labelsize": 12,
    "ytick.labelsize": 12,
    "legend.fontsize": 14
})

energy_vars = [
    "Secondary Energy|Electricity|Coal",
    "Secondary Energy|Electricity|Gas",
    "Secondary Energy|Electricity|Oil",
    "Secondary Energy|Electricity|Geothermal",
    "Secondary Energy|Electricity|Nuclear",
    "Secondary Energy|Electricity|Biomass",
    "Secondary Energy|Electricity|Hydro",
    "Secondary Energy|Electricity|Hydro|GEI",
    "Secondary Energy|Electricity|Solar",
    "Secondary Energy|Electricity|Solar|PV|GEI",
    "Secondary Energy|Electricity|Wind",
    "Secondary Energy|Electricity|Wind|GEI",
    "Secondary Energy|Electricity|Other"
]

energy_plot_vars = [
    "Secondary Energy|Electricity|Coal",
    "Secondary Energy|Electricity|Gas",
    "Secondary Energy|Electricity|Oil",
    "Secondary Energy|Electricity|Geothermal",
    "Secondary Energy|Electricity|Nuclear",
    "Secondary Energy|Electricity|Biomass",
    "Secondary Energy|Electricity|Hydro|noGEI",
    "Secondary Energy|Electricity|Hydro|GEI",
    "Secondary Energy|Electricity|Solar|noGEI",
    "Secondary Energy|Electricity|Solar|PV|GEI",
    "Secondary Energy|Electricity|Wind|noGEI",
    "Secondary Energy|Electricity|Wind|GEI",
    "Secondary Energy|Electricity|Other"
]

energy_label = ['Coal',
                'Gas',
                'Oil',
                'Geothermal',
                'Nuclear',
                'Biomass',
                'Hydro noGEI',
                'Hydro GEI',
                'Solar noGEI',
                'Solar GEI',
                'Wind noGEI',
                'Wind GEI',
                'Other']

energy_var_label_dict = dict(zip(energy_plot_vars, energy_label))

# colors
color_map = [
    "#d43e33",
    '#ff8b26',
    "#100303D7",
    '#c9a69e',
    '#e377c2',
    '#9e76c3',
    '#a7dde7',
    '#a7dde7',
    "#f3e48d",
    "#f3e48d",
    '#a2e295',
    '#a2e295',
    'grey'
]

energy_var_color_dict = dict(zip(energy_plot_vars, color_map))

# hatches
hatch = "\\\\"
hatch_map = [
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    hatch,
    "",
    hatch,
    "",
    hatch,
    ""
]

energy_var_hatch_dict = dict(zip(energy_plot_vars, hatch_map))

# ===== Data Process =====
df_long = stan_data_stru(df_con)

# filter
df_filter = df_long.pipe(
    lambda df: df[(df['year'] >= 2030) & (df['year'] <= 2060)]).pipe(
    lambda df: df[df['variable'].isin(energy_vars)]).pipe(
    lambda df: df[~(df['region'] == 'World')]).pipe(
    lambda df: df[df['scenario'].isin(list(scen_name_dict.keys()))]
)
df_filter['scenario'] = df_filter['scenario'].replace(scen_name_dict)

# Calculate world
df_world = df_filter.groupby(['model', 'scenario', 'unit', 'variable', 'year'])[
    'value'].sum().reset_index()
df_world['region'] = 'World'

df_filter_world = pd.concat([df_world, df_filter])

# Calculate noGEI
ID_vals = ['model', 'region', 'scenario', 'unit', 'year']
df_fw_wide = df_filter_world.pivot(index=ID_vals,
                                   columns='variable',
                                   values='value').reset_index()
df_fw_wide['Secondary Energy|Electricity|Solar|noGEI'] = df_fw_wide["Secondary Energy|Electricity|Solar"] - \
    df_fw_wide["Secondary Energy|Electricity|Solar|PV|GEI"]
df_fw_wide['Secondary Energy|Electricity|Hydro|noGEI'] = df_fw_wide["Secondary Energy|Electricity|Hydro"] - \
    df_fw_wide["Secondary Energy|Electricity|Hydro|GEI"]
df_fw_wide['Secondary Energy|Electricity|Wind|noGEI'] = df_fw_wide["Secondary Energy|Electricity|Wind"] - \
    df_fw_wide["Secondary Energy|Electricity|Wind|GEI"]

df = df_fw_wide.melt(
    id_vars=ID_vals,
    var_name='variable',
    value_name='value')

# Calculate scenario difference
ID_vals2 = ['model', 'region', 'variable', 'unit', 'year']

df_scen_wide = df.pivot(index=ID_vals2,
                        columns='scenario',
                        values='value').reset_index()
df_scen_wide['RCP7.0 GEI-BAU'] = df_scen_wide['RCP7.0 GEI'] - \
    df_scen_wide['RCP7.0 BAU']
df_scen_wide['Mitigation GEI-BAU'] = df_scen_wide['Mitigation GEI'] - \
    df_scen_wide['Mitigation BAU']

df_scen_wide['RCP7.0 IBWT-BAU'] = df_scen_wide['RCP7.0 IBWT'] - \
    df_scen_wide['RCP7.0 BAU']
df_scen_wide['Mitigation IBWT-BAU'] = df_scen_wide['Mitigation IBWT'] - \
    df_scen_wide['Mitigation BAU']

df_scen_wide['RCP7.0 GEI&IBWT-IBWT'] = df_scen_wide['RCP7.0 GEI&IBWT'] - \
    df_scen_wide['RCP7.0 IBWT']
df_scen_wide['Mitigation GEI&IBWT-IBWT'] = df_scen_wide['Mitigation GEI&IBWT'] - \
    df_scen_wide['Mitigation IBWT']

df2 = df_scen_wide.melt(
    id_vars=ID_vals2,
    # Here to change the scenario difference theme for plotting
    value_vars=['Mitigation GEI&IBWT-IBWT'],
    var_name='scenario',
    value_name='value')

# ====== Plot ======
regions = df2['region'].unique()
for region in regions:
    df_reg = df2[df2['region'] == region]
    df_reg = drop_all_zero_rows(df_reg)

    var_reg = df_reg['variable'].unique().tolist()
    unit = df_reg['unit'].unique()[0]

    # to wide
    df_reg_wide = df_reg.pivot(index=ID_vals,
                               columns='variable',
                               values='value').reset_index()

    years = sorted(df_reg_wide['year'].unique())
    scen_name = df_reg_wide['scenario'].unique()[0]

    fig, ax = plt.subplots(figsize=(8, 5.5))
    # bar bottom
    pos_cum = np.zeros(len(years))
    neg_cum = np.zeros(len(years))
    var_plot_list = [var for var in energy_plot_vars if var in var_reg]

    for var in var_plot_list:
        var_y = df_reg_wide[var].to_numpy()

        bottoms = np.where(var_y >= 0, pos_cum, neg_cum)
        ax.bar(years, var_y, bottom=bottoms, width=1.8,
               label=energy_var_label_dict[var],
               color=energy_var_color_dict[var],
               hatch=energy_var_hatch_dict[var])

        pos_cum = pos_cum + np.where(var_y > 0, var_y, 0)
        neg_cum = neg_cum + np.where(var_y < 0, var_y, 0)

    ax.set_title(f"{scen_name} ({region})")
    ax.set_xlabel("Year")
    ax.set_ylabel(f"Electricity Generation ({unit})")
    ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1),
              frameon=False)
    ax.axhline(y=0, color='black', linewidth=1, alpha=0.5)
    ax.grid(axis='y',
            color='lightgrey',
            linestyle='-',
            linewidth=0.5,
            alpha=0.5)

    plt.tight_layout()

    # ===== Output =====
    output_dir = package_data_path(
    ).parents[0] / f"reporting_output/plot_diff/{scen_name}/elec_generation_2060"
    output_dir.mkdir(parents=True, exist_ok=True)
    # save
    filename = f"elec_generation_{region}.png"
    save_path = os.path.join(output_dir, filename)
    plt.savefig(save_path, dpi=300, bbox_inches='tight')

    plt.show()
