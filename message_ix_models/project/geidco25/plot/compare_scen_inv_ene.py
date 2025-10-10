import glob
import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

from message_ix_models.util import package_data_path
from message_ix_models.project.geidco25.data_processing.plot_dp import (
    stan_data_stru, fill_missing_region)

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

# ----- Read data -----
file_list = glob.glob(
    r"D:\IIASA\Model\message-ix-models\message_ix_models\reporting_output\report_full_t2\MixG*.xlsx")

print(file_list)

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

# ----- Output -----
output_dir = package_data_path(
).parents[0] / f"reporting_output/plot_diff/compare_scen_t2/inv_ene"
output_dir.mkdir(parents=True, exist_ok=True)

# ----- Configuration ------
vars_inv_ene = [
    "Investment|Energy Supply|Electricity|UHV Transmission and Distribution",
    "Investment|Energy Supply|Electricity|Transmission and Distribution",
    "Investment|Energy Supply|Electricity|Coal",
    "Investment|Energy Supply|Electricity|Gas",
    "Investment|Energy Supply|Electricity|Oil",
    "Investment|Energy Supply|Electricity|Geothermal",
    "Investment|Energy Supply|Electricity|Nuclear",
    "Investment|Energy Supply|Electricity|Biomass",
    "Investment|Energy Supply|Electricity|Hydro",
    "Investment|Energy Supply|Electricity|Solar",
    "Investment|Energy Supply|Electricity|Wind",
    "Investment|Energy Supply|Electricity|Other"
]

labels = [
    "UHV Trans & Distr",
    "Trans & Distr",
    "Coal",
    "Gas",
    "Oil",
    "Geothermal",
    "Nuclear",
    "Biomass",
    "Hydro",
    "Solar",
    "Wind",
    "Other"
]

var_label_map = dict(zip(vars_inv_ene, labels))

# colors
color_map = {
    'Biomass': "#75c45f",
    'Coal': "#d43e33",
    'Gas': '#ff8b26',
    'Geothermal': '#c9a69e',
    'Hydro': '#a7dde7',
    'Nuclear': '#e377c2',
    'Oil': "#100303D7",
    'Solar': "#f3e48d",
    'Wind': '#a2e295',
    'Other': 'grey',
    'UHV Trans & Distr': "#8b4fc4",
    'Trans & Distr': "#a27ec3"
}

scenario_order = [
    "Base_RCP7_noint_noIBWT",
    "Base_RCP7_noint_IBWT",
    "Base_RCP7_int_noIBWT",
    "Base_RCP7_int_IBWT",
    "EN1000f_RCP26_noint_noIBWT",
    "EN1000f_RCP26_noint_IBWT",
    "EN1000f_RCP26_int_noIBWT",
    "EN1000f_RCP26_int_IBWT"
]

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

# ----- Data Preprocessing -----
# becasue some bugs when running gei reporting with nexus scenario
df = fill_missing_region(df_con)
df_long = stan_data_stru(df)

# Filter year
# long_df = long_df[(long_df["Year"] >= 2030) & (long_df["Year"] <= 2055)]
df_long = df_long[df_long["year"] >= 2030]

# Rename scenario
df_long["scenario"] = df_long["scenario"].str.replace(
    r"_t2$", "", regex=True)

# Filter variables
df_long = df_long[df_long["variable"].isin(vars_inv_ene)]

# ----- Plotting -----
for region in regions:
    if region != 'World':
        long_df = df_long[df_long["region"] == region]

        # Calculate annual average
        df_mean = long_df.groupby(["scenario", "variable"],
                                  as_index=False)["value"].mean()

        # Re-order scenarios
        df_mean["scenario"] = pd.Categorical(
            df_mean["scenario"], categories=scenario_order, ordered=True)

        # Rename variables
        df_mean["variable"] = df_mean["variable"].replace(var_label_map)

        df_pivot = df_mean.pivot(
            index="scenario", columns="variable", values="value").fillna(0)

    else:
        # due to incompatibility between gei reporting and nexus
        # need to aggregate "World" manually
        long_df = df_long[~df_long["region"].isin(["World", "Missing region"])]

        # sum up
        df_world = long_df.groupby(["scenario", "variable", "year"],
                                   as_index=False)["value"].sum()

        # Calculate annual average
        df_mean = df_world.groupby(["scenario", "variable"],
                                   as_index=False)["value"].mean()

        # Re-order scenarios
        df_mean["scenario"] = pd.Categorical(
            df_mean["scenario"], categories=scenario_order, ordered=True)

        # Rename variables
        df_mean["variable"] = df_mean["variable"].replace(var_label_map)

        df_pivot = df_mean.pivot(
            index="scenario", columns="variable", values="value").fillna(0)

    # Re-order variables
    df_pivot = df_pivot[[col for col in labels if col in df_pivot.columns]]

    ax = df_pivot.plot(kind="bar",
                       stacked=True,
                       color=[color_map[c] for c in df_pivot.columns],
                       figsize=(10, 6))

    plt.ylabel(f"Investment (billion US$2010/yr)")
    plt.title(f"Annual Investment for Electricity ({region})")
    plt.xticks(rotation=45, ha="right")
    # Legend
    plt.legend(
        title=None,
        bbox_to_anchor=(0.5, -0.5),
        loc="upper center",
        ncol=4
    )
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.25)
    out_png = f"Investment_ener_{region}.png"
    plt.savefig(os.path.join(output_dir, out_png),
                dpi=180, bbox_inches="tight")
    plt.show()
