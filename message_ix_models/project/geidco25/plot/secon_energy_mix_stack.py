import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from message_ix_models.util import package_data_path
from message_ix_models.project.geidco25.plot.water_final_energy_for_water import filter_series_labels_colors, get_colors
from message_ix_models.project.geidco25.plot.water_ibwt_reporting import stan_data_stru

# Read data
model = "MixG_GEIDCO5_SSP2_v6.1"
scen = "Base_RCP7_noint_noIBWT_t1"
data_file = (
    package_data_path().parents[0]
    / f"reporting_output/report_full/{model}_{scen}.csv"
)
df = pd.read_csv(data_file)
scenario = scen

# Output path
output_dir = package_data_path(
).parents[0] / f"reporting_output/plot_ibwt/{scenario}/energy_mix"
output_dir.mkdir(parents=True, exist_ok=True)

# Increase default font size for all text elements
plt.rcParams.update({
    'font.size': 20,
    'axes.titlesize': 16,
    'axes.labelsize': 14,
    'xtick.labelsize': 12,
    'ytick.labelsize': 12,
    'legend.fontsize': 20,
    'figure.titlesize': 18
})

# Define energy variables for secondary electricity
energy_vars = [
    "Secondary Energy|Electricity|Biomass",
    "Secondary Energy|Electricity|Coal",
    "Secondary Energy|Electricity|Gas",
    "Secondary Energy|Electricity|Geothermal",
    "Secondary Energy|Electricity|Hydro",
    "Secondary Energy|Electricity|Nuclear",
    "Secondary Energy|Electricity|Oil",
    "Secondary Energy|Electricity|Solar",
    "Secondary Energy|Electricity|Wind"
]

# Regions to plot in 4x4 grid
regions = [
    'GLB region',
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


df_long = stan_data_stru(df)

# filter by year
years = sorted(df_long['year'].unique())
# years = [y for y in years if (y >= 2030) & (y <= 2055)]
years = [y for y in years if y >= 2030]

# colors
colors = get_colors('tab20', len(energy_vars))
# Swap colors for Biomass (index 0) and Hydro (index 4)
colors[0], colors[4] = colors[4], colors[0]
labels = [v.split('|')[-1] for v in energy_vars]

# Determine unit label
unit_label = 'EJ/yr'


def panels():
    # Create 4x4 subplot grid
    n_rows, n_cols = 3, 4
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(28, 16), sharex=True)
    axes = axes.flatten()

    for idx, ax in enumerate(axes):
        if idx < len(regions):
            region = regions[idx]
            # Filter for region
            df_reg = df_long[df_long['Region'].str.contains(region, na=False)]
            # Pivot to wide format
            energy_df = (
                df_reg[df_reg['Variable'].isin(energy_vars)]
                .pivot_table(index='Year', columns='Variable', values='Value', aggfunc='sum')
                .reindex(years, fill_value=0)
            )

            # Ensure all variables exist as columns
            for var in energy_vars:
                if var not in energy_df.columns:
                    energy_df[var] = 0

            # 准备序列，并确保顺序与 energy_vars 一致
            series = [energy_df[var].to_numpy() for var in energy_vars]

            # stackplot：注意用 *series 展开；且仅在一个子图上传 labels
            if idx == 3:
                ax.stackplot(
                    years,
                    *series,
                    colors=colors,
                    labels=labels,
                    alpha=0.9
                )
            else:
                ax.stackplot(
                    years,
                    *series,
                    colors=colors,
                    alpha=0.9
                )

            ax.set_title(region)
            ax.set_ylabel(unit_label)
            ax.grid(True, color='lightgray', linestyle='--',
                    linewidth=0.5, alpha=0.7)
            if idx >= (n_rows - 1) * n_cols:
                ax.set_xlabel('Year')
        else:
            ax.axis('off')

    # 仅在第 4 个子图放图例（如果你想和面积层叠顺序一致，可以不反转）
    handles, labs = axes[3].get_legend_handles_labels()
    axes[3].legend(
        [handles[i] for i in reversed(range(len(handles)))],
        [labs[i] for i in reversed(range(len(labs)))],
        loc='upper left', bbox_to_anchor=(1.02, 1), fontsize='small'
    )

    plt.tight_layout()
    filename = "Energy_mix_regions_stackplot.png"
    save_path = os.path.join(output_dir, filename)
    plt.savefig(save_path, dpi=300)
    plt.show()


# plot by regions
for region in regions:
    # filter by regions
    df_reg = df_long[df_long['region'].str.contains(region, na=False)]

    # to wide
    energy_df = (
        df_reg[df_reg['variable'].isin(energy_vars)]
        .pivot_table(index='year', columns='variable', values='value', aggfunc='sum')
        .reindex(years, fill_value=0)
    )

    # for var in energy_vars:
    #     if var not in energy_df.columns:
    #         energy_df[var] = 0

    # filter non-zero records
    series_plot, labels_plot, colors_plot = filter_series_labels_colors(
        energy_df, energy_vars, labels, colors
    )

    # plot
    fig, ax = plt.subplots(figsize=(12, 7))
    ax.stackplot(
        years,
        *series_plot,
        colors=colors_plot,
        labels=labels_plot,
        alpha=0.9
    )

    ax.set_title(region)
    ax.set_xlabel('Year')
    ax.set_ylabel('Electricity (EJ/yr)')
    ax.grid(True, color='lightgray', linestyle='--', linewidth=0.5, alpha=0.7)

    leg = ax.legend(
        loc='upper left', bbox_to_anchor=(1.02, 1),
        frameon=False, ncol=1, fontsize='small'
    )

    plt.tight_layout()

    # save
    filename = f"Second_energy_mix_{region}_stackplot.png"
    save_path = os.path.join(output_dir, filename)
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.show()
