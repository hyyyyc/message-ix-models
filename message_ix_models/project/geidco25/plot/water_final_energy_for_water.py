import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from message_ix_models.util import package_data_path
from message_ix_models.project.geidco25.plot.water_ibwt_reporting import stan_data_stru

# Read data
model = "MixG_GEIDCO5_SSP2_v6.1"
scen = "Base_RCP7_noint_IBWT_t1"
data_file = (
    package_data_path().parents[0]
    / f"reporting_output/{model}_{scen}_nexus.csv"
)
df = pd.read_csv(data_file)
scenario = scen

# Output path
output_dir = package_data_path(
).parents[0] / f"reporting_output/plot_ibwt/{scenario}/final_energy_water"
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

# Define variables for final energy for water
energy_vars = [
    "Final Energy|Commercial|Water|Desalination",
    "Final Energy|Commercial|Water|Groundwater Extraction",
    "Final Energy|Commercial|Water|Interbasin Water Transfer",
    "Final Energy|Commercial|Water|Reuse",
    "Final Energy|Commercial|Water|Surface Water Extraction",
    "Final Energy|Commercial|Water|Transfer",
    "Final Energy|Commercial|Water|Treatment"
]

# Regions to plot
regions = ['World',
           'R12_CHN',
           'R12_EEU',
           'R12_FSU',
           'R12_LAM',
           'R12_MEA',
           'R12_NAM',
           'R12_PAS',
           'R12_PAO',
           'R12_RCPA',
           'R12_SAS',
           'R12_AFR',
           'R12_WEU']

# label name
label_map = {
    "Groundwater Extraction": "GW Extraction",
    "Surface Water Extraction": "SW Extraction",
    "Interbasin Water Transfer": "IBWT"
}


def get_colors(cmap_name, n):
    # Prepare colors and labels
    cmap = plt.get_cmap(cmap_name)
    return [cmap(i / (n - 1)) for i in range(n)]


def pretty_label(var: str) -> str:
    base = var.split('|')[-1]
    return label_map.get(base, base)


def filter_series_labels_colors(energy_df, energy_vars, labels, colors):
    """
    return non-zero records
    series_list、labels_list、colors_list
    """
    valid_idx = []
    for i, var in enumerate(energy_vars):
        vals = energy_df[var].to_numpy(
        ) if var in energy_df.columns else np.array([])
        # abs>0 -> non-zero
        if np.nansum(np.abs(vals)) > 0:
            valid_idx.append(i)

    series_list = [energy_df[energy_vars[i]].to_numpy() for i in valid_idx]
    labels_list = [labels[i] for i in valid_idx]
    colors_list = [colors[i] for i in valid_idx]
    return series_list, labels_list, colors_list


df_long = stan_data_stru(df)

# filter by year
years = sorted(df_long['year'].unique())
# years = [y for y in years if (y >= 2030) & (y <= 2055)]
years = [y for y in years if y >= 2030]

# colors
colors = get_colors('tab20', len(energy_vars))
# Swap colors for Biomass (index 0) and Hydro (index 4)
colors[0], colors[4] = colors[4], colors[0]

labels = [pretty_label(v) for v in energy_vars]

# unit label
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
            df_reg = df_long[df_long['region'].str.contains(region, na=False)]
            # Pivot to wide format
            energy_df = (
                df_reg[df_reg['variable'].isin(energy_vars)]
                .pivot_table(index='year', columns='variable', values='value', aggfunc='sum')
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
    filename = "Final_energy_regions_stackplot.png"
    save_path = os.path.join(output_dir, filename)
    plt.savefig(save_path, dpi=300)
    plt.show()


for region in regions:
    # filter by region
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

    # figure size
    fig, ax = plt.subplots(figsize=(12, 7))
    if len(series_plot) > 0:
        ax.stackplot(
            years,
            *series_plot,
            colors=colors_plot,
            labels=labels_plot,
            alpha=0.9
        )
        ax.legend(
            loc='upper left', bbox_to_anchor=(1.02, 1),
            frameon=False, ncol=1, fontsize='small'
        )
    else:
        ax.text(0.5, 0.5, 'No data', ha='center',
                va='center', transform=ax.transAxes)

    ax.set_title(region)
    ax.set_xlabel('Year')
    ax.set_ylabel('Final Energy for Water (EJ/yr)')
    ax.grid(True, color='lightgray', linestyle='--', linewidth=0.5, alpha=0.7)

    plt.tight_layout()

    # save
    filename = f"Final_Energy_{region}_stackplot.png"
    save_path = os.path.join(output_dir, filename)
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.show()
