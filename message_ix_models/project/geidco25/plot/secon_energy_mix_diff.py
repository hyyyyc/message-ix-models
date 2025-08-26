import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from message_ix_models.util import package_data_path
from message_ix_models.project.geidco25.plot.water_final_energy_for_water import get_colors
from message_ix_models.project.geidco25.plot.water_ibwt_reporting import stan_data_stru

'''
Run compare_report.py first
Using gei reporting if GEI scenario
'''

# Read data
model = "MixG_GEIDCO5_SSP2_v6.1"
scen_a = "Base_RCP7_noint_noIBWT_t1"
scen_b = "Base_RCP7_int_noIBWT_t1_gei"
data_file = (
    package_data_path().parents[0]
    / f"reporting_output/report_diff/diff_{scen_a}_{scen_b}.xlsx"
)
df = pd.read_excel(data_file, sheet_name="differences")

# output path
output_dir = package_data_path(
).parents[0] / f"reporting_output/plot_diff/{scen_a}_{scen_b}/secon_energy_mix"
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


def clean_diff_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # remove A and B column
    cols_to_drop = [col for col in df.columns if col.endswith(("A", "B"))]
    df.drop(columns=cols_to_drop, inplace=True)

    # rename diff column
    new_columns = {col: col.replace("_diff", "")
                   for col in df.columns if col.endswith("diff")}
    df.rename(columns=new_columns, inplace=True)

    return df


def active_var_indices(energy_df, energy_vars):
    """
    abs>0 -> non-zero records
    """
    tmp = energy_df.reindex(columns=energy_vars,
                            fill_value=0).abs().sum(axis=0)
    return [i for i, var in enumerate(energy_vars) if tmp.get(var, 0) > 0]


df = clean_diff_df(df)
df_long = stan_data_stru(df)

# filter by year
years = sorted(df_long['year'].unique())
# years = [y for y in years if (y >= 2030) & (y <= 2055)]
years = [y for y in years if (y >= 2030)]

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
            # Plot stacked bars with increased width
            bottom = np.zeros(len(years))
            for j, var in enumerate(energy_vars):
                vals = energy_df.get(var, pd.Series(0, index=years)).values
                ax.bar(
                    years,
                    vals,
                    bottom=bottom,
                    width=2,
                    label=labels[j] if idx == 3 else None,
                    color=colors[j]
                )
                bottom += vals

            ax.set_title(region)
            ax.set_ylabel(unit_label)
            ax.grid(True, color='lightgray', linestyle='--',
                    linewidth=0.5, alpha=0.7)
            if idx >= (n_rows - 1) * n_cols:
                ax.set_xlabel('Year')
        else:
            # Hide unused subplot
            ax.axis('off')

    # Legend on first subplot only
    # Reverse legend order on first subplot
    handles, labs = axes[3].get_legend_handles_labels()
    axes[3].legend(
        [handles[i] for i in reversed(range(len(handles)))],
        [labs[i] for i in reversed(range(len(labs)))],
        loc='upper left', bbox_to_anchor=(1.02, 1), fontsize='small'
    )

    plt.tight_layout()
    filename = f"Energy_mix_regions_{input_file}.png"
    save_path = os.path.join('diff', filename)
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

    # # make sure all the vars have values
    # for var in energy_vars:
    #     if var not in energy_df.columns:
    #         energy_df[var] = 0

    # keep non-zero records
    idx_active = active_var_indices(energy_df, energy_vars)

    # figure size
    fig, ax = plt.subplots(figsize=(12, 7))
    if len(idx_active) == 0:
        ax.text(0.5, 0.5, 'No data', ha='center',
                va='center', transform=ax.transAxes)
    else:
        bottom = np.zeros(len(years))
        for j in idx_active:
            var = energy_vars[j]
            vals = energy_df[var].to_numpy()
            ax.bar(
                years,
                vals,
                bottom=bottom,
                width=2,
                color=colors[j],
                label=labels[j]
            )
            bottom += vals

        handles, labs = ax.get_legend_handles_labels()
        if handles:
            ax.legend(
                handles, labs,
                loc='upper left', bbox_to_anchor=(1.02, 1),
                frameon=False, ncol=1, fontsize='small'
            )

    ax.set_title(region)
    ax.set_xlabel('Year')
    ax.set_ylabel(unit_label)
    ax.grid(True, color='lightgray', linestyle='--', linewidth=0.5, alpha=0.7)

    plt.tight_layout()

    # save
    filename = f"Secon_energy_mix_{region}_diff.png"
    save_path = os.path.join(output_dir, filename)
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.show()
