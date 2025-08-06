import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from message_ix_models.util import package_data_path

# Increase default font size for all text elements
plt.rcParams.update({
    'font.size': 14,
    'axes.titlesize': 16,
    'axes.labelsize': 14,
    'xtick.labelsize': 12,
    'ytick.labelsize': 12,
    'legend.fontsize': 12,
    'figure.titlesize': 18
})

# Read data
model = "MESSAGE_GLOBIOM_SSP2_v6.1_ibwt_t4"
scen = "baseline_nexus_7_high_ibwt_t4"
data_file = (
    package_data_path().parents[0]
    / f"reporting_output/{model}_{scen}.csv"
)
df = pd.read_csv(data_file)
version = scen.split('_')[-1]

output_dir = package_data_path(
).parents[0] / f"reporting_output/plot_ibwt/{version}"
output_dir.mkdir(parents=True, exist_ok=True)

# Melt wide to long for year columns
df_cols = df.columns.tolist()
id_cols = ['Model', 'Scenario', 'Region', 'Variable', 'Unit']
year_cols = [c for c in df_cols if c.isdigit()]
df_long = df.melt(
    id_vars=id_cols,
    value_vars=year_cols,
    var_name='Year',
    value_name='Value'
)
df_long['Year'] = df_long['Year'].astype(int)

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
    # 'GLB region',
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

# Years to include (from 2020 onward)
years = sorted(df_long['Year'].unique())
years = [y for y in years if y >= 2020]

# Prepare colors and labels


def get_colors(cmap_name, n):
    cmap = plt.get_cmap(cmap_name)
    return [cmap(i / (n - 1)) for i in range(n)]


colors = get_colors('tab20', len(energy_vars))
# Swap colors for Biomass (index 0) and Hydro (index 4)
colors[0], colors[4] = colors[4], colors[0]
labels = [v.split('|')[-1] for v in energy_vars]

# Determine unit label
unit_label = 'EJ/yr'

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
                width=1.2,
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
filename = f"Energy_mix_regions.png"
save_path = os.path.join(output_dir, filename)
plt.savefig(save_path, dpi=300)
plt.show()
