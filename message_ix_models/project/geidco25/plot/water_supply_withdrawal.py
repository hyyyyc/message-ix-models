import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from message_ix_models.util import package_data_path


def get_gradient_colors(cmap_name, n, highlight_index=None, highlight_color=None):
    '''
    Prepare color gradients with special color for Interbasin Water Transfer
    '''
    cmap = plt.get_cmap(cmap_name)
    colors = [cmap(0.3 + 0.7 * i / (n - 1)) for i in range(n)]
    if highlight_index is not None and highlight_color is not None:
        colors[highlight_index] = highlight_color
    return colors


# Read data
model = "MESSAGE_GLOBIOM_SSP2_v6.1_ibwt_t4"
scen = "baseline_nexus_7_high_ibwt_t4"
data = (
    package_data_path().parents[0]
    / f"reporting_output/{model}_{scen}.csv"
)
df = pd.read_csv(data)
version = scen.split('_')[-1]

output_dir = package_data_path(
).parents[0] / f"reporting_output/plot_ibwt/{version}"
output_dir.mkdir(parents=True, exist_ok=True)

# Define variables
supply = [
    "Water Extraction|Surface Water",
    "Water Extraction|Groundwater",
    "Water Waste|Reuse",
    "Water Extraction|Brackish Water",
    "Water Extraction|Seawater|Desalination"
]

supply_ibwt = [
    "Water Extraction|Surface Water",
    "Water Transfer|Interbasin Water Transfer",
    "Water Extraction|Groundwater",
    "Water Waste|Reuse",
    "Water Extraction|Brackish Water",
    "Water Extraction|Seawater|Desalination"
]

withdrawal = [
    "Water Withdrawal|Electricity",
    "Water Withdrawal|Extraction",
    "Water Withdrawal|Irrigation",
    "Water Withdrawal|Industrial Water|Unconnected",
    "Water Withdrawal|Municipal Water"
]

supply_label = [
    'Surface Water',
    'IBWT',
    'Groundwater',
    'Reuse',
    'Brackish Water',
    'Desalination'
]

withdrawal_label = [
    'Electricity',
    'Extraction',
    'Irrigation',
    'Industrial Water',
    'Municipal Water'
]

supply_dict = dict(zip(supply_ibwt, supply_label))
withdrawal_dict = dict(zip(withdrawal, withdrawal_label))

# Filter data for region
regions = ['World|GLB region',
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

for i, region in enumerate(regions):
    df_filter = df[df['Region'].str.contains(region, na=False)]

    # Melt wide to long
    df_cols = df_filter.columns.to_list()
    df_cols_id = ['Model', 'Scenario', 'Region', 'Variable', 'Unit']
    df_cols_yr = [c for c in df_cols if c not in df_cols_id]
    df_long = df_filter.melt(
        id_vars=df_cols_id,
        value_vars=df_cols_yr,
        var_name='Year',
        value_name='Value'
    )
    df_long['Year'] = df_long['Year'].astype(int)

    # Years from 2020
    years = sorted(df_long['Year'].unique())
    years = [y for y in years if y >= 2020]

    if region in ['World|GLB region', 'China', 'Subsaharan Africa']:
        # Pivot supply and withdrawal
        sup_df = df_long[df_long['Variable'].isin(
            supply_ibwt) & df_long['Year'].isin(years)]
        sup_table = (
            sup_df.pivot_table(index='Year', columns='Variable',
                               values='Value', aggfunc='sum')
            .reindex(years, fill_value=0)
        )
        # Reorder columns to desired sequence
        sup_table = sup_table[supply_ibwt]

        # Identify index of Interbasin in supply_table columns
        supply_cols = list(sup_table.columns)
        highlight_idx = supply_cols.index(
            "Water Transfer|Interbasin Water Transfer")
        # Use bright crimson for highlight
        highlight_color = (0.988, 0.608, 0.059, 1.0)  # RGBA bright color
        supply_cmap = get_gradient_colors('Reds', len(
            supply_cols), highlight_idx, highlight_color)
    else:
        # Pivot supply and withdrawal
        sup_df = df_long[df_long['Variable'].isin(
            supply) & df_long['Year'].isin(years)]
        sup_table = (
            sup_df.pivot_table(index='Year', columns='Variable',
                               values='Value', aggfunc='sum')
            .reindex(years, fill_value=0)
        )
        # Reorder columns to desired sequence
        sup_table = sup_table[supply]

        # Identify index of Interbasin in supply_table columns
        supply_cols = list(sup_table.columns)
        # Use bright crimson for highlight
        supply_cmap = get_gradient_colors('Reds', len(supply_cols))

    wth_df = df_long[df_long['Variable'].isin(
        withdrawal) & df_long['Year'].isin(years)]
    wth_table = (
        wth_df.pivot_table(index='Year', columns='Variable',
                           values='Value', aggfunc='sum')
        .reindex(years, fill_value=0)
    )
    wth_table = -wth_table

    withdrawal_cmap = get_gradient_colors('Blues', len(wth_table.columns))

    # Create subplots with no vertical gap
    fig, (ax_sup, ax_wth) = plt.subplots(
        2, 1,
        figsize=(8, 6),
        sharex=True,
        gridspec_kw={'hspace': 0}
    )

    # Supply stackplot in reds
    sup_labels = [supply_dict[col] for col in sup_table.columns]
    ax_sup.stackplot(
        years,
        *[sup_table[col] for col in sup_table.columns],
        labels=sup_labels,
        colors=supply_cmap
    )
    ax_sup.set_title(f"{region}", loc='center', pad=20, fontsize=14)
    ax_sup.set_ylabel('Supply (km³/yr)')
    ax_sup.grid(True, color='lightgray', linewidth=0.5, alpha=0.5)
    # Reverse legend order
    handles, labels = ax_sup.get_legend_handles_labels()
    ax_sup.legend(
        [handles[i] for i in reversed(range(len(handles)))],
        [labels[i] for i in reversed(range(len(labels)))],
        loc='upper left', bbox_to_anchor=(1.02, 1), fontsize='small'
    )

    # Withdrawal stackplot in blues
    wth_labels = [withdrawal_dict[col] for col in wth_table.columns]
    ax_wth.stackplot(
        years,
        *[wth_table[col] for col in wth_table.columns],
        labels=wth_labels,
        colors=withdrawal_cmap
    )
    ax_wth.set_ylabel('Withdrawal (km³/yr)')
    ax_wth.set_xlabel('Year')
    ax_wth.grid(True, color='lightgray', linewidth=0.5, alpha=0.5)
    ax_wth.legend(loc='upper left', bbox_to_anchor=(1.02, 1), fontsize='small')

    plt.tight_layout()

    if "|" in region:
        region_filename = "World"
    else:
        region_filename = region
    filename = f"Water_balance_{region_filename}.png"
    save_path = os.path.join(output_dir, filename)
    plt.savefig(save_path, dpi=300)
    plt.show()
