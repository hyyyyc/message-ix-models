import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from message_ix_models.util import package_data_path
from message_ix_models.project.geidco25.plot.water_ibwt_reporting import stan_data_stru

# Read data
model = "MixG_GEIDCO5_SSP2_v6.1"
scen = "Base_RCP7_noint_IBWT_t1"
data = (
    package_data_path().parents[0]
    / f"reporting_output/{model}_{scen}_nexus.csv"
)
df = pd.read_csv(data)
scenario = scen

# Output path
output_dir = package_data_path(
).parents[0] / f"reporting_output/plot_ibwt/{scenario}/water_balance"
output_dir.mkdir(parents=True, exist_ok=True)

# font size
plt.rcParams.update({
    "font.size": 15,
    "axes.titlesize": 14,
    "axes.labelsize": 12,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
    "legend.fontsize": 15
})

# Define variables
supply = [
    "Water Extraction|Surface Water",
    "Water Extraction|Groundwater",
    "Water Waste|Reuse",
    "Water Extraction|Fossil Groundwater",
    "Water Extraction|Seawater|Desalination"
]

supply_ibwt = [
    "Water Extraction|Surface Water",
    "Water Transfer|Interbasin Water Transfer",
    "Water Extraction|Groundwater",
    "Water Waste|Reuse",
    "Water Extraction|Fossil Groundwater",
    "Water Extraction|Seawater|Desalination"
]

withdrawal = [
    "Water Withdrawal|Electricity|Cooling|Fresh Water",
    "Water Withdrawal|Irrigation_in",
    "Water Withdrawal|Industrial Water",
    "Water Withdrawal|Municipal Water"
]

supply_label = [
    'Surface Water',
    'IBWT',
    'Groundwater',
    'Reuse',
    'Fossil Groundwater',
    'Desalination'
]

withdrawal_label = [
    'Electricity',
    'Irrigation',
    'Industrial Water',
    'Municipal Water'
]

supply_dict = dict(zip(supply_ibwt, supply_label))
withdrawal_dict = dict(zip(withdrawal, withdrawal_label))

# Filter data for region
# regions = ['World',
#            'China',
#            'Eastern Europe',
#            'Former Soviet Union',
#            'Latin America',
#            'Middle East and Africa',
#            'North America',
#            'Pacific Asia',
#            'Pacific OECD',
#            'Rest of Centrally planned Asia',
#            'South Asia',
#            'Subsaharan Africa',
#            'Western Europe']

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


def get_gradient_colors(cmap_name, n, highlight_index=None, highlight_color=None):
    '''
    Prepare color gradients with special color for water extraction and withdrawal.
    If highlight_index and highlight_color are given, insert highlight_color
    at the specified index and shift the rest of the colors.
    '''
    cmap = plt.get_cmap(cmap_name)
    colors = [cmap(0.3 + 0.7 * i / (n - 1)) for i in range(n)]

    if highlight_index is not None and highlight_color is not None:
        # insert highlight_color, shift the later color down the sequence
        colors.insert(highlight_index, highlight_color)

    return colors


def add_up_vars(df: pd.DataFrame) -> pd.DataFrame:
    # Add up variables
    ind_vars = [
        "Water Withdrawal|Industrial Water|Unconnected Eff",
        "Water Withdrawal|Industrial Water|Unconnected",
    ]
    df_ind = df[df["variable"].isin(ind_vars)].copy()

    if not df_ind.empty:
        # group by except variable, value
        group_cols = [c for c in df_ind.columns if c not in [
            "variable", "value"]]
        df_ind_sum = (
            df_ind.groupby(group_cols, as_index=False, sort=False)["value"]
            .sum()
            .assign(variable="Water Withdrawal|Industrial Water")
        )
        # substitute
        df = pd.concat(
            [df[~df["variable"].isin(ind_vars)], df_ind_sum],
            ignore_index=True
        )
    return df


df = add_up_vars(df)
df = stan_data_stru(df)

for i, region in enumerate(regions):
    # Filter by region
    df_filter = df[df['region'].str.contains(region, na=False)]

    df_long = df_filter

    # Filter by year
    years = sorted(df_long['year'].unique())
    years = [y for y in years if y >= 2030]
    # years = [y for y in years if (y >= 2030) & (y <= 2055)]

    # regions with IBWT
    if region in ['World', 'R12_CHN', 'R12_AFR'] and "noint_noIBWT" not in scen:
        # Pivot supply and withdrawal
        sup_df = df_long[df_long['variable'].isin(
            supply_ibwt) & df_long['year'].isin(years)]
        sup_table = (
            sup_df.pivot_table(index='year', columns='variable',
                               values='value', aggfunc='sum')
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
            supply_cols)-1, highlight_idx, highlight_color)
    else:
        # Pivot supply and withdrawal
        sup_df = df_long[df_long['variable'].isin(
            supply) & df_long['year'].isin(years)]
        sup_table = (
            sup_df.pivot_table(index='year', columns='variable',
                               values='value', aggfunc='sum')
            .reindex(years, fill_value=0)
        )
        # Reorder columns to desired sequence
        sup_table = sup_table[supply]

        # Identify index of Interbasin in supply_table columns
        supply_cols = list(sup_table.columns)
        # Use bright crimson for highlight
        supply_cmap = get_gradient_colors('Reds', len(supply_cols))

    wth_df = df_long[df_long['variable'].isin(
        withdrawal) & df_long['year'].isin(years)]
    wth_table = (
        wth_df.pivot_table(index='year', columns='variable',
                           values='value', aggfunc='sum')
        .reindex(years, fill_value=0)
    )
    wth_table = -wth_table

    withdrawal_cmap = get_gradient_colors('Blues', len(wth_table.columns))

    # Create subplots with no vertical gap
    fig, (ax_sup, ax_wth) = plt.subplots(
        2, 1,
        figsize=(9, 6),
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
