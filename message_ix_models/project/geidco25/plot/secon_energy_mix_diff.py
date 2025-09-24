import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

from message_ix_models.util import package_data_path
from message_ix_models.project.geidco25.data_processing.plot_dp import (
    stan_data_stru, fill_missing_region, add_noGEI_vars, clean_diff_df)

'''
Run compare_report.py first
Using gei reporting if GEI scenario
'''

# Read data
model = "MixG_GEIDCO5_SSP2_v6.1"
scen_a = "Base_RCP7_noint_noIBWT_t2"
scen_b = "Base_RCP7_noint_IBWT_t2"
data_file = (
    package_data_path().parents[0]
    / f"reporting_output/report_diff/diff_{scen_a}_{scen_b}.xlsx"
)
df = pd.read_excel(data_file, sheet_name="differences")
# becasue some bugs when running gei reporting with nexus scenario
df = fill_missing_region(df)

# output path
output_dir = package_data_path(
).parents[0] / f"reporting_output/plot_diff/{scen_a}_{scen_b}/secon_energy_mix_2060"
output_dir.mkdir(parents=True, exist_ok=True)

# Increase default font size for all text elements
plt.rcParams.update({
    'axes.titlesize': 26,
    'axes.labelsize': 24,
    'xtick.labelsize': 22,
    'ytick.labelsize': 22,
    'legend.fontsize': 24,
    'figure.titlesize': 26
})

# Define energy variables for secondary electricity
energy_vars = [
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

# labels
labels = ['Coal',
          'Gas',
          'Oil',
          'Geothermal',
          'Nuclear',
          'Biomass',
          'Hydro other',
          'Hydro GEI',
          'Solar other',
          'Solar GEI',
          'Wind other',
          'Wind GEI',
          'Other']

var_label_map = dict(zip(energy_vars, labels))

# colors
color_map = {
    'Biomass': '#9e76c3',
    'Coal': "#d43e33",
    'Gas': '#ff8b26',
    'Geothermal': '#c9a69e',
    'Hydro other': '#a7dde7',
    'Hydro GEI': '#a7dde7',
    'Nuclear': '#e377c2',
    'Oil': "#100303D7",
    'Solar other': "#f3e48d",
    'Solar GEI': "#f3e48d",
    'Wind other': '#a2e295',
    'Wind GEI': '#a2e295',
    'Other': 'grey'
}

# hatches
hatch = "\\\\"
hatch_map = {
    'Biomass': "",
    'Coal': "",
    'Gas': "",
    'Geothermal': "",
    'Hydro other': "",
    'Hydro GEI': hatch,
    'Nuclear': "",
    'Oil': "",
    'Solar other': "",
    'Solar GEI': hatch,
    'Wind other': "",
    'Wind GEI': hatch,
    'Other': ""
}

regions = [
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


def active_var_indices(energy_df, energy_vars):
    """
    abs>0 -> non-zero records
    """
    tmp = energy_df.reindex(columns=energy_vars,
                            fill_value=0).abs().sum(axis=0)
    return [i for i, var in enumerate(energy_vars) if tmp.get(var, 0) > 0]


df = clean_diff_df(df)
# calculate noGEI vars
df = add_noGEI_vars(df, id_cols=("region", "unit"))
df_long = stan_data_stru(df)

# filter by year
years = sorted(df_long['year'].unique())
years = [y for y in years if (y >= 2030) & (y <= 2060)]
# years = [y for y in years if (y >= 2030)]

# Determine unit label
unit_label = 'EJ/yr'


def panels():
    n_rows, n_cols = 4, 3
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(26, 27), sharex=True)
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
            # Plot stacked bars with increased width
            bottom_pos = np.zeros(len(years))
            bottom_neg = np.zeros(len(years))
            for j, var in enumerate(energy_vars):
                label = labels[j]
                vals = energy_df.get(var, pd.Series(0, index=years)).values

                # 拆分正值/负值（负值保持为负数，高度为负即可向下画）
                pos = np.where(vals > 0, vals, 0)
                neg = np.where(vals < 0, vals, 0)

                # 先画正值堆叠
                if np.any(pos):
                    ax.bar(
                        years, pos, bottom=bottom_pos, width=2,
                        color=color_map[label],
                        hatch=hatch_map.get(label, ""),
                        label=None   # 只在目标子图加图例项
                    )
                    bottom_pos += pos

                # 再画负值堆叠（注意 bottom 用负向基线，height 传负数）
                if np.any(neg):
                    ax.bar(
                        years, neg, bottom=bottom_neg, width=2,
                        color=color_map[label],
                        hatch=hatch_map.get(label, ""),
                        label=None
                    )
                    bottom_neg += neg

            ax.set_title(region)
            ax.set_ylabel(unit_label)
            ax.grid(True, color='lightgray', linestyle='--',
                    linewidth=0.5, alpha=0.7)
            if idx >= (n_rows - 1) * n_cols:
                ax.set_xlabel('Year')

        else:
            # Hide unused subplot
            ax.axis('off')

    # # Legend on first subplot only
    # # Reverse legend order on first subplot
    # handles, labs = axes[3].get_legend_handles_labels()
    # axes[3].legend(
    #     [handles[i] for i in reversed(range(len(handles)))],
    #     [labs[i] for i in reversed(range(len(labs)))],
    #     loc='upper left', bbox_to_anchor=(1.02, 1), fontsize='small'
    # )

    # ----- Create a whole label -----
    # # try to filter labels only exist in this df_long
    # df_label = df_long[df_long['variable'].isin(energy_vars) &
    #                    df_long['value'] != 0]
    # label_origin = df_label['variable'].unique()
    # labels2 = [var_label_map.get(v, v) for v in label_origin]

    handles = [
        Patch(
            facecolor=color_map[l],
            hatch=hatch_map.get(l, ""),
            label=l
        )
        for l in labels
    ]

    axes[2].legend(
        handles=[handles[i] for i in reversed(range(len(handles)))],
        labels=[labels[i] for i in reversed(range(len(labels)))],
        loc="upper left",
        bbox_to_anchor=(1.02, 1.09),
        labelspacing=0.3,
        frameon=False
    )

    plt.tight_layout()
    filename = f"Energy_mix_regions_diff.png"
    save_path = os.path.join(output_dir, filename)
    plt.savefig(save_path, dpi=300)
    plt.show()


def plot_by_region():
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
            # bottom = np.zeros(len(years))
            bottom_pos = np.zeros(len(years))
            bottom_neg = np.zeros(len(years))
            for j in idx_active:
                var = energy_vars[j]
                vals = energy_df[var].to_numpy()

                label = labels[j]

                # 拆分正值/负值（负值保持为负数，高度为负即可向下画）
                pos = np.where(vals > 0, vals, 0)
                neg = np.where(vals < 0, vals, 0)

                # 先画正值堆叠
                if np.any(pos):
                    ax.bar(
                        years, pos, bottom=bottom_pos, width=2,
                        color=color_map[label],
                        hatch=hatch_map.get(label, ""),
                        label=None   # 只在目标子图加图例项
                    )
                    bottom_pos += pos

                # 再画负值堆叠（注意 bottom 用负向基线，height 传负数）
                if np.any(neg):
                    ax.bar(
                        years, neg, bottom=bottom_neg, width=2,
                        color=color_map[label],
                        hatch=hatch_map.get(label, ""),
                        label=None
                    )
                    bottom_neg += neg

                handles = [
                    Patch(
                        facecolor=color_map[l],
                        hatch=hatch_map.get(l, ""),
                        label=l
                    )
                    for l in labels
                ]

                ax.legend(
                    handles=[handles[i]
                             for i in reversed(range(len(handles)))],
                    labels=[labels[i] for i in reversed(range(len(labels)))],
                    loc="upper left",
                    bbox_to_anchor=(1.02, 1.09),
                    labelspacing=0.3,
                    frameon=False
                )

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
        ax.grid(True, color='lightgray', linestyle='--',
                linewidth=0.5, alpha=0.7)

        plt.tight_layout()

        # save
        filename = f"Secon_energy_mix_{region}_diff.png"
        save_path = os.path.join(output_dir, filename)
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.show()


# panels()
plot_by_region()
