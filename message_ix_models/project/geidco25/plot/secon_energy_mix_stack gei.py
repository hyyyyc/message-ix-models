import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

from message_ix_models.util import package_data_path
from message_ix_models.project.geidco25.data_processing.plot_dp import (
    stan_data_stru, filter_series_labels_colors, fill_missing_region, add_noGEI_vars)

# Read data
model = "MixG_GEIDCO5_SSP2_v6.1"
scen = "Base_RCP7_int_noIBWT_t2"
data_file = (
    package_data_path().parents[0]
    / f"reporting_output/report_full_t2/{model}_{scen}.xlsx"
)
df = pd.read_excel(data_file, sheet_name="data")
# becasue some bugs when running gei reporting with nexus scenario
df = fill_missing_region(df)
# calculate noGEI vars
df = add_noGEI_vars(df)
scenario = scen

# Output path
output_dir = package_data_path(
).parents[0] / f"reporting_output/plot_ibwt/{scenario}/energy_mix"
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
          'Hydro',
          'Hydro GEI',
          'Solar',
          'Solar GEI',
          'Wind',
          'Wind GEI',
          'Other']

# colors
color_map = {
    'Biomass': '#9e76c3',
    'Coal': "#d43e33",
    'Gas': '#ff8b26',
    'Geothermal': '#c9a69e',
    'Hydro': '#a7dde7',
    'Hydro GEI': '#a7dde7',
    'Nuclear': '#e377c2',
    'Oil': "#100303D7",
    'Solar': "#f3e48d",
    'Solar GEI': "#f3e48d",
    'Wind': '#a2e295',
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
    'Hydro': "",
    'Hydro GEI': hatch,
    'Nuclear': "",
    'Oil': "",
    'Solar': "",
    'Solar GEI': hatch,
    'Wind': "",
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


df_long = stan_data_stru(df)

# filter by year
years = sorted(df_long['year'].unique())
# years = [y for y in years if (y >= 2030) & (y <= 2055)]
years = [y for y in years if y >= 2030]

# Determine unit label
unit_label = 'EJ/yr'


def panels():
    n_rows, n_cols = 3, 3
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(29, 26), sharex=True)
    axes = axes.flatten()

    regions = [
        'World',
        'China',
        # 'Eastern Europe',
        'Former Soviet Union',
        'Latin America',
        # 'Middle East and Africa',
        'North America',
        'Pacific Asia',
        'Pacific OECD',
        # 'Rest of Centrally planned Asia',
        # 'South Asia',
        'Subsaharan Africa',
        # 'Western Europe'
    ]

    legend_ax = axes[-1]  # 右下角留作图例
    legend_ax.axis("off")  # 关掉坐标轴
    legend_labels = labels  # 你的分类顺序

    for idx, ax in enumerate(axes):
        if idx < len(regions):
            region = regions[idx]
            if region != "World":
                # Filter for region
                df_reg = df_long[df_long['region'].str.contains(
                    region, na=False)]
                # Pivot to wide format
                energy_df = (
                    df_reg[df_reg['variable'].isin(energy_vars)]
                    .pivot_table(index='year', columns='variable', values='value', aggfunc='sum')
                    .reindex(years, fill_value=0)
                )
            else:
                # sum up regions to world
                df_energy = df_long[(df_long['variable'].isin(energy_vars)) &
                                    ~df_long["region"].isin(["World", "Missing region"])]
                energy_df = (
                    df_energy
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
            # if idx == 7:
            #     polys = ax.stackplot(
            #         years,
            #         *series,
            #         colors=[color_map[i] for i in labels],
            #         labels=labels,
            #         alpha=0.9
            #     )

            polys = ax.stackplot(
                years,
                *series,
                colors=[color_map[i] for i in labels],
                alpha=0.9
            )

            for poly, lab in zip(polys, labels):
                poly.set_hatch(hatch_map.get(lab, ""))
                # poly.set_edgecolor("black")
                poly.set_linewidth(0.5)
                poly.set_alpha(1.0)

            ax.set_title(region)
            ax.set_ylabel(unit_label)
            ax.grid(True, color='lightgray', linestyle='--',
                    linewidth=0.5, alpha=0.7)
            if idx >= (n_cols*(n_rows - 1)):
                ax.set_xlabel('Year')
        else:
            pass

    # —— 只在最后一格画图例 ——
    # 用代理 Patch，保证图例项与颜色/纹理一致
    handles = [
        Patch(
            label=lab,
            facecolor=color_map[lab],
            hatch=hatch_map.get(lab, ""),
            # edgecolor="black"
        )
        for lab in legend_labels
    ]

    # 如果要反转顺序，解开下面两行
    handles = list(reversed(handles))
    legend_labels = list(reversed(legend_labels))

    legend_ax.legend(
        handles, legend_labels,
        loc="upper left", frameon=False, ncol=1
    )

    plt.tight_layout()
    filename = "Second_energy_mix_regions_stackplot3.png"
    save_path = os.path.join(output_dir, filename)
    plt.savefig(save_path, dpi=300)
    plt.show()


def plot_by_regions():
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
        series_plot, labels_plot = filter_series_labels_colors(
            energy_df, energy_vars, labels
        )

        # plot
        fig, ax = plt.subplots(figsize=(12, 7))
        ax.stackplot(
            years,
            *series_plot,
            colors=[color_map[i] for i in labels_plot],
            labels=labels_plot,
            alpha=0.9
        )

        ax.set_title(region)
        ax.set_xlabel('Year')
        ax.set_ylabel('Electricity (EJ/yr)')
        ax.grid(True, color='lightgray', linestyle='--',
                linewidth=0.5, alpha=0.7)

        # Reverse legend order
        handles, labels_re = ax.get_legend_handles_labels()
        ax.legend(
            [handles[i] for i in reversed(range(len(handles)))],
            [labels_re[i] for i in reversed(range(len(labels_re)))],
            loc='upper left', bbox_to_anchor=(1.02, 1),
            frameon=False, ncol=1, fontsize='small'
        )

        plt.tight_layout()

        # save
        filename = f"Second_energy_mix_gei_{region}_stackplot.png"
        save_path = os.path.join(output_dir, filename)
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.show()


panels()
