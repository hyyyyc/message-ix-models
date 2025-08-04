import os
import pandas as pd
import matplotlib.pyplot as plt

from message_ix_models.util import package_data_path

# Read data
model = "MESSAGE_GLOBIOM_SSP2_v6.1_ibwt_t4"
scen = "baseline_nexus_7_high_ibwt_t4"
data = package_data_path().parents[0] / \
    f"reporting_output/{model}_{scen}.csv"
df = pd.read_csv(data)
version = scen.split('_')[-1]


def plot_by_region(df: pd.DataFrame) -> None:
    # Filter IBWT
    df_filtered = df[
        df['Variable'].str.contains('Interbasin Water Transfer', na=False)
        & df['Variable'].str.contains('Existing|Planned', na=False)
        & ~df['Variable'].str.contains('route', na=False)
        & df['Region'].str.startswith('B', na=False)]

    # Wide to long
    df_cols = df_filtered.columns.to_list()
    df_cols_id = ['Model', 'Scenario', 'Region', 'Variable', 'Unit']
    df_cols_yr = [x for x in df_cols if x not in df_cols_id]
    df_long = df_filtered.melt(
        id_vars=df_cols_id,
        value_vars=df_cols_yr,
        var_name='Year',
        value_name='Value'
    )
    df_long['Year'] = df_long['Year'].astype(int)

    # Variable: theme | subvar | planned or exsiting
    split_cols = df_long['Variable'].str.split('|', expand=True)
    split_cols.columns = ['theme', 'subvar', 'PE']
    df_long = pd.concat([df_long, split_cols], axis=1)

    # Convert BCU name to basin name
    BCU2basin = {
        "B62|CHN": "Huang He",
        "B159|CHN": "Yangtze",
        "B162|CHN": "Ziya He Interior",
        "B35|CHN": "China Coast",
        "B105|CHN": "Ob",
        "B54|CHN": "Gobi Interior",
        "B53|CHN": "Ganges Bramaputra",
        "B38|AFR": "Congo",
        "B90|NAM": "Mississipy",
        "B9|LAM": "Amazon"
    }

    # Themes to be plotted
    themes = [
        'Capacity',
        'Water Transfer',
        'Investment',
        'Total Operation Management Cost',
        'Final Energy'
    ]

    # Output path
    output_dir = package_data_path(
    ).parents[0] / f"reporting_output/plot_ibwt/{version}"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Color
    region_colors = {'B159|CHN': '#1f77b4',
                     'B38|AFR': '#ff7f0e',
                     'B53|CHN': '#2ca02c',
                     'B62|CHN': '#d62728',
                     'B90|NAM': '#9467bd',
                     'B9|LAM': '#8c564b'}

    # Plot
    for theme in themes:
        df_theme = df_long[df_long['theme'].str.strip() == theme]
        if df_theme.empty:
            print(f"'{theme}' no data")
            continue

        plt.figure(figsize=(8, 5))
        # Group by region and Existing/Planned
        for (region, cls), group in df_theme.groupby(['Region', 'PE']):
            # If not in the dict, use original region
            basin_name = BCU2basin.get(region, region)
            linestyle = '-' if cls.strip().lower() == 'existing' else '--'
            plt.plot(
                group['Year'],
                group['Value'],
                label=f"{basin_name} ({cls})",
                color=region_colors[region],
                linestyle=linestyle,
                marker='o',
                markersize=4
            )

        plt.grid(True, color='lightgray', linestyle='-',
                 linewidth=0.5, alpha=0.5)

        # Add unit
        units = df_theme['Unit'].unique()
        ylabel = f"Value ({units[0]})" if len(units) == 1 else "Value"

        plt.title(f"{theme} — Interbasin Water Transfer")
        plt.xlabel('Year')
        plt.ylabel(ylabel)

        # Legend
        # Get legend
        handles, labels = plt.gca().get_legend_handles_labels()
        existing_idxs = [i for i, lab in enumerate(
            labels) if '(Existing)' in lab]
        planned_idxs = [i for i, lab in enumerate(
            labels) if '(Planned)' in lab]
        new_order = existing_idxs + planned_idxs
        # Reorder
        plt.legend(
            [handles[i] for i in new_order],
            [labels[i] for i in new_order]
        )

        plt.grid(True)
        plt.tight_layout()
        # Save
        filename = f"{theme.replace(' ', '_')}.png"
        save_path = os.path.join(output_dir, filename)
        plt.savefig(save_path, dpi=300)
        plt.show()


def plot_by_route(df: pd.DataFrame) -> None:
    # Filter IBWT
    df_filtered = df[
        df['Variable'].str.contains('Interbasin Water Transfer', na=False)
        & df['Variable'].str.contains('route', na=False)
        & df['Region'].str.contains('World', na=False)]

    df_cols = df_filtered.columns.to_list()
    df_cols_id = ['Model', 'Scenario', 'Region', 'Variable', 'Unit']
    df_cols_yr = [x for x in df_cols if x not in df_cols_id]

    # Remove all value = 0
    # Fill NaN as 0, check all value = 0
    mask_nonzero = ~(df_filtered[df_cols_yr].fillna(0).eq(0).all(axis=1))
    df_filtered = df_filtered.loc[mask_nonzero].copy()

    # Wide to long
    df_long = df_filtered.melt(
        id_vars=df_cols_id,
        value_vars=df_cols_yr,
        var_name='Year',
        value_name='Value'
    )
    df_long['Year'] = df_long['Year'].astype(int)

    # Variable: theme | subvar | planned or exsiting | route
    split_cols = df_long['Variable'].str.split('|', expand=True)
    split_cols.columns = ['theme', 'subvar', 'PE', 'route']
    df_long = pd.concat([df_long, split_cols], axis=1)

    # Planned or Existing
    plan_exist = [
        'Planned',
        'Existing'
    ]

    # Convert route_id to source_destination
    route2basin = {
        'route1': 'Yangtze->China Coast',
        'route2': 'Yangtze->Ziya He Interior',
        'route3': 'Yangtze->China Coast',
        'route4': 'Huang He->Ziya He Interior',
        'route5': 'Huang He->Ziya He Interior',
        'route6': 'Yangtze->Huang He',
        'route7': 'Yangtze->Ziya He Interor',
        'route8': 'Yangtze->Huang He',
        'route9': 'Ob->Gobi Interior',
        'route10': 'Huang He->Yangtze',
        'route11': 'Yangtze->Huang He',
        'route12': 'Ganges Bramaputra->Huang He',
        'route13': 'Ganges Bramaputra->Traim Interior',
        'route14': 'Yangtze->Ziya He Interior',
        'route15': 'Congo->Nile',
        'route16': 'Conga->Nile',
        'route17': 'Mississipy->Colorado',
        'route18': 'Amazon->Sao Francisco'
    }

    # Themes to be plotted
    themes = [
        'Capacity',
        'Water Transfer',
        'Investment',
        'Total Operation Management Cost',
        'Final Energy'
    ]

    # Output path
    output_dir = package_data_path(
    ).parents[0] / f"reporting_output/plot_ibwt/{version}"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Color
    routes = sorted(df_long['route'].unique())
    color_cycle = plt.rcParams['axes.prop_cycle'].by_key()['color']
    route_colors = {
        route: color_cycle[i % len(color_cycle)]
        for i, route in enumerate(routes)
    }

    # Plot
    for pe in plan_exist:
        df_pe = df_long[df_long['PE'].str.strip() == pe]
        for theme in themes:
            df_theme = df_pe[df_pe['theme'].str.strip() == theme]
            if df_theme.empty:
                print(f"'{theme}' no data")
                continue

            plt.figure(figsize=(8, 5))
            # Group by region and Existing/Planned
            for key, group in df_theme.groupby(['route']):
                route = key[0]
                linestyle = '-' if (group['PE'] == 'Existing').all() else '--'
                plt.plot(
                    group['Year'],
                    group['Value'],
                    label=route2basin[route],
                    color=route_colors[route],
                    linestyle=linestyle,
                    marker='o',
                    markersize=4
                )

            plt.grid(True, color='lightgray', linestyle='-',
                     linewidth=0.5, alpha=0.5)

            # Add unit
            units = df_theme['Unit'].unique()
            ylabel = f"Value ({units[0]})" if len(units) == 1 else "Value"

            plt.title(f"{theme} — Interbasin Water Transfer")
            plt.xlabel('Year')
            plt.ylabel(ylabel)

            # Legend
            plt.legend()

            plt.grid(True)
            plt.tight_layout()
            # Save
            filename = f"{pe}_{theme.replace(' ', '_')}_by_route.png"
            save_path = os.path.join(output_dir, filename)
            plt.savefig(save_path, dpi=300)
            plt.show()
