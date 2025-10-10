import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

from message_ix_models.util import package_data_path
from message_ix_models.project.geidco25.data_processing.plot_dp import (
    stan_data_stru, route2basin)

# ----- Read data -----
model = "MixG_GEIDCO5_SSP2_v6.1"
scen = "Base_RCP7_noint_IBWT_t3"
data = package_data_path().parents[0] / \
    f"reporting_output/{model}_{scen}_nexus.csv"
df = pd.read_csv(data)

# Designed capacity
df_designed_cap = pd.DataFrame({
    'route': ['route11', 'route12', 'route13', 'route14', 'route15', 'route16', 'route17', 'route18'],
    'designed': [17.4, 15, 10, 20, 9.52, 14.27, 23.79, 31.73]
})

# ----- Output path -----
output_dir = package_data_path(
).parents[0] / f"reporting_output/plot_ibwt/{scen}/IBWT_2060"
output_dir.mkdir(parents=True, exist_ok=True)

# ----- Settings -----
route_colors = {
    'Yangtze->Huang He': '#2ca02c',
    'Ganges Bramaputra->Huang He': '#d52122',
    'Yangtze->Ziya He Interior': '#9366bd',
    'Congo->Nile': '#874e43'
}
route_list = list(route_colors.keys())
color_list = list(route_colors.values())

# ----- Data process -----
df_long = stan_data_stru(df)
# Filter IBWT and year
df_long_filter = df_long[
    df_long['variable'].str.contains('Interbasin Water Transfer', na=False)
    & df_long['variable'].str.contains('route', na=False)
    & df_long['variable'].str.contains('Capacity', na=False)
    & df_long['variable'].str.contains('Planned', na=False)
    & df_long['region'].str.contains('World', na=False)
    & (df_long['year'] <= 2060)]

# split variable
# Variable: theme | subvar | planned or exsiting | route
split_cols = df_long_filter['variable'].str.split('|', expand=True)
split_cols.columns = ['theme', 'subvar', 'PE', 'route']
df_long_filter = pd.concat([df_long_filter, split_cols], axis=1)

# Calculate annual capacity
df_annual_cap = df_long_filter.groupby(
    'route', as_index=False)['value'].mean().rename(columns={'value': 'optimized'})

# Merge
df = pd.merge(df_annual_cap, df_designed_cap)
df['route'] = df['route'].map(route2basin)
df = df[~(df['optimized'] == 0)]

# ----- Plotting -----
fig, ax = plt.subplots(figsize=(6, 5))
# fig, ax = plt.subplots(figsize=(9, 4))
x_route = np.arange(len(df["route"]))
colors = df["route"].map(route_colors)
new_route_labels = df["route"].str.replace("->", "\n->", regex=False)
width = 0.27

ax.bar(x_route - width/2 - width/8,
       df["optimized"], width, color=colors)
ax.bar(x_route + width/2 + width/8, df["designed"],
       width, color=colors, alpha=0.5)

ax.set_ylabel("km³/yr")
# ax.set_xlabel("Water transfer route")
ax.set_title("Annual water transfer capacity \n(Optimized vs. Designed)")
ax.set_xticks(x_route)
ax.set_xticklabels(new_route_labels, rotation=45, ha="center")

legend = [
    Patch(facecolor="gray", label="Optimized"),
    Patch(facecolor="gray", alpha=0.5, label="Designed")
    # Patch(facecolor=color_list[0], label=route_list[0]),
    # Patch(facecolor=color_list[1], label=route_list[1]),
    # Patch(facecolor=color_list[2], label=route_list[2]),
    # Patch(facecolor=color_list[3], label=route_list[3])
]
ax.legend(handles=legend
          # loc="upper left", bbox_to_anchor=(1.02, 1)
          )

plt.tight_layout()
# Save
filename = f"compare_designed_capacity2.png"
save_path = os.path.join(output_dir, filename)
plt.savefig(save_path, dpi=300)
plt.show()
