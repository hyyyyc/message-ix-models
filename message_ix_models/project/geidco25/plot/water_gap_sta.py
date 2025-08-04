import os
import pandas as pd
from message_ix_models.util import package_data_path
import matplotlib.pyplot as plt
import numpy as np

out_path = r'D:\IIASA\Data\Rosa_2025_NC_water gaps\my_result'

# Read BCU_name and BCU_id
FILE_id = "R12_basinName2id.csv"
PATH_id = package_data_path("geidco25", FILE_id)
id2name_df = pd.read_csv(PATH_id, index_col=0)

id2name_dict = id2name_df.set_index(['BCU_name'])['BASIN'].to_dict()

# Read IBWT in China
FILE_wt = "IBWT.csv"
PATH_wt = package_data_path("geidco25", FILE_wt)
wt = pd.read_csv(PATH_wt, index_col=0).pipe(
    lambda df: df[(df.status == 'Planned') & (df.time == 'year')]).pipe(
    lambda df: df.filter(items=['basin_dest_id', 'vol_yr_km3'])).pipe(
    lambda df: df.groupby('basin_dest_id').sum().reset_index()
)

# Add global IBWT from Julian
wt_glo = pd.DataFrame({
    'basin_dest_id': ['96|AFR', '96|MEA', '97|NAM', '125|LAM'],
    'vol_yr_km3': [23.79456, 0, 23.79456, 31.72608]
})
wt = pd.concat([wt, wt_glo])

# Read water gap statistic
# R12 BCU_name as the zonal .shp field
FILE = "R12_water_gap_sta.csv"
PATH = package_data_path("geidco25", FILE)
df = pd.read_csv(PATH, index_col=0).pipe(
    lambda df: df.applymap(
        lambda x: x * -1 if isinstance(x, (int, float)) else x)  # value*-1
)
# Add basin name according to BCU id
df['basin_name'] = [id2name_dict[value] for value in list(df.BCU)]
# Filter
# Sta: add '96|AFR' and '96|MEA'
df_dest = pd.merge(df, wt, left_on='BCU', right_on='basin_dest_id', how='inner').pipe(
    lambda df: df.drop(columns='basin_dest_id')).pipe(
    lambda df: df.groupby('basin_name').sum().reset_index()
)

# Calculate error bar
df_dest['15C_min'] = df_dest[['15C_h08_gfdl-esm4', '15C_h08_ipsl-cm6a-lr',
                              '15C_h08_mpi-esm1-2-hr', '15C_h08_mri-esm2-0', '15C_h08_ukesm1-0-ll']].min(axis=1)
df_dest['15C_max'] = df_dest[['15C_h08_gfdl-esm4', '15C_h08_ipsl-cm6a-lr',
                              '15C_h08_mpi-esm1-2-hr', '15C_h08_mri-esm2-0', '15C_h08_ukesm1-0-ll']].max(axis=1)
df_dest['15C_error_low'] = df_dest['15C_average'] - df_dest['15C_min']
df_dest['15C_error_high'] = df_dest['15C_max'] - df_dest['15C_average']

df_dest['3C_min'] = df_dest[['3C_h08_gfdl-esm4', '3C_h08_ipsl-cm6a-lr',
                             '3C_h08_mpi-esm1-2-hr', '3C_h08_mri-esm2-0', '3C_h08_ukesm1-0-ll']].min(axis=1)
df_dest['3C_max'] = df_dest[['3C_h08_gfdl-esm4', '3C_h08_ipsl-cm6a-lr',
                             '3C_h08_mpi-esm1-2-hr', '3C_h08_mri-esm2-0', '3C_h08_ukesm1-0-ll']].max(axis=1)
df_dest['3C_error_low'] = df_dest['3C_average'] - df_dest['3C_min']
df_dest['3C_error_high'] = df_dest['3C_max'] - df_dest['3C_average']

# print(df_dest.columns)
# Save the data
df_dest.to_csv(os.path.join(out_path, 'water gaps vs water transfer.csv'))

# Plot
# Sort
df_dest = df_dest.sort_values(by='vol_yr_km3', ascending=False)

# X-axis and y-axis
x = np.arange(len(df_dest['basin_name']))
y_columns = ['baseline', '15C_average', '3C_average', 'vol_yr_km3']
width = 0.2

# Font size
plt.rcParams.update({'font.size': 16})

fig, ax = plt.subplots(figsize=(10, 7))

bar1 = ax.bar(x - width * 1.5,
              df_dest['baseline'],
              width,
              label='Water gap under baseline',
              color='#9AA6B2')
bar2 = ax.bar(x - width / 2,
              df_dest['15C_average'],
              width,
              label='Water gap under 1.5°C warming',
              color='#FFA955',
              yerr=[df_dest['15C_error_low'], df_dest['15C_error_high']],
              capsize=5,
              error_kw={'ecolor': '#ff8208', 'elinewidth': 2, 'capsize': 5})
bar3 = ax.bar(x + width / 2,
              df_dest['3C_average'],
              width,
              label='Water gap under 3°C warming',
              color='#F75A5A',
              yerr=[df_dest['3C_error_low'], df_dest['3C_error_high']],
              capsize=5,
              error_kw={'ecolor': '#f31111', 'elinewidth': 2, 'capsize': 5})
bar4 = ax.bar(x + width * 1.5,
              df_dest['vol_yr_km3'],
              width,
              label='Designed water transfer volume',
              color='#6DE1D2')

# Add labels
# ax.set_xlabel('Basin')
ax.set_ylabel('Values (km$^3$/year)')
# ax.set_title('Comparison of Baseline, 15C Average, 3C Average, and Vol Yr Km3')
ax.set_xticks(x)
ax.set_xticklabels(df_dest['basin_name'], rotation=25, ha="right")

# Add legend
ax.legend()

# Grid
ax.grid(axis='y', linestyle='--', alpha=0.7)

# Save
plt.savefig(os.path.join(out_path, 'water gap vs water transfer2.png'),
            dpi=300, bbox_inches='tight')

# Show plot
plt.tight_layout()
plt.show()
