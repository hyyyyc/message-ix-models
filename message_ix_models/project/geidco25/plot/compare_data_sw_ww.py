import os
from matplotlib.patches import Patch
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

path = r'D:\IIASA\Data\calibration'
file = 'com_water_balance.xlsx'

plt.rcParams.update({
    'font.size': 20,
    'axes.titlesize': 16,
    'axes.labelsize': 14,
    'xtick.labelsize': 14,
    'ytick.labelsize': 14,
    'legend.fontsize': 16,
    'figure.titlesize': 18
})

CHN_basin = ["Yangtze", "Ganges Bramaputra", "Tarim", "Huang He",
             "Ziya He", "China Coast"]

supply_basins = ["Yangtze", "Ganges Bramaputra", "Mississipy", "Amazon"]

# -------------------------
# Read surface water sheet
# -------------------------
df_sw = pd.read_excel(os.path.join(path, file), sheet_name="sw")
# filter basin
df_sw = df_sw[df_sw.basin.isin(CHN_basin)].copy()
# filter col
df_sw = df_sw[["basin",
               "qtot_7p0_low_2025",
               "China natural runoff_2018",
               "Water Resource Bulletin_2020",
               "China water statistical yearbook_2022"]]

rename_dict_sw = {
    "qtot_7p0_high_2020": "qtot_7p0_high (2020)",
    "qtot_7p0_low_2025": "qtot_7p0_low (2025)",
    "China natural runoff_2018": "China Natural Runoff raster dataset (2018)",
    "Water Resource Bulletin_2020": "Water Resource Bulletin (2020)",
    "China water statistical yearbook_2022": "China Water Statistical Yearbook (2022)"
}
df_sw = df_sw.rename(columns=rename_dict_sw)

x_sw = np.arange(len(df_sw["basin"]))
width_sw = 0.17
colors_sw = [
    "royalblue" if b in supply_basins else "lightcoral" for b in df_sw["basin"]]

# -------------------------
# Read water withdrawal sheet
# -------------------------
df_ww = pd.read_excel(os.path.join(path, file), sheet_name="ww")
# filter basin
df_ww = df_ww[df_ww.basin.isin(CHN_basin)].copy()

rename_dict_ww = {
    "Base_RCP7_noint_noIBWT_2030": "Base_RCP7_noint_noIBWT (2030)",
    "Khan_SSP2_rcp60_2030": "Khan_SSP2_RCP60 (2030)"
}
df_ww = df_ww.rename(columns=rename_dict_ww)

x_ww = np.arange(len(df_ww["basin"]))
width_ww = 0.35
colors_ww = [
    "royalblue" if b in supply_basins else "lightcoral" for b in df_ww["basin"]]

# -------------------------
# Plot
# -------------------------
fig, axes = plt.subplots(2, 1, figsize=(12, 14))

# ===== ax1：Surface Water =====
ax = axes[0]
ax.bar(x_sw - 3*(width_sw/2),
       df_sw["qtot_7p0_low (2025)"], width_sw, color=colors_sw)
ax.bar(x_sw - width_sw/2,
       df_sw["China Natural Runoff raster dataset (2018)"], width_sw, color=colors_sw, alpha=0.75)
ax.bar(x_sw + width_sw/2,
       df_sw["Water Resource Bulletin (2020)"], width_sw, color=colors_sw, alpha=0.5)
ax.bar(x_sw + 3*(width_sw/2),
       df_sw["China Water Statistical Yearbook (2022)"], width_sw, color=colors_sw, alpha=0.25)

ax.set_ylabel("km³/yr")
# ax.set_xlabel("Basin")
ax.set_title("Comparison of Surface Water")
ax.set_xticks(x_sw)
ax.set_xticklabels(df_sw["basin"], rotation=45, ha="right")

legend_sw = [
    Patch(facecolor="gray", label="qtot_7p0_low (2025)"),
    Patch(facecolor="gray", alpha=0.75,
          label="China Natural Runoff raster dataset (2018)"),
    Patch(facecolor="gray", alpha=0.5, label="Water Resource Bulletin (2020)"),
    Patch(facecolor="gray", alpha=0.25,
          label="China Water Statistical Yearbook (2022)")
]
ax.legend(handles=legend_sw)

# ===== ax2：Water Withdrawal =====
ax = axes[1]
ax.bar(x_ww - width_ww/2,
       df_ww["Base_RCP7_noint_noIBWT (2030)"], width_ww, color=colors_ww)
ax.bar(x_ww + width_ww/2, df_ww["Khan_SSP2_RCP60 (2030)"],
       width_ww, color=colors_ww, alpha=0.5)

ax.set_ylabel("km³/yr")
ax.set_xlabel("Basin")
ax.set_title("Comparison of Water Withdrawal")
ax.set_xticks(x_ww)
ax.set_xticklabels(df_ww["basin"], rotation=45, ha="right")

legend_ww = [
    Patch(facecolor="gray", label="Base_RCP7_noint_noIBWT (2030)"),
    Patch(facecolor="gray", alpha=0.5, label="Khan_SSP2_RCP60 (2030)")
]
ax.legend(handles=legend_ww)

fig_name = "com_sw_ww_CHN.png"
plt.tight_layout()
plt.savefig(os.path.join(path, fig_name), dpi=300)
plt.show()
