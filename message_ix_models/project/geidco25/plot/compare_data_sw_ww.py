import os
from matplotlib.patches import Patch
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from message_ix_models.project.geidco25.data_processing.plot_dp import (
    supply_basins)

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

CHN = False

# -------------------------
# Read surface water sheet
# -------------------------
rename_dict_sw = {
    "qtot_7p0_high_2020": "qtot_7p0_high (2020)",
    "qtot_7p0_low_2025": "qtot_7p0_low (2025)",
    "China natural runoff_2018": "China Natural Runoff raster dataset (2018)",
    "Water Resource Bulletin_2020": "Water Resource Bulletin (2020)",
    "China water statistical yearbook_2022": "China Water Statistical Yearbook (2022)",
    "GRADES-hydroDL_2025": "GRADES-hydroDL (2025)"
}

df_sw = pd.read_excel(os.path.join(path, file), sheet_name="sw")
if CHN:
    # filter basin
    df_sw = df_sw[df_sw.basin.isin(CHN_basin)].copy()
    # filter col
    df_sw = df_sw[["basin",
                   "qtot_7p0_low_2025",
                   "China natural runoff_2018",
                   "Water Resource Bulletin_2020",
                   "China water statistical yearbook_2022"]]

    df_sw = df_sw.rename(columns=rename_dict_sw)

    width_sw = 0.17
else:
    # Filter basin
    df_sw = df_sw[df_sw['basin'] != 'China']
    # filter col
    df_sw = df_sw[["basin",
                   "qtot_7p0_low_2025",
                   "GRADES-hydroDL_2025"]]

    df_sw = df_sw.rename(columns=rename_dict_sw)

x_sw = np.arange(len(df_sw["basin"]))
colors_sw = [
    "royalblue" if b in supply_basins else "lightcoral" for b in df_sw["basin"]]

# -------------------------
# Read water withdrawal sheet
# -------------------------
df_ww = pd.read_excel(os.path.join(path, file), sheet_name="ww")
if CHN:
    # filter basin
    df_ww = df_ww[df_ww.basin.isin(CHN_basin)].copy()

rename_dict_ww = {
    "Base_RCP7_noint_noIBWT_2030": "Base_RCP7_noint_noIBWT (2030)",
    "Khan_SSP2_rcp60_2030": "Khan_SSP2_RCP60 (2030)"
}
df_ww = df_ww.rename(columns=rename_dict_ww)

x_ww = np.arange(len(df_ww["basin"]))
width_ww = 0.27
colors_ww = [
    "royalblue" if b in supply_basins else "lightcoral" for b in df_ww["basin"]]

# -------------------------
# Plot
# -------------------------
# fig, axes = plt.subplots(2, 1, figsize=(12, 14))

# ===== ax1：Surface Water =====
# ax = axes[0]
if CHN:
    fig, ax = plt.subplots(figsize=(12, 7))
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
        Patch(facecolor="gray", alpha=0.5,
              label="Water Resource Bulletin (2020)"),
        Patch(facecolor="gray", alpha=0.25,
              label="China Water Statistical Yearbook (2022)")
    ]
    ax.legend(handles=legend_sw)
else:
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 7), sharex='all',
                                   gridspec_kw={'height_ratios': [1, 3]})
    fig.subplots_adjust(hspace=0.05)

    ax1.bar(x_sw - width_ww/2,
            df_sw["qtot_7p0_low (2025)"], width_ww, color=colors_ww)
    ax1.bar(x_sw + width_ww/2, df_sw["GRADES-hydroDL (2025)"],
            width_ww, color=colors_ww, alpha=0.5)

    ax2.bar(x_sw - width_ww/2,
            df_sw["qtot_7p0_low (2025)"], width_ww, color=colors_ww)
    ax2.bar(x_sw + width_ww/2, df_sw["GRADES-hydroDL (2025)"],
            width_ww, color=colors_ww, alpha=0.5)

    ax2.set_ylim(0, 2000)
    ax1.set_ylim(4400, 5800)

    ax1.spines.bottom.set_visible(False)
    ax2.spines.top.set_visible(False)

    ax1.tick_params(axis='x', length=0)  # hide x ticks

    # broken axis line
    d = 0.5
    kwargs = dict(marker=[(-1, -d), (1, d)], markersize=10,
                  linestyle="none", color='k', mec='k', mew=1, clip_on=False)
    ax1.plot([0, 1], [0, 0], transform=ax1.transAxes, **kwargs)
    ax2.plot([0, 1], [1, 1], transform=ax2.transAxes, **kwargs)

    ax2.set_ylabel("km³/yr")
    ax2.yaxis.set_label_coords(-0.07, 0.7)
    ax2.set_xlabel("Basin")
    ax1.set_title("Comparison of Surface Water")
    ax2.set_xticks(x_sw)
    ax2.set_xticklabels(df_sw["basin"], rotation=45, ha="right")

    legend_sw = [
        Patch(facecolor="gray", label="qtot_7p0_low (2025)"),
        Patch(facecolor="gray", alpha=0.5, label="GRADES-hydroDL (2025)")
    ]
    ax1.legend(handles=legend_sw)

fig_name = "com_sw_global3.png"
plt.tight_layout()
plt.savefig(os.path.join(path, fig_name), dpi=300)
plt.show()

# ===== ax2：Water Withdrawal =====
fig, ax = plt.subplots(figsize=(12, 7))
# ax = axes[1]
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

fig_name = "com_ww_global2.png"
plt.tight_layout()
plt.savefig(os.path.join(path, fig_name), dpi=300)
plt.show()
