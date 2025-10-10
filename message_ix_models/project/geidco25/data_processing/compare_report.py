import pandas as pd
import os
import re

from message_ix_models.util import package_data_path
from message_ix_models.project.geidco25.data_processing.plot_dp import (
    stan_data_stru)

ID_COLS = ["region", "variable", "unit"]


def to_wide(df, col, suffix):
    wide = df.pivot_table(
        index=ID_COLS,
        columns="year",
        values=col,
        aggfunc="first"
    ).reset_index()
    wide = wide.rename(
        columns={y: f"{y}_{suffix}" for y in wide.columns if isinstance(y, int)})
    return wide


def compare_and_pivot(file_a, file_b, out_file="comparison_result.xlsx"):
    a_long = stan_data_stru(file_a)
    b_long = stan_data_stru(file_b)

    merged = a_long.merge(
        b_long,
        on=ID_COLS + ["year"],
        how="outer",
        suffixes=("_A", "_B"),
        indicator=True
    )
    # IBWT in "noIBWT" scenarios is NaN
    merged['value_A'] = merged['value_A'].fillna(0)

    # differences
    # includes vars both in A and B, and vars only in B
    diffs = merged.loc[(merged["_merge"] == "both") | (
        merged["_merge"] == "right_only")].copy()
    diffs["diff"] = diffs["value_B"] - diffs["value_A"]

    # vars only in A or B
    only_in_a = merged.loc[merged["_merge"] == "left_only"].copy()
    only_in_b = merged.loc[merged["_merge"] == "right_only"].copy()

    # long to wide
    diffs_wide = (
        to_wide(diffs, "value_A", "A")
        .merge(to_wide(diffs, "value_B", "B"), on=ID_COLS, how="outer")
        .merge(to_wide(diffs, "diff", "diff"), on=ID_COLS, how="outer")
    )
    only_in_a_wide = to_wide(only_in_a, "value_A", "A")
    only_in_b_wide = to_wide(only_in_b, "value_B", "B")

    # export
    with pd.ExcelWriter(out_file, engine="openpyxl") as writer:
        diffs_wide.to_excel(writer, sheet_name="differences", index=False)
        only_in_a_wide.to_excel(writer, sheet_name="only_in_A", index=False)
        only_in_b_wide.to_excel(writer, sheet_name="only_in_B", index=False)
    # with pd.ExcelWriter(out_file, engine="openpyxl") as writer:
    #     diffs_wide.to_excel(writer, sheet_name="differences", index=False)

    print(f"Output file: {out_file}")


# ----- Read Data -----
model = "MixG_GEIDCO5_SSP2_v6.1"
scen_a = "Base_RCP7_noint_IBWT_t2_nexus"
scen_b = "Base_RCP7_int_IBWT_t2_nexus"

if "nexus" in scen_a:
    data_a = (
        package_data_path().parents[0]
        / f"reporting_output/{model}_{scen_a}.csv"
    )
    data_b = (
        package_data_path().parents[0]
        / f"reporting_output/{model}_{scen_b}.csv"
    )

    df_a = pd.read_csv(data_a)
    df_b = pd.read_csv(data_b)
else:
    data_a = (
        package_data_path().parents[0]
        / f"reporting_output/report_full_t2/{model}_{scen_a}.xlsx"
    )
    data_b = (
        package_data_path().parents[0]
        / f"reporting_output/report_full_t2/{model}_{scen_b}.xlsx"
    )

    df_a = pd.read_excel(data_a, sheet_name="data")
    df_b = pd.read_excel(data_b, sheet_name="data")

# ----- Output -----
output_dir = package_data_path(
).parents[0] / f"reporting_output/report_diff"
output_dir.mkdir(parents=True, exist_ok=True)
output_name = f"diff_{scen_a}_{scen_b}.xlsx"

compare_and_pivot(df_a, df_b, os.path.join(output_dir, output_name))
