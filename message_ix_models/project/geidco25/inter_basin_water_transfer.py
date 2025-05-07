import pandas as pd

from message_ix import make_df
from message_ix_models.model.water.utils import map_yv_ya_lt
from message_ix_models.util import (
    package_data_path,
    broadcast
)

basin2BCU = {
    "Huang He": "62|CHN",
    "Yangtze": "159|CHN",
    "Ziya He Interior": "162|CHN",
    "China Coast": "35|CHN",
    "Ob": "105|CHN",
    "Gobi Interior": "54|CHN",
    "Ganges Bramaputra": "53|CHN"
}
basin2BCU = {key: 'B'+value for key, value in basin2BCU.items()}

FILE = "IBWT_yr.csv"
PATH = package_data_path("geidco25", FILE)
df = pd.read_csv(PATH)
# presettings for node(e.g.B11|CHN) and region(e.g.R11_CHN)
df['node_in'] = [basin2BCU[value] for value in list(df.source)]
df['node_out'] = [basin2BCU[value] for value in list(df.recipient)]
df['routes'] = df["node_in"] + '_' + df["node_out"]
df['region'] = 'R11_CHN'
df_exist = df[df.status == "Existing"]
df_plan = df[df.status == "Planned"]


def inter_basin_water_transfer_exist() -> dict[str, pd.DataFrame]:
    """Add existing inter basin water transfers (IBWT)
    This function defines design volume, energy consumption and 
    capacity factor of existing water transfers between R12 nodes. 

    Parameters
    ----------


    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'hist_new_cap'.
        Values are data frames ready for :meth:`~.Scenario.add_par`.
        Years in the data include [2010,2015,2020,2030,2040,2050]
    """

    # presenttings for vintage and year_all
    year_all = [2010, 2015, 2020, 2030, 2040, 2050]
    first_year = 2020
    yv_ya_sw = map_yv_ya_lt(year_all, 70, first_year)

    # returns of the function
    result = {}

    input_df = pd.DataFrame()
    output_df = pd.DataFrame()
    cap_factor_df = pd.DataFrame()
    hist_new_cap_df = pd.DataFrame()
    for index, row in df_exist.iterrows():
        input_df = pd.concat(
            [input_df,
             make_df(
                 "input",
                 technology="wtrs_"+row['routes'],
                 value=1,
                 unit="km3",
                 level="water_avail_basin",
                 commodity="surfacewater_basin",
                 mode="M1",
                 time="year",
                 time_origin="year",
                 node_loc=row['node_in'],
                 node_origin=row['node_in']
             ).pipe(
                 broadcast, yv_ya_sw
             )]
        )
        input_df = pd.concat(
            [input_df,
             make_df(
                 "input",
                 technology="wtrs_"+row['routes'],
                 value=row['energy_con']/row['vol_yr'],
                 unit="GWh/km3",
                 level="final",
                 commodity="electr",
                 mode="M1",
                 time="year",
                 time_origin="year",
                 node_loc=row['node_in'],
                 node_origin=row['region']
             ).pipe(
                 broadcast, yv_ya_sw
             )]
        )

        output_df = pd.concat(
            [output_df,
             make_df(
                 "output",
                 technology="wtrs_"+row['routes'],
                 value=1,
                 unit="km3",
                 level="water_avail_basin",
                 commodity="surfacewater_basin",
                 mode="M1",
                 time="year",
                 time_dest="year",
                 node_loc=row['node_in'],
                 node_dest=row['node_out']
             ).pipe(
                 broadcast, yv_ya_sw
             )]
        )

        cap_factor_df = pd.concat(
            [cap_factor_df,
             make_df(
                 "capacity_factor",
                 node_loc=row['node_in'],
                 technology="wtrs_"+row['routes'],
                 time="year",
                 value=0.8  # according to (Sun,2021,Water Research)
             ).pipe(
                 broadcast, yv_ya_sw
             )]
        )

        hist_new_cap_df = pd.concat(
            [hist_new_cap_df,
             make_df(
                 "historical_new_capacity",
                 node_loc=row['node_in'],
                 technology="wtrs_"+row['routes'],
                 value=row['vol_yr'],
                 unit="km3/year",
                 year_vtg=2015,
             )]
        )

    result['input'] = input_df
    result['output'] = output_df
    result['capacity_factor'] = cap_factor_df
    result['historical_new_capacity'] = hist_new_cap_df

    return result


def inter_basin_water_transfer_plan() -> dict[str, pd.DataFrame]:
    result = {}

    return result
