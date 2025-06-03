import pandas as pd

from message_ix import make_df
from message_ix_models.model.water.utils import map_yv_ya_lt
from message_ix_models.util import (
    package_data_path,
    broadcast
)

# # convert basin name to BCU name(R12)
# basin2BCU = {
#     "Huang He": "62|CHN",
#     "Yangtze": "159|CHN",
#     "Ziya He Interior": "162|CHN",
#     "China Coast": "35|CHN",
#     "Ob": "105|CHN",
#     "Gobi Interior": "54|CHN",
#     "Ganges Bramaputra": "53|CHN"
# }
# basin2BCU = {key: 'B'+value for key, value in basin2BCU.items()}

# read monthly and yearly data in one .csv file
FILE = "IBWT.csv"
PATH = package_data_path("geidco25", FILE)
df = pd.read_csv(PATH, index_col=0)

# presettings for node(e.g.B35|CHN) and region(e.g.R12_CHN)
df['node_in'] = 'B'+df.basin_origin_id
df['node_out'] = 'B'+df.basin_dest_id
# routes for technology name
# format: B159|CHN_B35|CHN_1
df['routes'] = df.node_in + '_' + df.node_out + '_' + df.id.astype(str)
df['region'] = 'R12_'+df.MSG_reg


def inter_basin_water_transfer_exist(sc) -> dict[str, pd.DataFrame]:
    """Add existing inter basin water transfers (IBWT)
    This function defines design volume (historical new capacity), 
    energy consumption (input), capacity factor, technical lifetime, 
    fixed cost and variable cost of existing water transfers between R12 nodes. 

    Parameters
    ----------
    sc : scenario

    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'hist_new_cap'.
        Values are data frames ready for :meth:`~.Scenario.add_par`.
    """

    # filter existing water transfer routes
    df_exist = df[df.status == "Existing"]
    df_exist_yr = df_exist[df_exist.time == "year"]

    # presenttings for vintage and year_all
    year_all = sc.set('year').tolist()
    first_year = sc.firstmodelyear
    # retrieve historic time-steps
    history = [y for y in year_all if y < first_year]
    yv_ya_sw = map_yv_ya_lt(year_all, 70, first_year)

    # returns of the function
    result = {}

    input_df = pd.DataFrame()
    output_df = pd.DataFrame()
    cap_factor_df = pd.DataFrame()
    tech_lifetime_df = pd.DataFrame()
    hist_new_cap_df = pd.DataFrame()
    var_cost_df = pd.DataFrame()
    fix_cost_df = pd.DataFrame()
    for index, row in df_exist_yr.iterrows():
        input_df = pd.concat(
            [input_df,
             make_df(
                 "input",
                 technology="wtrs_"+row.routes,
                 value=1,
                 unit="MCM",
                 level="water_avail_basin",
                 commodity="surfacewater_basin",
                 mode="M1",
                 time="year",
                 time_origin="year",
                 node_loc=row.node_in,
                 node_origin=row.node_in
             ).pipe(
                 broadcast, yv_ya_sw
             )]
        )
        input_df = pd.concat(
            [input_df,
             make_df(
                 "input",
                 technology="wtrs_"+row.routes,
                 value=row.energy_con_MWh_MCM,
                 unit="MWh/MCM",
                 level="final",
                 commodity="electr",
                 mode="M1",
                 time="year",
                 time_origin="year",
                 node_loc=row.node_in,
                 node_origin=row.region
             ).pipe(
                 broadcast, yv_ya_sw
             )]
        )

        output_df = pd.concat(
            [output_df,
             make_df(
                 "output",
                 technology="wtrs_"+row.routes,
                 value=1,
                 unit="MCM",
                 level="water_avail_basin",
                 commodity="surfacewater_basin",
                 mode="M1",
                 time="year",
                 time_dest="year",
                 node_loc=row.node_in,
                 node_dest=row.node_out
             ).pipe(
                 broadcast, yv_ya_sw
             )]
        )

        cap_factor_df = pd.concat(
            [cap_factor_df,
             make_df(
                 "capacity_factor",
                 node_loc=row.node_in,
                 technology="wtrs_"+row.routes,
                 time="year",
                 value=0.8,  # according to (Sun,2021,Water Research)
                 unit="%"
             ).pipe(
                 broadcast, yv_ya_sw
             )]
        )

        tech_lifetime_df = pd.concat(
            [tech_lifetime_df,
             make_df(
                 "technical_lifetime",
                 node_loc=row.node_in,
                 technology="wtrs_"+row.routes,
                 value=70,
                 unit="y"
             ).pipe(
                 broadcast, year_vtg=year_all
             )]
        )

        hist_new_cap_df = pd.concat(
            [hist_new_cap_df,
             make_df(
                 "historical_new_capacity",
                 node_loc=row.node_in,
                 technology="wtrs_"+row.routes,
                 value=row.vol_yr_MCM,
                 unit="MCM/year",
                 year_vtg=2015,
             )]
        )

        fix_cost_df = pd.concat(
            [fix_cost_df,
             make_df(
                 "fix_cost",
                 node_loc=row.node_in,
                 technology="wtrs_"+row.routes,
                 value=row.fixed_cost_USD_MCM,
                 unit="USD/MCM"
             ).pipe(
                 broadcast, yv_ya_sw
             )]
        )

        var_cost_df = pd.concat(
            [var_cost_df,
             make_df(
                 "var_cost",
                 node_loc=row.node_in,
                 technology="wtrs_"+row.routes,
                 value=row.var_cost_USD_MCM,
                 unit="USD/MCM",
                 mode="M1",
                 time="year"
             ).pipe(
                 broadcast, yv_ya_sw
             )]
        )

    result['input'] = input_df
    result['output'] = output_df
    result['capacity_factor'] = cap_factor_df
    result['technical_lifetime'] = tech_lifetime_df
    result['historical_new_capacity'] = hist_new_cap_df
    result['fix_cost'] = fix_cost_df
    result['var_cost'] = var_cost_df

    return result


def inter_basin_water_transfer_plan(sc) -> dict[str, pd.DataFrame]:
    """Add planned inter basin water transfers (IBWT)
    This function defines design volume, energy consumption, capacity factor, 
    technical lifetime, investment cost, fixed cost and variable cost of 
    existing water transfers between R12 nodes. 

    Parameters
    ----------
    sc : scenario

    Returns
    -------
    data : dict of (str -> pandas.DataFrame)
        Keys are MESSAGE parameter names such as 'input', 'hist_new_cap'.
        Values are data frames ready for :meth:`~.Scenario.add_par`.
    """

    # filter existing water transfer routes
    df_exist = df[df.status == "Planned"]
    df_exist_yr = df_exist[df_exist.time == "year"]

    # presenttings for vintage and year_all
    year_all = sc.set('year').tolist()
    first_year = sc.firstmodelyear
    # retrieve historic time-steps
    history = [y for y in year_all if y < first_year]
    yv_ya_sw = map_yv_ya_lt(year_all, 70, first_year)

    # returns of the function
    result = {}

    input_df = pd.DataFrame()
    output_df = pd.DataFrame()
    cap_factor_df = pd.DataFrame()
    tech_lifetime_df = pd.DataFrame()
    inv_cost_df = pd.DataFrame()
    var_cost_df = pd.DataFrame()
    fix_cost_df = pd.DataFrame()
    bound_total_cap_df = pd.DataFrame()
    for index, row in df_exist_yr.iterrows():
        input_df = pd.concat(
            [input_df,
             make_df(
                 "input",
                 technology="wtrs_"+row.routes,
                 value=1,
                 unit="MCM",
                 level="water_avail_basin",
                 commodity="surfacewater_basin",
                 mode="M1",
                 time="year",
                 time_origin="year",
                 node_loc=row.node_in,
                 node_origin=row.node_in
             ).pipe(
                 broadcast, yv_ya_sw
             )]
        )
        input_df = pd.concat(
            [input_df,
             make_df(
                 "input",
                 technology="wtrs_"+row.routes,
                 value=row.energy_con_MWh_MCM,
                 unit="MWh/MCM",
                 level="final",
                 commodity="electr",
                 mode="M1",
                 time="year",
                 time_origin="year",
                 node_loc=row.node_in,
                 node_origin=row.region
             ).pipe(
                 broadcast, yv_ya_sw
             )]
        )

        output_df = pd.concat(
            [output_df,
             make_df(
                 "output",
                 technology="wtrs_"+row.routes,
                 value=1,
                 unit="MCM",
                 level="water_avail_basin",
                 commodity="surfacewater_basin",
                 mode="M1",
                 time="year",
                 time_dest="year",
                 node_loc=row.node_in,
                 node_dest=row.node_out
             ).pipe(
                 broadcast, yv_ya_sw
             )]
        )

        cap_factor_df = pd.concat(
            [cap_factor_df,
             make_df(
                 "capacity_factor",
                 node_loc=row.node_in,
                 technology="wtrs_"+row.routes,
                 time="year",
                 value=0.8,  # according to (Sun,2021,Water Research)
                 unit="%"
             ).pipe(
                 broadcast, yv_ya_sw
             )]
        )

        tech_lifetime_df = pd.concat(
            [tech_lifetime_df,
             make_df(
                 "technical_lifetime",
                 node_loc=row.node_in,
                 technology="wtrs_"+row.routes,
                 value=70,
                 unit="y"
             ).pipe(
                 broadcast, year_vtg=year_all
             )]
        )

        inv_cost_df = pd.concat(
            [inv_cost_df,
             make_df(
                 "inv_cost",
                 node_loc=row.node_in,
                 technology="wtrs_"+row.routes,
                 value=row.inv_cost_USD_MCM,
                 unit="USD/MCM"
             ).pipe(
                 broadcast, year_vtg=year_all
             )]
        )

        fix_cost_df = pd.concat(
            [fix_cost_df,
             make_df(
                 "fix_cost",
                 node_loc=row.node_in,
                 technology="wtrs_"+row.routes,
                 value=row.fixed_cost_USD_MCM,
                 unit="USD/MCM"
             ).pipe(
                 broadcast, yv_ya_sw
             )]
        )

        var_cost_df = pd.concat(
            [var_cost_df,
             make_df(
                 "var_cost",
                 node_loc=row.node_in,
                 technology="wtrs_"+row.routes,
                 value=row.var_cost_USD_MCM,
                 unit="USD/MCM",
                 mode="M1",
                 time="year"
             ).pipe(
                 broadcast, yv_ya_sw
             )]
        )

        bound_total_cap_df = pd.concat(
            [bound_total_cap_df,
             make_df(
                 "bound_total_capacity_up",
                 node_loc=row.node_in,
                 technology="wtrs_"+row.routes,
                 value=row.vol_yr_MCM,
                 unit="MCM/year"
             ).pipe(
                 broadcast, year_act=year_all
             )]
        )
        # Bound should start from 2030
        bound_total_cap_df = bound_total_cap_df[bound_total_cap_df["year_act"] > 2025]

    result['input'] = input_df
    result['output'] = output_df
    result['capacity_factor'] = cap_factor_df
    result['technical_lifetime'] = tech_lifetime_df
    result['inv_cost'] = inv_cost_df
    result['fix_cost'] = fix_cost_df
    result['var_cost'] = var_cost_df
    result['bound_total_capacity_up'] = bound_total_cap_df

    return result
