import pandas as pd

from message_ix import (
    make_df,
    Scenario)
from message_ix_models.model.water.utils import map_yv_ya_lt
from message_ix_models.util import (
    package_data_path,
    broadcast
)


def ibwt_data_preprocess() -> pd.DataFrame:
    # read monthly and yearly data in one .csv file
    FILE = "IBWT.csv"
    PATH = package_data_path("geidco25", FILE)
    df = pd.read_csv(PATH, index_col=0)

    # presettings for node(e.g.B35|CHN) and region(e.g.R12_CHN)
    df['node_in'] = 'B'+df.basin_origin_id
    df['node_out'] = 'B'+df.basin_dest_id
    # routes for technology name
    # format: B159CHN_B35CHN_1
    # remove "|" from nodes
    df['routes'] = (df.node_in + '_' + df.node_out + '_' +
                    df.id.astype(str)).str.replace('|', '', regex=False)
    df['region'] = 'R12_'+df.MSG_reg

    # Remove route 9 (Ob->Gobi Interior)
    # Surfacewater in B105|CHN cannot sustain water transfer
    df = df[df['id'] != 9]

    return df


def inter_basin_water_transfer_exist(sc: Scenario) -> dict[str, pd.DataFrame]:
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

    df = ibwt_data_preprocess()
    # filter existing water transfer routes
    df_exist = df[df.status == "Existing"]
    df_exist_yr = df_exist[df_exist.time == "year"]

    # presenttings for vintage and year_all
    year_all = sc.set('year').tolist()
    # in the future (e.g. 2030), not including history
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
    bound_total_cap_lo_df = pd.DataFrame()
    bound_total_cap_up_df = pd.DataFrame()
    bound_act_lo_df = pd.DataFrame()
    bound_act_up_df = pd.DataFrame()
    for index, row in df_exist_yr.iterrows():
        input_df = pd.concat(
            [input_df,
             make_df(
                 "input",
                 technology="ibwt_e_"+row.routes,
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
                 technology="ibwt_e_"+row.routes,
                 value=row.energy_con_GWa_MCM,
                 unit="GWa/MCM",
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
                 technology="ibwt_e_"+row.routes,
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
                 technology="ibwt_e_"+row.routes,
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
                 technology="ibwt_e_"+row.routes,
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
                 technology="ibwt_e_"+row.routes,
                 value=row.vol_yr_MCM,
                 unit="MCM/year",
                 year_vtg=2025,
             )]
        )

        fix_cost_df = pd.concat(
            [fix_cost_df,
             make_df(
                 "fix_cost",
                 node_loc=row.node_in,
                 technology="ibwt_e_"+row.routes,
                 value=(row.fixed_cost_USD_MCM)/1e6,
                 unit="MUSD/MCM"
             ).pipe(
                 broadcast, yv_ya_sw
             )]
        )

        var_cost_df = pd.concat(
            [var_cost_df,
             make_df(
                 "var_cost",
                 node_loc=row.node_in,
                 technology="ibwt_e_"+row.routes,
                 value=(row.var_cost_USD_MCM)/1e6,
                 unit="MUSD/MCM",
                 mode="M1",
                 time="year"
             ).pipe(
                 broadcast, yv_ya_sw
             )]
        )

        # bound_total_cap_lo_df = pd.concat(
        #     [bound_total_cap_lo_df,
        #      make_df(
        #          "bound_total_capacity_lo",
        #          node_loc=row.node_in,
        #          technology="ibwt_e_"+row.routes,
        #          value=0.95*row.vol_yr_MCM,
        #          unit="MCM/year"
        #      ).pipe(
        #          broadcast, year_act=year_all
        #      )]
        # )
        # # Bound should start from after 2025
        # bound_total_cap_lo_df = bound_total_cap_lo_df[bound_total_cap_lo_df["year_act"] > 2025]

        bound_total_cap_up_df = pd.concat(
            [bound_total_cap_up_df,
             make_df(
                 "bound_total_capacity_up",
                 node_loc=row.node_in,
                 technology="ibwt_e_"+row.routes,
                 value=row.vol_yr_MCM,
                 unit="MCM/year"
             ).pipe(
                 broadcast, year_act=year_all
             )]
        )
        # Bound should start from after 2025
        bound_total_cap_up_df = bound_total_cap_up_df[bound_total_cap_up_df["year_act"] > 2025]

        bound_act_lo_df = pd.concat(
            [bound_act_lo_df,
             make_df(
                 "bound_activity_lo",
                 node_loc=row.node_in,
                 technology="ibwt_e_"+row.routes,
                 mode="M1",
                 time="year",
                 value=0.8*0.6*row.vol_yr_MCM,  # capcity factor: 0.8
                 unit="MCM/year"
             ).pipe(
                 broadcast, year_act=year_all
             )]
        )
        bound_act_lo_df = bound_act_lo_df[bound_act_lo_df["year_act"] >= 2025]

        bound_act_up_df = pd.concat(
            [bound_act_up_df,
             make_df(
                 "bound_activity_up",
                 node_loc=row.node_in,
                 technology="ibwt_e_"+row.routes,
                 mode="M1",
                 time="year",
                 value=0.8*row.vol_yr_MCM,  # capcity factor: 0.8
                 unit="MCM/year"
             ).pipe(
                 broadcast, year_act=year_all
             )]
        )
        bound_act_up_df = bound_act_up_df[bound_act_up_df["year_act"] >= 2025]

    result['input'] = input_df
    result['output'] = output_df
    result['capacity_factor'] = cap_factor_df
    result['technical_lifetime'] = tech_lifetime_df
    result['historical_new_capacity'] = hist_new_cap_df
    result['fix_cost'] = fix_cost_df
    result['var_cost'] = var_cost_df
    # result['bound_total_capacity_lo'] = bound_total_cap_lo_df
    result['bound_total_capacity_up'] = bound_total_cap_up_df
    result['bound_activity_lo'] = bound_act_lo_df
    result['bound_activity_up'] = bound_act_up_df

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

    df = ibwt_data_preprocess()
    # filter planned water transfer routes
    df_exist = df[df.status == "Planned"]
    df_exist_yr = df_exist[df_exist.time == "year"]

    # presenttings for vintage and year_all
    year_all = sc.set('year').tolist()
    # in the future (e.g. 2030), not including history
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
    bound_total_cap_up_df = pd.DataFrame()
    for index, row in df_exist_yr.iterrows():
        input_df = pd.concat(
            [input_df,
             make_df(
                 "input",
                 technology="ibwt_p_"+row.routes,
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
                 technology="ibwt_p_"+row.routes,
                 value=row.energy_con_GWa_MCM,
                 unit="GWa/MCM",
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
                 technology="ibwt_p_"+row.routes,
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
                 technology="ibwt_p_"+row.routes,
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
                 technology="ibwt_p_"+row.routes,
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
                 technology="ibwt_p_"+row.routes,
                 value=(row.inv_cost_USD_MCM)/1e6,
                 unit="MUSD/MCM"
             ).pipe(
                 broadcast, year_vtg=year_all
             )]
        )

        fix_cost_df = pd.concat(
            [fix_cost_df,
             make_df(
                 "fix_cost",
                 node_loc=row.node_in,
                 technology="ibwt_p_"+row.routes,
                 value=(row.fixed_cost_USD_MCM)/1e6,
                 unit="MUSD/MCM"
             ).pipe(
                 broadcast, yv_ya_sw
             )]
        )

        var_cost_df = pd.concat(
            [var_cost_df,
             make_df(
                 "var_cost",
                 node_loc=row.node_in,
                 technology="ibwt_p_"+row.routes,
                 value=(row.var_cost_USD_MCM)/1e6,
                 unit="MUSD/MCM",
                 mode="M1",
                 time="year"
             ).pipe(
                 broadcast, yv_ya_sw
             )]
        )

        bound_total_cap_up_df = pd.concat(
            [bound_total_cap_up_df,
             make_df(
                 "bound_total_capacity_up",
                 node_loc=row.node_in,
                 technology="ibwt_p_"+row.routes,
                 value=row.vol_yr_MCM,
                 unit="MCM/year"
             ).pipe(
                 broadcast, year_act=year_all
             )]
        )
        # Bound should start from 2030
        bound_total_cap_up_df = bound_total_cap_up_df[bound_total_cap_up_df["year_act"] > 2025]

    result['input'] = input_df
    result['output'] = output_df
    result['capacity_factor'] = cap_factor_df
    result['technical_lifetime'] = tech_lifetime_df
    result['inv_cost'] = inv_cost_df
    result['fix_cost'] = fix_cost_df
    result['var_cost'] = var_cost_df
    result['bound_total_capacity_up'] = bound_total_cap_up_df

    return result
