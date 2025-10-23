import sys
import numpy as np
import pandas as pd
import ixmp
import message_ix
from message_ix_models.util import (
    broadcast
)
from message_ix_models.project.geidco25.inter_basin_water_transfer import (
    inter_basin_water_transfer_exist,
    inter_basin_water_transfer_plan,
    ibwt_data_preprocess
)


def add_ibwt(scen: message_ix.Scenario) -> None:
    print("Add IBWT technologies")
    # read data from IBWT functions
    data_exist = inter_basin_water_transfer_exist(scen)
    data_plan = inter_basin_water_transfer_plan(scen)
    # IBWT technologies
    tech_ibwt = np.append(data_exist['input']['technology'].unique(),
                          data_plan['input']['technology'].unique())
    # pars for existing IBWT
    par_exist = [key for key in data_exist]
    # pars for planned IBWT
    par_plan = [key for key in data_plan]
    with scen.transact("Add water transfer technology"):
        for tech in tech_ibwt:
            scen.add_set('technology', tech)

        for par in par_exist:
            scen.add_par(par, data_exist[par])

        for par in par_plan:
            scen.add_par(par, data_plan[par])


def change_ibwt_name(scen: message_ix.Scenario) -> None:
    print("Rename ibwt technology")
    # parameters including ibwt technology
    # read data from IBWT functions
    data_exist = inter_basin_water_transfer_exist(scen)
    data_plan = inter_basin_water_transfer_plan(scen)
    # pars for existing IBWT
    par_exist = [key for key in data_exist]
    # pars for planned IBWT
    par_plan = [key for key in data_plan]
    # merge ibwt parameters
    tech_parameters = list(set(par_exist + par_plan))

    # old tech name under this scenario
    all_tech = scen.set("technology")
    old_name = all_tech[all_tech.str.startswith('ibwt')].tolist()
    # new tech name
    new_name = [s.replace('|', '') for s in old_name]

    with scen.transact("Rename ibwt technology"):
        scen.remove_set("technology", old_name)
        scen.add_set("technology", new_name)

        for param in tech_parameters:
            try:
                # Get data for old technology
                old_data = scen.par(param, filters={"technology": old_name})

                if not old_data.empty:
                    # Create new data with updated technology name
                    new_data = old_data.copy()
                    new_data["technology"] = new_data["technology"].str.replace(
                        '|', '', regex=False)

                    # Remove old data
                    scen.remove_par(param, old_data)
                    # Add new data
                    # Warning: Pars are not added, don't know why
                    scen.add_par(param, new_data)

            except Exception as e:
                # skip
                print("Parameter doesn't exist or no data")
                continue


def add_act_bound_exsiting(scen: message_ix.Scenario) -> None:
    print("Remove lower capacity boundary for existing IBWT")
    # Read data from IBWT functions
    data_exist = inter_basin_water_transfer_exist(scen)
    # IBWT technologies
    tech = data_exist['input']['technology'].unique()
    # Get lower boundary for existing IBWT
    cap_lo = scen.par("bound_total_capacity_lo", filters={"technology": tech})
    # Remove lower capacity boundary
    with scen.transact("Remove lower capacity boundary for existing IBWT"):
        scen.remove_par("bound_total_capacity_lo", cap_lo)

    print("Add activity boundary for existing IBWT")
    df = ibwt_data_preprocess()
    # Filter existing water transfer routes
    df_exist = df[df.status == "Existing"]
    df_exist_yr = df_exist[df_exist.time == "year"]
    # Presenttings for vintage and year_all
    year_all = scen.set('year').tolist()

    bound_act_lo_df = pd.DataFrame()
    bound_act_up_df = pd.DataFrame()
    for index, row in df_exist_yr.iterrows():
        bound_act_lo_df = pd.concat(
            [bound_act_lo_df,
             message_ix.make_df(
                 "bound_activity_lo",
                 node_loc=row.node_in,
                 technology="ibwt_e_"+row.routes,
                 mode="M1",
                 time="year",
                 value=0.6*row.vol_yr_MCM,
                 unit="MCM/year"
             ).pipe(
                 broadcast, year_act=year_all
             )]
        )
        bound_act_lo_df = bound_act_lo_df[bound_act_lo_df["year_act"] >= 2025]

        bound_act_up_df = pd.concat(
            [bound_act_up_df,
             message_ix.make_df(
                 "bound_activity_up",
                 node_loc=row.node_in,
                 technology="ibwt_e_"+row.routes,
                 mode="M1",
                 time="year",
                 value=0.95*row.vol_yr_MCM,
                 unit="MCM/year"
             ).pipe(
                 broadcast, year_act=year_all
             )]
        )
        bound_act_up_df = bound_act_up_df[bound_act_up_df["year_act"] >= 2025]

    # Add activity boundary
    with scen.transact("Add activity boundary for existing IBWT"):
        scen.add_par("bound_activity_lo", bound_act_lo_df)
        scen.add_par("bound_activity_up", bound_act_up_df)


def sa_oth_water_demand(scen: message_ix.Scenario) -> None:
    '''
    Sensitivity Analysis: change water demand other than irrigation
    including industry, municipal(urban, rural)
    Warning: municipal water demand↑ may increase xxx investment cost
    Warning: need to calculate a certain value instead of a ratio
    to control total water demand (irrigation + others) at the basin-level
    '''
    water_receive_node = ["35|CHN", "162|CHN", "62|CHN",
                          "148|CHN", "96|AFR", "96|MEA", "97|NAM", "125|LAM"]
    old_wat_de = scen.par("demand", filters={"level": ["final"],
                                             "commodity": ["urban_mw", "urban_disconnected",
                                                           "rural_mw", "rural_disconnected",
                                                           "industry_mw"],
                                             "node": ['B'+x for x in water_receive_node]})
    with scen.transact("Remove old other water demand"):
        scen.remove_par("demand", old_wat_de)

    new_wat_de = old_wat_de.copy()
    new_wat_de["value"] = old_wat_de["value"] * 1.5
    with scen.transact("Add new other water demand"):
        scen.add_par("demand", new_wat_de)


def cal_all_water_demand(scen: message_ix.Scenario) -> pd.DataFrame:
    '''
    irrigation & industry & municipal
    '''
    water_receive_node = ["35|CHN", "162|CHN", "62|CHN",
                          "148|CHN", "96|AFR", "96|MEA", "97|NAM", "125|LAM"]
    # Calculate all water demand
    # irrigation water demand
    irr_wat = scen.var("ACT", filters={"technology": ["basin_to_reg_plus"],
                                       "mode": ['M'+x for x in water_receive_node]})
    # other water demand
    oth_wat = scen.par("demand", filters={"level": ["final"],
                                          "commodity": ["urban_mw", "urban_disconnected",
                                                        "rural_mw", "rural_disconnected",
                                                        "industry_mw"],
                                          "node": ['B'+x for x in water_receive_node]})
    irr_wat['node'] = irr_wat['mode'].str.replace('^M', 'B', regex=True)
    irr_wat['commodity'] = 'irrigation'
    irr_wat['level'] = 'final'
    irr_wat['year'] = irr_wat['year_act']
    irr_wat['value'] = irr_wat['lvl']
    irr_wat['unit'] = 'MCM/year'

    irr_wat_re = irr_wat[['node', 'commodity',
                          'level', 'year', 'time', 'value', 'unit']]
    # all water demand
    water_demand = pd.concat([irr_wat_re, oth_wat])

    return water_demand


def sa_add_ind_water_demand(scen: message_ix.Scenario, ratio: float) -> None:
    '''
    Sensitivity Analysis: add only industrial water demand
    to make the variation in water demand less relevant to other sensitive variables
    (such as investment)
    Warning: need to calculate a certain value instead of a ratio
    to control total water demand (irrigation + others) at the basin-level
    Warning: don't use this function for minus industrial water demand
    '''
    if ratio <= 1:
        raise ValueError("The parameter 'ratio' must be greater than 1.")

    water_receive_node = ["35|CHN", "162|CHN", "62|CHN",
                          "148|CHN", "96|AFR", "96|MEA", "97|NAM", "125|LAM"]

    # all water demand
    water_demand = cal_all_water_demand(scen)
    # sum up
    water_demand_sta = water_demand.groupby(
        ['node', 'year'], as_index=False)['value'].sum()
    water_demand_sta['value_new'] = water_demand_sta['value']*ratio
    water_demand_sta['value_change'] = water_demand_sta['value_new'] - \
        water_demand_sta['value']

    # Change industrial water demand
    old_ind_wat = scen.par("demand", filters={"level": ["final"],
                                              "commodity": ["industry_mw"],
                                              "node": ['B'+x for x in water_receive_node]})
    with scen.transact("Remove old industrial water demand"):
        scen.remove_par("demand", old_ind_wat)

    new_ind_wat_sta = pd.merge(old_ind_wat, water_demand_sta,
                               how="inner", on=['node', 'year'])
    new_ind_wat_sta["value"] = new_ind_wat_sta["value_x"] + \
        new_ind_wat_sta['value_change']
    new_ind_wat = new_ind_wat_sta[[
        'node', 'commodity', 'level', 'year', 'time', 'value', 'unit']]
    with scen.transact("Add new industrial water demand"):
        scen.add_par("demand", new_ind_wat)


def cascade_deduct_water_demand(row,
                                # deduct water demand order
                                order=('industry_mw', 'rural_mw', 'urban_mw'),
                                delta_col='delta',
                                suffix=''):
    # check delta
    remaining = row.get(delta_col, 0)
    if pd.isna(remaining) or remaining < 0:
        remaining = 0.0
    else:
        remaining = float(remaining)

    result = {}

    for col in order:
        # old value for col
        val = row.get(col, 0)
        if pd.isna(val):
            val = 0.0
        else:
            val = float(val)

        # deduct value for col (>=0)
        take = min(val, remaining)
        new_val = val - take                     # >= 0
        result[col + suffix] = new_val         # new value after deducting
        remaining -= take                        # remaining delta

    # remaining delta after deducting from industry, urban and rural water demand
    result['delta_left'] = remaining

    return pd.Series(result)


def sa_rem_oth_water_demand(scen: message_ix.Scenario, ratio: float) -> None:
    if ratio >= 1:
        raise ValueError("The parameter 'ratio' must be lower than 1.")

    water_receive_node = ["35|CHN", "162|CHN", "62|CHN",
                          "148|CHN", "96|AFR", "96|MEA", "97|NAM", "125|LAM"]

    # all water demand
    water_demand = cal_all_water_demand(scen)
    # sum up
    base_id_cols = ['node', 'level', 'year', 'time', 'unit']
    water_demand_sta = water_demand.groupby(
        base_id_cols, as_index=False)['value'].sum()
    water_demand_sta['value_new'] = water_demand_sta['value']*ratio
    water_demand_sta['value_change'] = water_demand_sta['value'] - \
        water_demand_sta['value_new']  # >=

    # compare value_change and industrial/municipal water demand
    water_demand_sta['commodity'] = 'delta'
    water_demand_sta_re = water_demand_sta[['node',
                                           'commodity', 'level', 'year', 'time', 'value_change', 'unit']].rename(columns={'value_change': 'value'})

    df_com = pd.concat([water_demand, water_demand_sta_re])
    df_com_wide = df_com.pivot(index=base_id_cols,
                               columns='commodity',
                               values='value').reset_index()

    # cascade deduct from industry, urban, rural water demand
    df_deduct = pd.concat(
        [df_com_wide[base_id_cols].reset_index(drop=True),
         df_com_wide.apply(cascade_deduct_water_demand, axis=1).reset_index(drop=True)],
        axis=1
    )

    # Change other water demand
    old_other_wat = scen.par("demand", filters={"level": ["final"],
                                                "commodity": ["industry_mw",
                                                              'urban_mw',
                                                              'rural_mw'],
                                                "node": ['B'+x for x in water_receive_node]})
    with scen.transact("Remove old other water demand"):
        scen.remove_par("demand", old_other_wat)

    # add new other water demand
    demand_id_cols = ['node', 'commodity',
                      'level', 'year', 'time', 'value', 'unit']
    new_ind_wat = df_deduct.melt(
        id_vars=base_id_cols,
        value_vars='industry_mw',
        var_name='commodity',
        value_name='value'
    )
    new_ind_wat = new_ind_wat[demand_id_cols]

    new_urban_wat = df_deduct.melt(
        id_vars=base_id_cols,
        value_vars='urban_mw',
        var_name='commodity',
        value_name='value'
    )
    new_urban_wat = new_urban_wat[demand_id_cols]

    new_rural_wat = df_deduct.melt(
        id_vars=base_id_cols,
        value_vars='rural_mw',
        var_name='commodity',
        value_name='value'
    )
    new_rural_wat = new_rural_wat[demand_id_cols]

    with scen.transact("Add new other water demand"):
        scen.add_par("demand", new_ind_wat)
        scen.add_par("demand", new_urban_wat)
        scen.add_par("demand", new_rural_wat)


def sa_rem_irr_water_demand(scen: message_ix.Scenario, ratio: float) -> None:
    '''
    Sensitivity Analysis: subtract irrigation water demand
    Warning: irrigation water demand is at the region-level
    So it's subtracting water demand for both water-supplying and water-receiving basins
    '''
    if ratio >= 1:
        raise ValueError("The parameter 'ratio' must be lower than 1.")

    old_irr_de = scen.par("land_input", filters={"commodity": "freshwater"})
    with scen.transact("Remove old irrigation demand"):
        scen.remove_par("land_input", old_irr_de)

    new_irr_de = old_irr_de.copy()
    new_irr_de["value"] = old_irr_de["value"] * ratio
    with scen.transact("Add new irrigation demand"):
        scen.add_par("land_input", new_irr_de)


def sa_ibwt_cost(scen: message_ix.Scenario, ratio: float) -> None:
    '''
    Sensitivity Analysis: 
    change investment, fixed and variable costs for IBWT technologies
    '''
    ibwt_techs = [x for x in scen.set("technology") if x.startswith('ibwt')]
    old_inv = scen.par("inv_cost", filters={"technology": ibwt_techs})
    old_fix = scen.par("fix_cost", filters={"technology": ibwt_techs})
    old_var = scen.par("var_cost", filters={"technology": ibwt_techs})

    # Remove old values
    with scen.transact("Remove old IBWT cost"):
        scen.remove_par("inv_cost", old_inv)
        scen.remove_par("fix_cost", old_fix)
        scen.remove_par("var_cost", old_var)

    new_inv = old_inv.copy()
    new_fix = old_fix.copy()
    new_var = old_var.copy()
    new_inv["value"] = old_inv["value"] * ratio
    new_fix["value"] = old_fix["value"] * ratio
    new_var["value"] = old_var["value"] * ratio

    # Add new values
    with scen.transact("Add new IBWT cost"):
        scen.add_par("inv_cost", new_inv)
        scen.add_par("fix_cost", new_fix)
        scen.add_par("var_cost", new_var)


def sa_power_cost(scen: message_ix.Scenario, ratio: float) -> None:
    '''
    Sensitivity Analysis: 
    change investment, fixed and variable costs for power generation technologies
    including GEI relevant technologies
    '''
    # GEI relevant techs
    gei_techs = [x for x in scen.set("technology") if "gei" in x or "uhv" in x]
    # All power techs
    power_tec = [
        "coal_ppl",
        "ucoal_ppl",
        "coal_adv",
        "coal_adv_ccs",
        "igcc",
        "igcc_ccs",
        "foil_ppl",
        "loil_ppl",
        "loil_cc",
        "gas_ppl",
        "gas_ct",
        "gas_cc",
        "gas_cc_ccs",
        "bio_ppl",
        "bio_istig",
        "bio_istig_ccs",
        "geo_ppl",
        "solar_res1",
        "solar_res2",
        "solar_res3",
        "solar_res4",
        "solar_res5",
        "solar_res6",
        "solar_res7",
        "solar_res8",
        "solar_res_RT1",
        "solar_res_RT2",
        "solar_res_RT3",
        "solar_res_RT4",
        "solar_res_RT5",
        "solar_res_RT6",
        "solar_res_RT7",
        "solar_res_RT8",
        "csp_sm1_res1",
        "csp_sm1_res2",
        "csp_sm1_res3",
        "csp_sm1_res4",
        "csp_sm1_res5",
        "csp_sm1_res6",
        "csp_sm1_res7",
        "wind_res1",
        "wind_res2",
        "wind_res3",
        "wind_res4",
        "wind_ref1",
        "wind_ref2",
        "wind_ref3",
        "wind_ref4",
        "wind_ref5",
        "nuc_lc",
        "nuc_hc",
        "nuc_fbr",
    ] + gei_techs

    old_inv = scen.par("inv_cost", filters={"technology": power_tec})
    old_fix = scen.par("fix_cost", filters={"technology": power_tec})
    old_var = scen.par("var_cost", filters={"technology": power_tec})

    # Remove old values
    with scen.transact("Remove old power generation cost"):
        scen.remove_par("inv_cost", old_inv)
        scen.remove_par("fix_cost", old_fix)
        scen.remove_par("var_cost", old_var)

    new_inv = old_inv.copy()
    new_fix = old_fix.copy()
    new_var = old_var.copy()
    new_inv["value"] = old_inv["value"] * ratio
    new_fix["value"] = old_fix["value"] * ratio
    new_var["value"] = old_var["value"] * ratio

    # Add new values
    with scen.transact("Add new power generation cost"):
        scen.add_par("inv_cost", new_inv)
        scen.add_par("fix_cost", new_fix)
        scen.add_par("var_cost", new_var)


# Connect to a db
mp = ixmp.Platform(name="ixmp_dev", jvmargs=["-Xmx14G"])

# Source scenario based on existing model in the db
model_sour = "MixG_GEIDCO5_SSP2_v6.1"
scen_sour = "Base_RCP7_noint_IBWT_t4"
sour_scen = message_ix.Scenario(mp, model=model_sour, scenario=scen_sour)


# Target scenario
model_tar = "MixG_GEIDCO5_SSP2_v6.1"
scen_tar = "Reduced_SA_addDemand5_Base_RCP7_noint_IBWT_t2"
tar_scen = sour_scen.clone(model=model_tar, scenario=scen_tar,
                           keep_solution=False)

# Check water technology in the scenario
if "surfacewater_basin" in list(tar_scen.set("commodity")):
    pass
else:
    print("No water technology in the scenario.")
    sys.exit()

# add_ibwt(tar_scen)
# Sensitivity Analysis
sa_ibwt_cost(tar_scen, ratio=1.5)

tar_scen.set_as_default()
tar_scen.solve(solve_options={"lpmethod": "4", "scaind": "-1"})

mp.close_db()
