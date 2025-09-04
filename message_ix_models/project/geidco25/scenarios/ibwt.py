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


def sa_irr_water_demand(scen: message_ix.Scenario) -> None:
    '''
    Sensitivity Analysis: change irrigation water demand
    '''
    old_irr_de = scen.par("land_input", filters={"commodity": "freshwater"})
    with scen.transact("Remove old irrigation demand"):
        scen.remove_par("land_input", old_irr_de)

    new_irr_de = old_irr_de.copy()
    new_irr_de["value"] = old_irr_de["value"] * 1.25
    with scen.transact("Add new irrigation demand"):
        scen.add_par("land_input", new_irr_de)


# Connect to a db
mp = ixmp.Platform(name="ixmp_dev", jvmargs=["-Xmx14G"])

# Source scenario based on existing model in the db
model_sour = "MESSAGE_GLOBIOM_SSP2_v6.1"
scen_sour = "Reduced_Base_RCP7_noint_noIBWT_t2"
sour_scen = message_ix.Scenario(mp, model=model_sour, scenario=scen_sour)

# Target scenario
model_tar = "MESSAGE_GLOBIOM_SSP2_v6.1"
scen_tar = "Reduced_SA_addIrrDemand25_Base_RCP7_noint_IBWT_t2"
tar_scen = sour_scen.clone(model=model_tar, scenario=scen_tar,
                           keep_solution=False)

# Check water technology in the scenario
if "surfacewater_basin" in list(tar_scen.set("commodity")):
    pass
else:
    print("No water technology in the scenario.")
    sys.exit()

add_ibwt(tar_scen)
# Sensitivity Analysis
# sa_irr_water_demand(tar_scen)

tar_scen.set_as_default()
tar_scen.solve(solve_options={"lpmethod": "4", "scaind": "-1"})

mp.close_db()
