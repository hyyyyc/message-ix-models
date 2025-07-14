import sys
import numpy as np
import ixmp
import message_ix
from message_ix_models.project.geidco25.inter_basin_water_transfer import (
    inter_basin_water_transfer_exist,
    inter_basin_water_transfer_plan
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


# Connect to a db
mp = ixmp.Platform(name="ixmp_dev", jvmargs=["-Xmx14G"])

# Source scenario based on existing model in the db
model_sour = "MESSAGE_GLOBIOM_SSP2_v6.1"
scen_sour = "baseline_nexus_7_high"
sour_scen = message_ix.Scenario(mp, model=model_sour, scenario=scen_sour)

# Target scenario
model_tar = "MESSAGE_GLOBIOM_SSP2_v6.1_ibwt_t3"
scen_tar = "baseline_nexus_7_high_ibwt_t3"
tar_scen = sour_scen.clone(model=model_tar, scenario=scen_tar,
                           keep_solution=False)

# Check water technology in the scenario
if "surfacewater_basin" in list(tar_scen.set("commodity")):
    pass
else:
    print("No water technology in the scenario.")
    sys.exit()

add_ibwt(tar_scen)

tar_scen.set_as_default()
tar_scen.solve(solve_options={"lpmethod": "4", "scaind": "-1"})

mp.close_db()
