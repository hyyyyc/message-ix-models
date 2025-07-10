import sys
import numpy as np
import ixmp
import message_ix
from message_ix_models.project.geidco25.inter_basin_water_transfer import (
    inter_basin_water_transfer_exist,
    inter_basin_water_transfer_plan
)

# Connect to a db
mp = ixmp.Platform(name="ixmp_dev", jvmargs=["-Xmx14G"])

# Source scenario based on existing model in the db
model_sour = "clone_geidco_test"
scen_sour = "baseline_geidco_test_nexus_3_july"
sour_scen = message_ix.Scenario(mp, model=model_sour, scenario=scen_sour)

# Target scenario
model_tar = "clone_geidco_test_ibwt_t2"
scen_tar = "baseline_geidco_test_nexus_3_july_ibwt_t2"
tar_scen = sour_scen.clone(model=model_tar, scenario=scen_tar,
                           keep_solution=False)

# Check water technology in the scenario
if "surfacewater_basin" in list(tar_scen.set("commodity")):
    pass
else:
    print("No water technology in the scenario.")
    sys.exit()

# read data from IBWT functions
data_exist = inter_basin_water_transfer_exist(tar_scen)
data_plan = inter_basin_water_transfer_plan(tar_scen)
# IBWT technologies
tech_ibwt = np.append(data_exist['input']['technology'].unique(),
                      data_plan['input']['technology'].unique())
# pars for existing IBWT
par_exist = [key for key in data_exist]
# pars for planned IBWT
par_plan = [key for key in data_plan]

with tar_scen.transact("Add water transfer technology"):
    # add IBWT technologies
    for tech in tech_ibwt:
        tar_scen.add_set('technology', tech)

    for par in par_exist:
        tar_scen.add_par(par, data_exist[par])

    for par in par_plan:
        tar_scen.add_par(par, data_plan[par])

tar_scen.set_as_default()
tar_scen.solve()

mp.close_db()
