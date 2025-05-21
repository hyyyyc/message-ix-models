import numpy as np
import ixmp
import message_ix
from message_ix_models.project.geidco25.inter_basin_water_transfer import (
    inter_basin_water_transfer_exist,
    inter_basin_water_transfer_plan
)

# Connect to a db
mp = ixmp.Platform()

# Source scenario based on existing model in the db
model_sour = "clone_SSP_SSP5_v4.0"  # which one?
scen_sour = "baseline_clone_testing"
sour_scen = message_ix.Scenario(mp, model=model_sour, scenario=scen_sour)

# Target scenario
model_tar = "clone_SSP_SSP5_v4.0_test"
scen_tar = "baseline_clone_testing"
tar_scen = sour_scen.clone(model=model_tar, scenario=scen_tar,
                           keep_solution=False)
tar_scen.check_out()

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

tar_scen.commit(comment="define parameters for inter-basin water transfer")
tar_scen.set_as_default()
tar_scen.solve()

mp.close_db()

# # test
# input_test = data_exist['input']
# output_test = data_exist['output']
