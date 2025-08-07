import ixmp
import message_ix

'''
This script is to rename scenario names
Specifically:
MixG_GEIDCO5_SSP2_v6.1 EN1000f_RCP26_int_noIBWT
MixG_GEIDCO5_SSP2_v6.1 Base_RCP7_int_noIBWT
(These two scenarios didn't include nexus but occupy the designed name)
renaming to:
MixG_GEIDCO5_SSP2_v6.1 EN1000f_int_noIBWT
MixG_GEIDCO5_SSP2_v6.1 Base_int_noIBWT

model_name = "MESSAGE_GLOBIOM_SSP2_v6.1"
base_scenario = "baseline_G_nexus_7p0_high"
renaming to:
model_name = "MixG_GEIDCO5_SSP2_v6.1"
base_scenario = "Base_RCP7_noint_noIBWT"
'''

# Connect to a db
mp = ixmp.Platform(name="ixmp_dev", jvmargs=["-Xmx14G"])

# Source scenario based on existing model in the db
model_sour = "MESSAGE_GLOBIOM_SSP2_v6.1"
scen_sour = "baseline_G_nexus_7p0_high"
sour_scen = message_ix.Scenario(mp, model=model_sour, scenario=scen_sour)

# Target scenario
model_tar = "MixG_GEIDCO5_SSP2_v6.1"
scen_tar = "Base_RCP7_noint_noIBWT"
tar_scen = sour_scen.clone(model=model_tar, scenario=scen_tar,
                           keep_solution=False)

mp.close_db()
