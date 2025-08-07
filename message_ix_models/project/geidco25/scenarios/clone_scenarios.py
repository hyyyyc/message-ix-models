import ixmp
import message_ix

# Connect to a db
mp = ixmp.Platform(name="ixmp_dev", jvmargs=["-Xmx14G"])

# Source scenario based on existing model in the db
model_sour = "MESSAGE_GLOBIOM_SSP2_v6.1_ibwt_t3"
scen_sour = "baseline_nexus_7_high_ibwt_t3"
sour_scen = message_ix.Scenario(mp, model=model_sour, scenario=scen_sour)

# Target scenario
model_tar = "MESSAGE_GLOBIOM_SSP2_v6.1_ibwt_t4"
scen_tar = "baseline_nexus_7_high_ibwt_t4"
tar_scen = sour_scen.clone(model=model_tar, scenario=scen_tar,
                           keep_solution=False)

mp.close_db()
