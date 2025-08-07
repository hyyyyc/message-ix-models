import message_ix

from message_ix_models import Context
from message_ix_models.model.water.build import main as build
from message_ix_models.model.water.cli import water_ini

# 1) Create a new Context and point it at your existing base‐scenario
ctx = Context()
ctx.handle_cli_args(url="ixmp://ixmp_dev/MESSAGE_GLOBIOM_SSP2_v6.1/baseline")

# 2) "Pass" the same CLI options that `mix‐models … water-ix` would have done
ctx.ssp = "SSP2"  # scenario_param("--ssp", default="SSP2")
regions = "R12"  # common_params("regions")

# 3) Initialize all of the MESSAGE-water bits (this is what the group() does)
water_ini(ctx, regions=regions, time=None)

ctx.nexus_set = "nexus"
ctx.RCP = "7p0"
ctx.SDG = "SDG"
ctx.REL = "high"
# baseline_geidco_test_nexus_desal_31_june
# after water_ini…
sc_base = ctx.get_scenario()

sc_new = sc_base.clone(
    model="MixG_GEIDCO5_SSP2_v6.1",
    scenario="SDG_RCP7_noint_noIBWT",
    keep_solution=False,
)
build(ctx, sc_new)

sc_new.set_as_default()
sc_new.solve(
    solve_options={"lpmethod": "4", "scaind": "-1", "threads": "16"},
)
