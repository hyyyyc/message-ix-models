from message_ix_models import Context
from message_ix_models.model.water.build import main as build
from message_ix_models.model.water.cli import water_ini

# 1) Create a new Context and point it at your existing base‐scenario
ctx = Context()
model = "MixG_GEIDCO5_SSP2_v6.1"  # change to GEI model
base_scen = "Base_int_noIBWT"  # change to GEI scen
ctx.handle_cli_args(url=f"ixmp://ixmp_dev/{model}/{base_scen}")

# 2) "Pass" the same CLI options that `mix‐models … water-ix` would have done
ctx.ssp = "SSP2"  # scenario_param("--ssp", default="SSP2")
regions = "R12"  # common_params("regions")

# 3) Initialize all of the MESSAGE-water bits (this is what the group() does)
water_ini(ctx, regions=regions, time=None)

ctx.nexus_set = "nexus"
ctx.RCP = "7p0"
ctx.SDG = "baseline"  # Change to SDG
ctx.REL = "high"
# after water_ini…
sc_base = ctx.get_scenario()
sc_new = sc_base.clone(
    model=f"{model}",
    scenario="Base_RCP7_int_noIBWT",
    keep_solution=False,
)
build(ctx, sc_new)

sc_new.set_as_default()
sc_new.solve(
    solve_options={"lpmethod": "4", "scaind": "-1", "threads": "16"},
)
