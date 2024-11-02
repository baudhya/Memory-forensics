from gem5.components.boards.simple_board import SimpleBoard
from gem5.components.cachehierarchies.classic.no_cache import NoCache
from gem5.components.cachehierarchies.classic.private_l1_private_l2_cache_hierarchy import PrivateL1PrivateL2CacheHierarchy
from gem5.components.cachehierarchies.classic.private_l1_cache_hierarchy import PrivateL1CacheHierarchy
from gem5.components.memory.single_channel import SingleChannelDDR3_1600
from gem5.components.processors.simple_processor import SimpleProcessor
from gem5.components.processors.cpu_types import CPUTypes
from gem5.resources.resource import Resource, CustomResource
from gem5.simulate.simulator import Simulator
from gem5.isas import ISA


# Obtain the components.
# cache_hierarchy = NoCache()

cache_hierarchy = PrivateL1CacheHierarchy(
    "32KiB",
    "32KiB"
)

# cache_hierarchy = PrivateL1PrivateL2CacheHierarchy(
#     "32KiB",
#     "32KiB",
#     "256KiB"
# )


memory = SingleChannelDDR3_1600("8GiB")
processor = SimpleProcessor(cpu_type=CPUTypes.ATOMIC, num_cores=1, isa=ISA.X86)

#Add them to the board.
board = SimpleBoard(
    clk_freq="1GHz",
    processor=processor,
    memory=memory,
    cache_hierarchy=cache_hierarchy,
)

# Set the workload.
# binary = Resource("x86-hello64-static")
binary = CustomResource("main")
board.set_se_binary_workload(binary)

# Setup the Simulator and run the simulation.
simulator = Simulator(board=board)
simulator.run()