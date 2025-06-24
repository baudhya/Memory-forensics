from migen import *
from migen.sim import *

from litesdcard.phy import SDPHY
# from litesdcard.core import SDCore
from sdcore import SDCore
from litesdcard.common import *
from litesdcard.frontend.dma import SDMem2BlockDMA

from litex.build.generic_platform import *
from litex_boards.platforms import radiona_ulx3s

class SDController(Module):
    def __init__(self, platform):
        leds = [platform.request("user_led", i) for i in range(8)]
        pads = platform.request("sdcard")
        clk_freq = int(50e6)

        self.sdphy = SDPHY(pads=pads, device=platform.device, sys_clk_freq=clk_freq)
        self.submodules += self.sdphy
        self.sdcore = SDCore(phy=self.sdphy, leds=leds)
        self.submodules += self.sdcore

        mem = Memory(32, 128, init=[0x00000000]*128)
        self.specials += mem

        mem_port = mem.get_port()
        self.specials += mem_port

        fsm = FSM(reset_state="INIT")
        self.submodules += fsm

        counter = Signal(32, reset=0)
        event = Signal(4)

        fsm.act("INIT",
            self.sdphy.clocker.divider.storage.eq(16),
            If(counter < int(clk_freq/1000),
                NextValue(counter, counter + 1),
            ).Else(
                NextValue(counter, 0),
                NextState("PHY-INIT")
            )
        )
        fsm.act("PHY-INIT",
            self.sdphy.init.initialize.re.eq(1),
            NextState("PHY-INIT-WAIT")
        )
        fsm.act("PHY-INIT-WAIT",
            If(counter < int(clk_freq/1000),
                NextValue(counter, counter + 1)
            ).Else(
                NextValue(counter, 0),
                NextState("SD-GO-IDLE")
            )
        )
        fsm.act("SD-GO-IDLE",
            # self.send_command(0, 0, SDCARD_CTRL_RESPONSE_NONE, SDCARD_CTRL_DATA_TRANSFER_NONE),
            self.sdcore.cmd_argument.eq(0),
            self.sdcore.cmd.eq(0),
            self.sdcore.cmd_type.eq(SDCARD_CTRL_RESPONSE_NONE),
            self.sdcore.data_type.eq(SDCARD_CTRL_DATA_TRANSFER_NONE),
            self.sdcore.cmd_send.eq(1),
            NextState("WAIT-CMD-DONE")
        )
        fsm.act("WAIT-CMD-DONE",
            If(self.sdcore.cmd_done,
                NextState("CMD-DONE")
                # NextState("SEND-EXT-CSD")
            ).Else(
                NextState("WAIT-CMD-DONE")
            )
        )
        fsm.act("SEND-EXT-CSD",
            # self.send_command(0x000001aa, 8, SDCARD_CTRL_RESPONSE_SHORT, SDCARD_CTRL_DATA_TRANSFER_NONE),
            self.sdcore.cmd_argument.eq(0x000001aa),
            self.sdcore.cmd.eq(8),
            self.sdcore.cmd_type.eq(SDCARD_CTRL_RESPONSE_SHORT),
            self.sdcore.data_type.eq(SDCARD_CTRL_DATA_TRANSFER_NONE),
            self.sdcore.cmd_send.eq(1),
            NextValue(self.sdcore.cmd_send, 0),
            NextState("EXT-CSD-WAIT")
        )
        fsm.act("EXT-CSD-WAIT",
            # self.sdcore.cmd_send.eq(0),
            If(self.sdcore.cmd_done,
                NextState("SET-CLK-FREQ")
            ).Else(
                NextValue(counter, 0),
                NextState("EXT-CSD-WAIT")
            )
        )
        fsm.act("SET-CLK-FREQ",
            self.sdphy.clocker.divider.storage.eq(2),
            If(counter < int(clk_freq/1000),
                NextValue(counter, counter + 1),
            ).Else(
                NextValue(counter, 0),
                NextState("SEND-APP-CMD")
            )
        )
        fsm.act("SEND-APP-CMD",
            # self.send_command(0, 55, SDCARD_CTRL_RESPONSE_SHORT, SDCARD_CTRL_DATA_TRANSFER_NONE),
            self.sdcore.cmd_argument.eq(0),
            self.sdcore.cmd.eq(55),
            self.sdcore.cmd_type.eq(SDCARD_CTRL_RESPONSE_SHORT),
            self.sdcore.data_type.eq(SDCARD_CTRL_DATA_TRANSFER_NONE),
            self.sdcore.cmd_send.eq(1),
            NextValue(self.sdcore.cmd_send, 0),
            NextState("APP-CMD-WAIT")
        )
        fsm.act("APP-CMD-WAIT",
            self.sdcore.cmd_send.eq(0),
            If(self.sdcore.cmd_done,
                NextState("APP-SEND-OP-COND")
            ).Else(
                NextState("APP-CMD-WAIT")
            )
        )
        fsm.act("APP-SEND-OP-COND",
            # self.send_command(0x10ff8000 | 0x60000000, 41, SDCARD_CTRL_RESPONSE_SHORT_BUSY, SDCARD_CTRL_DATA_TRANSFER_NONE),
            self.sdcore.cmd_argument.eq(0x10ff8000 | 0x60000000),
            self.sdcore.cmd.eq(41),
            self.sdcore.cmd_type.eq(SDCARD_CTRL_RESPONSE_SHORT_BUSY),
            self.sdcore.data_type.eq(SDCARD_CTRL_DATA_TRANSFER_NONE),
            self.sdcore.cmd_send.eq(1),
            NextValue(self.sdcore.cmd_send, 0),
            NextState("APP-SEND-OP-COND-WAIT")
        )
        fsm.act("APP-SEND-OP-COND-WAIT",
            If(self.sdcore.cmd_done,
                NextState("CMD-DONE")
            ).Else(
                NextState("APP-SEND-OP-COND-WAIT")
            )
        )
        fsm.act("READ-OCR",
            If(self.sdcore.cmd_response[127],
                NextState("CMD-DONE")
            ).Else(
                NextState("SEND-APP-CMD")
            )
        )


        fsm.act("CMD-DONE",
            # leds[0].eq(1),
            # leds[2].eq(self.sdcore.cmd_timeout),
            NextState("CMD-DONE")
        )
        fsm.act("CMD-FAILED",
            # leds[7].eq(1),
            NextState("CMD-FAILED")
        )

        
        self.sync += [
            # leds[0].eq(1),
        ]
    def send_command(self, args, cmd, cmd_type, data_type):
        return [
            self.sdcore.cmd_argument.eq(args),
            self.sdcore.cmd.eq(cmd),
            self.sdcore.cmd_type.eq(cmd_type),
            self.sdcore.data_type.eq(data_type),
            self.sdcore.cmd_send.eq(1)
        ]


def build():
    platform = radiona_ulx3s.Platform(device="LFE5U-85F", revision="2.0", toolchain="trellis")
    design = SDController(platform=platform)

    platform.build(design)


if __name__ == "__main__":
    build()
