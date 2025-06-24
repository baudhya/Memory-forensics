#!/usr/bin/env python3

from migen import *
from migen.sim import *
# from spi_master import SPIMaster
from litex.soc.cores.spi import SPIMaster
from litex.build.generic_platform import *
from litex_boards.platforms import radiona_ulx3s


class SDController(Module):
    def __init__(self, spipads, leds):

        self.leds = leds
        pads = spipads
        self.spi = SPIMaster(pads=spipads, data_width=8, sys_clk_freq=int(50e6), spi_clk_freq=int(400e3), with_csr=True)

        self.counter = Signal(24)
        self.init_counter = Signal()
        self.byte_sent = Signal(8, reset=0xFF)

        self.fsm = FSM(reset_state="INIT")
        init_wait_counter = Signal(16, reset=0)
        init_count = Signal(4)

        self.cmd_index = Signal(3)   # Index for CMD0 transmission
        self.response = Signal(8)    # SD card response byte
        self.retry_counter = Signal(32)
        self.done = Signal()

        # CMD0 packet (0x40 00 00 00 00 95)
        self.cmd0 = Array([
            Signal(8, reset=0x40),  # Command index (CMD0)
            Signal(8, reset=0x00),  # Argument byte 1
            Signal(8, reset=0x00),  # Argument byte 2
            Signal(8, reset=0x00),  # Argument byte 3
            Signal(8, reset=0x00),  # Argument byte 4
            Signal(8, reset=0x95)   # Correct CRC for CMD0
        ])

        self.fsm.act("INIT",
            If(init_wait_counter < 55000,
                NextValue(init_wait_counter, init_wait_counter + 1),
                NextState("INIT")
            ).Else(
                NextValue(init_count, 0),
                NextState("SEND_BYTE")
            )
            
        )
        self.fsm.act("SEND_BYTE",
            self.spi.cs.eq(0),
            self.spi.mosi.eq(self.byte_sent),
            self.spi._control.storage.eq(0x801),
            NextState("WAIT_FOR_DONE")
            
        )
        self.fsm.act("WAIT_FOR_DONE",
            self.spi.start.eq(0),
            If(self.spi.done,
                NextValue(init_count, init_count + 1),
                If(init_count == 14,
                    NextValue(self.response, self.spi._miso.status),
                    self.spi.cs.eq(1),
                    NextState("SEND_CMD0") 
                ).Else(
                    NextState("SEND_BYTE")
                )
            )
        )
        self.fsm.act("SEND_CMD0",
            self.spi.mosi.eq(self.cmd0[self.cmd_index]),
            self.spi._control.storage.eq(0x801),
            NextState("WAIT_CMD0_DONE")
        )
        self.fsm.act("WAIT_CMD0_DONE",
            If(self.spi.done,
                NextValue(self.cmd_index, self.cmd_index + 1),
                If(self.cmd_index == 5,
                   NextState("READ_CMD0_RES")
                ).Else(
                    NextState("SEND_CMD0")
                ),
                NextValue(self.retry_counter, 0),
            )
        )
        self.fsm.act("READ_CMD0_RES",
            self.spi.mosi.eq(0xFF),
            self.spi._control.storage.eq(0x801),
            NextState("WAIT_CMD0_RES")
        )
        self.fsm.act("WAIT_CMD0_RES",
            # If(self.spi.done,
                If(~pads.miso,
                    NextValue(self.response, self.spi._miso.status),
                    If(self.spi._miso.status == 0x01,
                        NextState("CMD0_SUCCESS")
                    ).Else(
                        NextState("SHOW_RESPONSE")
                    )
                ).Else(
                    NextValue(self.retry_counter, self.retry_counter + 1),
                    If(self.retry_counter <   int(25e6),#1000,  
                        NextState("READ_CMD0_RES")
                    ).Else(
                        NextState("CMD0_FAIL")  
                    )
                )
            # )
        )
        self.fsm.act("SHOW_RESPONSE",
            self.leds[1].eq(1),
            self.done.eq(1)
            # self.leds[0].eq(self.response[7]),
            # self.leds[1].eq(self.response[6]),
            # self.leds[2].eq(self.response[5]),
            # self.leds[3].eq(self.response[4]),
            # self.leds[4].eq(self.response[3]),
            # self.leds[5].eq(self.response[2]),
            # self.leds[6].eq(self.response[1]),
            # self.leds[7].eq(self.response[0]),
        )
        self.fsm.act("CMD0_SUCCESS",
            self.leds[0].eq(1),
            self.done.eq(1),
        )
        self.fsm.act("CMD0_FAIL",
            self.leds[7].eq(1),
            self.done.eq(1), 
        )

        self.fsm.act("OFF",
            self.leds[0].eq(0),
            self.leds[1].eq(1),
            If(self.counter == 0xFFFFFF,
               NextState("ON")
            )     
        )

        self.fsm.act("ON",
            self.leds[0].eq(1),
            self.leds[1].eq(0),
            If(self.counter == 0xFFFFFF,
               NextState("OFF")
            )
        )

        self.sync += [
            If(self.counter == 0xFFFFFF,
                self.counter.eq(0)
            ).Else(
                self.counter.eq(self.counter + 1)
            )
        ]

        self.comb += [
            pads.cs_n.eq(0),
            # self.leds[1].eq(pads.miso)
        ]

        self.submodules += self.spi
        self.submodules += self.fsm




def build():
    platform = radiona_ulx3s.Platform(device="LFE5U-85F", revision="2.0", toolchain="trellis")
    pads=platform.request("spisdcard")
    leds = [platform.request("user_led", i) for i in range(8)]
    # spi = SPIMaster(pads=pads, data_width=8, sys_clk_freq=int(50e6), spi_clk_freq=int(400e3), with_csr=False)
    design = SDController(leds=leds, spipads=pads)

    platform.build(design)
    # platform.build(spi)


def sim(dut):
    while (yield dut.done) == 0:
        yield
    yield
    print("Sim finished")

def test():

    leds = [Signal() for i in range(8)]
    pads_layout = [("clk", 1), ("cs_n", 1), ("mosi", 1), ("miso", 1)]
    pads = Record(pads_layout)
    dut = SDController(spipads=pads, leds=leds)
    run_simulation(dut, sim(dut), vcd_name="sdcontroller.vcd")


if __name__ == "__main__":
    build()
    # test()

