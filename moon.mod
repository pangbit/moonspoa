// Learn more about moon.mod configuration:
// https://docs.moonbitlang.com/en/latest/toolchain/moon/module.html
//
// To add a dependency, run this command in your terminal:
//   moon add moonbitlang/x
//
// Or manually declare it in `import`, for example:
// import {
//   "moonbitlang/x@0.4.6",
// }

name = "pangbit/moonspoa"

version = "0.1.0"

readme = "README.mbt.md"

repository = "https://github.com/pangbit/moonspoa"

license = "Apache-2.0"

preferred_target = "native"

description = "SPOA (Stream Processing Offload Agent) library for MoonBit: SPOP protocol codec, agent server (TCP/UDS) and SPOE client"

keywords = [ "spoa", "spoe", "spop", "haproxy" ]

import {
  "moonbitlang/async@0.22.1",
}
