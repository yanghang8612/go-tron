#!/usr/bin/env python3
"""Disassemble TVM/EVM runtime or creation bytecode.

Usage:
    scripts/dev/evm_disasm.py runtime.hex          # file containing hex (0x prefix ok)
    echo 600143034060005500 | scripts/dev/evm_disasm.py -

Used in sync-stall debugging to read on-chain contracts pulled via
wallet/getcontractinfo (see docs/dev/sync-stall-runbook.md). Includes the
TRON-specific opcodes (CALLTOKEN family 0xd0-0xd3).
"""
import sys

OPS = {
    0x00: 'STOP', 0x01: 'ADD', 0x02: 'MUL', 0x03: 'SUB', 0x04: 'DIV',
    0x05: 'SDIV', 0x06: 'MOD', 0x07: 'SMOD', 0x08: 'ADDMOD', 0x09: 'MULMOD',
    0x0a: 'EXP', 0x0b: 'SIGNEXTEND',
    0x10: 'LT', 0x11: 'GT', 0x12: 'SLT', 0x13: 'SGT', 0x14: 'EQ',
    0x15: 'ISZERO', 0x16: 'AND', 0x17: 'OR', 0x18: 'XOR', 0x19: 'NOT',
    0x1a: 'BYTE', 0x1b: 'SHL', 0x1c: 'SHR', 0x1d: 'SAR',
    0x20: 'SHA3',
    0x30: 'ADDRESS', 0x31: 'BALANCE', 0x32: 'ORIGIN', 0x33: 'CALLER',
    0x34: 'CALLVALUE', 0x35: 'CALLDATALOAD', 0x36: 'CALLDATASIZE',
    0x37: 'CALLDATACOPY', 0x38: 'CODESIZE', 0x39: 'CODECOPY',
    0x3a: 'GASPRICE', 0x3b: 'EXTCODESIZE', 0x3c: 'EXTCODECOPY',
    0x3d: 'RETURNDATASIZE', 0x3e: 'RETURNDATACOPY', 0x3f: 'EXTCODEHASH',
    0x40: 'BLOCKHASH', 0x41: 'COINBASE', 0x42: 'TIMESTAMP', 0x43: 'NUMBER',
    0x44: 'DIFFICULTY', 0x45: 'GASLIMIT', 0x46: 'CHAINID', 0x47: 'SELFBALANCE',
    0x48: 'BASEFEE',
    0x50: 'POP', 0x51: 'MLOAD', 0x52: 'MSTORE', 0x53: 'MSTORE8',
    0x54: 'SLOAD', 0x55: 'SSTORE', 0x56: 'JUMP', 0x57: 'JUMPI',
    0x58: 'PC', 0x59: 'MSIZE', 0x5a: 'GAS', 0x5b: 'JUMPDEST',
    0x5e: 'MCOPY', 0x5f: 'PUSH0',
    0xd0: 'CALLTOKEN', 0xd1: 'TOKENBALANCE', 0xd2: 'CALLTOKENVALUE',
    0xd3: 'CALLTOKENID',
    0xf0: 'CREATE', 0xf1: 'CALL', 0xf2: 'CALLCODE', 0xf3: 'RETURN',
    0xf4: 'DELEGATECALL', 0xf5: 'CREATE2', 0xfa: 'STATICCALL',
    0xfd: 'REVERT', 0xfe: 'INVALID', 0xff: 'SELFDESTRUCT',
}
for i in range(1, 33):
    OPS[0x5f + i] = 'PUSH%d' % i
for i in range(1, 17):
    OPS[0x7f + i] = 'DUP%d' % i
    OPS[0x8f + i] = 'SWAP%d' % i
for i in range(0, 5):
    OPS[0xa0 + i] = 'LOG%d' % i


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    raw = sys.stdin.read() if sys.argv[1] == '-' else open(sys.argv[1]).read()
    raw = raw.strip().removeprefix('0x')
    code = bytes.fromhex(raw)
    pc = 0
    while pc < len(code):
        op = code[pc]
        name = OPS.get(op, '??%02x' % op)
        if 0x60 <= op <= 0x7f:
            n = op - 0x5f
            arg = code[pc + 1:pc + 1 + n]
            print('%04x: %s 0x%s' % (pc, name, arg.hex()))
            pc += 1 + n
        else:
            print('%04x: %s' % (pc, name))
            pc += 1


if __name__ == '__main__':
    main()
