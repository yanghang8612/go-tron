import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bouncycastle.util.encoders.Hex;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.runtime.InternalTransaction;
import org.tron.common.runtime.ProgramResult;
import org.tron.common.runtime.vm.DataWord;
import org.tron.common.utils.ForkController;
import org.tron.core.ChainBaseManager;
import org.tron.core.capsule.ContractCapsule;
import org.tron.core.capsule.ContractStateCapsule;
import org.tron.core.store.AccountStore;
import org.tron.core.store.CodeStore;
import org.tron.core.store.ContractStateStore;
import org.tron.core.store.ContractStore;
import org.tron.core.store.DynamicPropertiesStore;
import org.tron.core.store.StorageRowStore;
import org.tron.core.store.StoreFactory;
import org.tron.core.vm.OperationRegistry;
import org.tron.core.vm.VM;
import org.tron.core.vm.config.VMConfig;
import org.tron.core.vm.program.Program;
import org.tron.core.vm.program.invoke.ProgramInvokeImpl;
import org.tron.core.vm.repository.Repository;
import org.tron.core.vm.repository.RepositoryImpl;
import org.tron.protos.Protocol.AccountType;
import org.tron.protos.contract.SmartContractOuterClass.SmartContract;

/** Real Java VM and RepositoryImpl; only empty backing stores are mocked. */
public final class JavaFrameOracle {
  static final Gson JSON = new GsonBuilder().setPrettyPrinting().create();
  static final String CALLER = address(0xc0), PARENT = address(0xc1);
  static final String CHILD = address(0xc2), LEAF = address(0xc3);
  static final String ROOT_ID = "0000000000000000000000000000000000000000000000000000000000000001";

  static class Input {
    String name, family;
    boolean dynamic, london, compatibility, osaka;
    int version;
    long messageValue;
    long energy = 1_000_000;
    Map<String, String> code = new LinkedHashMap<>();
    Map<String, Long> balances = new LinkedHashMap<>();
    Map<String, long[]> states = new LinkedHashMap<>();
  }

  static String address(int first) {
    byte[] b = new byte[21]; b[0] = 0x41; b[1] = (byte) first;
    return Hex.toHexString(b);
  }

  static byte[] bytes(int... values) {
    byte[] b = new byte[values.length];
    for (int i = 0; i < b.length; i++) b[i] = (byte) values[i];
    return b;
  }

  static byte[] join(byte[]... arrays) {
    ByteArrayOutputStream b = new ByteArrayOutputStream();
    for (byte[] a : arrays) b.write(a, 0, a.length);
    return b.toByteArray();
  }

  static byte[] invoke(int op, String target, int value, int energy) {
    byte[] args = bytes(0x60, 0, 0x60, 0, 0x60, 0, 0x60, 0);
    if (op == 0xf1 || op == 0xf2) args = join(args, bytes(0x60, value));
    // Solidity ABI addresses on the stack contain 20 bytes, without TRON's
    // network prefix. Java's precompile lookup compares that original word.
    return join(args, bytes(0x73), Arrays.copyOfRange(Hex.decode(target), 1, 21),
        bytes(0x62, energy >> 16, energy >> 8, energy, op));
  }

  static Input input(String name, String family) {
    Input i = new Input(); i.name = name; i.family = family;
    for (String a : new String[]{CALLER, PARENT, CHILD, LEAF}) i.balances.put(a, 0L);
    return i;
  }

  static List<Input> cases() {
    List<Input> all = new ArrayList<>();
    for (boolean dynamic : new boolean[]{false, true}) {
      for (String ending : new String[]{"revert", "invalid", "write-protection", "out-of-energy", "success"}) {
        Input i = input("static/dynamic=" + dynamic + "/" + ending, "static");
        i.dynamic = dynamic;
        i.states.put(CHILD, new long[]{1, 17, 0}); i.states.put(LEAF, new long[]{1, 17, 0});
        i.code.put(LEAF, "60015000");
        byte[] child = join(invoke(0xfa, LEAF, 0, 10_000), bytes(0x50));
        byte[] tail;
        switch (ending) {
          case "revert": tail = bytes(0x60, 0, 0x60, 0, 0xfd); break;
          case "invalid": tail = bytes(0xfe); break;
          case "write-protection": tail = bytes(0x60, 1, 0x60, 0, 0x55); break;
          case "out-of-energy": tail = bytes(0x5b, 0x60, child.length, 0x56); break;
          default: tail = bytes(0);
        }
        i.code.put(CHILD, Hex.toHexString(join(child, tail)));
        i.code.put(PARENT, Hex.toHexString(join(invoke(0xfa, CHILD, 0, 100_000), bytes(0x60, 0, 0x55, 0))));
        all.add(i);
        if (dynamic && (ending.equals("revert") || ending.equals("success"))) {
          Input hot = JSON.fromJson(JSON.toJson(i), Input.class);
          hot.name += "/factor=5000";
          hot.states.get(CHILD)[2] = 5000; hot.states.get(LEAF)[2] = 5000;
          all.add(hot);
        }
      }
    }
    for (boolean precompile : new boolean[]{false, true}) {
      for (int[] pair : new int[][]{{0, 1}, {1, 2}, {1, 1}, {0, 0}}) {
        Input i = input("callcode/precompile=" + precompile + "/balance=" + pair[0] + "/value=" + pair[1], "callcode");
        i.balances.put(PARENT, (long) pair[0]);
        i.code.put(CHILD, "604460005500");
        String target = precompile ? "410000000000000000000000000000000000000004" : CHILD;
        i.code.put(PARENT, Hex.toHexString(join(invoke(0xf2, target, pair[1], 100_000), bytes(0x60, 1, 0x55, 0))));
        all.add(i);
      }
      Input i = input("delegate/inherited-value/precompile=" + precompile, "delegate");
      i.messageValue = 2;
      i.code.put(CHILD, "3460005500");
      String target = precompile ? "410000000000000000000000000000000000000004" : CHILD;
      i.code.put(PARENT, Hex.toHexString(join(invoke(0xf4, target, 0, 100_000), bytes(0x60, 1, 0x55, 0))));
      all.add(i);
    }
    for (int op : new int[]{0xf0, 0xf5}) {
      for (boolean london : new boolean[]{false, true}) {
        for (int prefix : new int[]{0xee, 0xef}) {
          for (boolean revert : new boolean[]{false, true}) {
            all.add(creation(op, london, prefix, revert, false, false));
          }
        }
      }
    }
    all.add(creation(0xf5, true, 0xef, true, true, false));
    all.add(creation(0xf5, true, 0xef, true, true, true));
    return all;
  }

  static Input creation(int op, boolean london, int prefix, boolean revert, boolean priorReturn, boolean osaka) {
    Input i = input((op == 0xf0 ? "create" : "create2") + "/london=" + london
        + "/prefix=" + prefix + "/revert=" + revert + "/prior=" + priorReturn + "/osaka=" + osaka, "create");
    i.london = london; i.compatibility = true; i.version = 1; i.osaka = osaka;
    byte[] init = bytes(0x60, prefix, 0x60, 0, 0x53, 0x60, 1, 0x60, 0, revert ? 0xfd : 0xf3);
    byte[] seed = new byte[0];
    if (priorReturn) {
      i.code.put(CHILD, "61aabb6000526002601ef3");
      seed = join(invoke(0xf1, CHILD, 0, 10_000), bytes(0x50));
    }
    byte[] args = op == 0xf5 ? bytes(0x60, 1) : new byte[0];
    byte[] copy = bytes(0x60, init.length, 0x60, 0, 0x60, 0, 0x39);
    // Return CREATE's result, exact RETURNDATASIZE and the return bytes.
    byte[] body = join(args, bytes(0x60, init.length, 0x60, 0, 0x60, 0, op),
        bytes(0x60, 0, 0x52, 0x3d, 0x80, 0x60, 32, 0x52,
            0x60, 0, 0x60, 64, 0x3e, 0x60, 96, 0x60, 0, 0xf3));
    copy[3] = (byte) (seed.length + copy.length + body.length);
    i.code.put(PARENT, Hex.toHexString(join(seed, copy, body, init))); return i;
  }

  static Repository repository() {
    ChainBaseManager manager = mock(ChainBaseManager.class);
    DynamicPropertiesStore dp = mock(DynamicPropertiesStore.class);
    when(manager.getDynamicPropertiesStore()).thenReturn(dp);
    when(manager.getAccountStore()).thenReturn(mock(AccountStore.class));
    when(manager.getCodeStore()).thenReturn(mock(CodeStore.class));
    when(manager.getContractStore()).thenReturn(mock(ContractStore.class));
    when(manager.getContractStateStore()).thenReturn(mock(ContractStateStore.class));
    when(manager.getStorageRowStore()).thenReturn(mock(StorageRowStore.class));
    when(dp.getCurrentCycleNumber()).thenReturn(2L);
    when(dp.getAllowMultiSign()).thenReturn(1L);
    when(dp.getLatestBlockHeaderTimestamp()).thenReturn(1000L);
    when(dp.getMaintenanceTimeInterval()).thenReturn(21_600_000L);
    when(dp.getLatestVersion()).thenReturn(1);
    StoreFactory.init(); StoreFactory.getInstance().setChainBaseManager(manager);
    ForkController.instance().init(manager);
    return RepositoryImpl.createRoot(StoreFactory.getInstance());
  }

  static Map<String, Object> execute(Input i) {
    VMConfig.Snapshot config = new VMConfig.Snapshot();
    config.allowMultiSign = true; config.allowTvmConstantinople = true;
    config.allowTvmLondon = i.london; config.allowTvmCompatibleEvm = i.compatibility;
    config.allowTvmOsaka = i.osaka; config.allowDynamicEnergy = i.dynamic;
    config.dynamicEnergyThreshold = 1_000_000; config.dynamicEnergyIncreaseFactor = 2000;
    config.dynamicEnergyMaxFactor = 34000; VMConfig.setGlobalSnapshot(config);
    Repository repo = repository();
    for (Map.Entry<String, Long> entry : i.balances.entrySet()) {
      byte[] a = Hex.decode(entry.getKey()); repo.createAccount(a, AccountType.Contract);
      repo.addBalance(a, entry.getValue());
      repo.createContract(a, new ContractCapsule(SmartContract.newBuilder()
          .setContractAddress(ByteString.copyFrom(a)).setVersion(i.version).build()));
    }
    for (Map.Entry<String, String> entry : i.code.entrySet()) repo.saveCode(Hex.decode(entry.getKey()), Hex.decode(entry.getValue()));
    for (Map.Entry<String, long[]> entry : i.states.entrySet()) {
      long[] s = entry.getValue(); ContractStateCapsule cs = new ContractStateCapsule(s[0]);
      cs.addEnergyUsage(s[1]); cs.setEnergyFactor(s[2]);
      repo.updateContractState(Hex.decode(entry.getKey()), cs);
    }
    byte[] parent = Hex.decode(PARENT), caller = Hex.decode(CALLER);
    InternalTransaction root = new InternalTransaction(new byte[32], -1, 0, caller, parent, 0,
        new byte[0], "call", 0, null);
    ProgramInvokeImpl invoke = new ProgramInvokeImpl(new DataWord(parent), new DataWord(caller),
        new DataWord(caller), new DataWord(repo.getBalance(parent)), new DataWord(i.messageValue),
        DataWord.ZERO(), DataWord.ZERO(), new byte[0], DataWord.ZERO(), DataWord.ZERO(),
        new DataWord(1), new DataWord(1), DataWord.ZERO(), repo, 0, false, false,
        0, Long.MAX_VALUE, i.energy);
    Program program = new Program(Hex.decode(i.code.get(PARENT)), parent, invoke, root);
    program.setRootTransactionId(Hex.decode(ROOT_ID)); program.setContractVersion(i.version);
    VM.play(program, OperationRegistry.prepareAndGetTable(false));
    ProgramResult r = program.getResult();
    if (r.getException() != null || r.isRevert()) throw new AssertionError(i.name + ": unexpected root failure " + r.getException());
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("return", Hex.toHexString(r.getHReturn())); out.put("energy_left", i.energy - r.getEnergyUsed());
    out.put("penalty", r.getEnergyPenaltyTotal()); out.put("nonce", program.getNonce());
    List<Object> internal = new ArrayList<>();
    for (InternalTransaction tx : r.getInternalTransactions()) {
      Map<String, Object> row = new LinkedHashMap<>(); row.put("note", tx.getNote());
      row.put("rejected", tx.isRejected()); row.put("value", tx.getValue()); internal.add(row);
    }
    out.put("internal", internal);
    Map<String, Object> states = new LinkedHashMap<>(), balances = new LinkedHashMap<>();
    for (String addr : i.balances.keySet()) {
      byte[] a = Hex.decode(addr); balances.put(addr, repo.getBalance(a));
      ContractStateCapsule cs = repo.getContractState(a);
      if (cs != null) states.put(addr, new long[]{cs.getUpdateCycle(), cs.getEnergyUsage(), cs.getEnergyFactor()});
    }
    out.put("states", states); out.put("balances", balances);
    List<String> slots = new ArrayList<>();
    for (int slot = 0; slot < 2; slot++) {
      DataWord word = repo.getStorageValue(parent, new DataWord(slot));
      slots.add(Hex.toHexString(word == null ? new byte[32] : word.getData()));
    }
    out.put("storage", slots); return out;
  }

  public static void main(String[] args) throws Exception {
    CommonParameter.getInstance().setDebug(true);
    CommonParameter.ENERGY_LIMIT_HARD_FORK = true;
    List<Object> records = new ArrayList<>();
    for (Input input : cases()) {
      Map<String, Object> record = new LinkedHashMap<>(); record.put("input", input);
      record.put("output", execute(input)); records.add(record);
    }
    Map<String, Object> result = new LinkedHashMap<>(); result.put("java_commit", args[1]);
    result.put("cases", records);
    Files.write(Paths.get(args[0]), JSON.toJson(result).getBytes(StandardCharsets.UTF_8));
    System.out.println("JAVA_FRAME_ORACLE_PASS cases=" + records.size());
  }
}
