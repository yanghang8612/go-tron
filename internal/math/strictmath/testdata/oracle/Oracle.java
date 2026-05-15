// Oracle: reads `<a> <b>` pairs (one per line, whitespace separated) from
// stdin, where each value is either decimal or a hex-float (Java
// Double.toHexString format). For each pair, writes one line to stdout
// containing the 16-hex-digit bit pattern of StrictMath.pow(a, b),
// padded with leading zeros.
//
// Usage:
//   javac Oracle.java
//   java -cp . Oracle < inputs.txt > expected.txt
import java.io.BufferedReader;
import java.io.InputStreamReader;

public final class Oracle {
    public static void main(String[] args) throws Exception {
        BufferedReader r = new BufferedReader(new InputStreamReader(System.in));
        StringBuilder out = new StringBuilder();
        String line;
        while ((line = r.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) continue;
            String[] parts = line.split("\\s+");
            if (parts.length != 2) {
                throw new RuntimeException("bad line: " + line);
            }
            double a = parseDouble(parts[0]);
            double b = parseDouble(parts[1]);
            double r1 = StrictMath.pow(a, b);
            long bits = Double.doubleToLongBits(r1);
            // Always emit a 16-hex-digit zero-padded value for deterministic
            // textual comparison.
            out.append(String.format("%016x", bits));
            out.append('\n');
        }
        System.out.print(out.toString());
    }

    private static double parseDouble(String s) {
        // Accept Java hex-float (0x1.8p3), Java's "NaN" / "Infinity" /
        // "-Infinity" strings, and plain decimal.
        return Double.parseDouble(s);
    }
}
