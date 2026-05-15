// Bit-identical Go port of Java's `java.lang.StrictMath.pow`, which the
// JDK implements via `java.lang.FdLibm.Pow.compute` (a Java translation
// of fdlibm's `e_pow.c`). The algorithmic source — including the special
// cases, accuracy claims, and constants documented below — is fdlibm
// `e_pow.c` (Sun Microsystems, 1993; redistributed in OpenJDK under
// GPLv2 + Classpath Exception).
//
// Algorithmic reference:
//   - C:    openjdk/jdk8u  src/share/native/java/lang/fdlibm/src/e_pow.c
//   - Java: openjdk/jdk17u src/java.base/share/classes/java/lang/FdLibm.java
//           (class `FdLibm.Pow.compute`)
//
// __ieee754_pow(x,y) returns x**y.
//
// Method: Let x = 2**n * (1+f).
//   1. Compute and return log2(x) in two pieces:
//          log2(x) = w1 + w2,
//      where w1 has 53-24 = 29 bit trailing zeros.
//   2. Perform y*log2(x) = n+y' by simulating muti-precision
//      arithmetic, where |y'|<=0.5.
//   3. Return x**y = 2**n*exp(y'*log2)
//
// Special cases:
//   1.  (anything) ** 0  is 1
//   2.  (anything) ** 1  is itself
//   3.  (anything) ** NAN is NAN
//   4.  NAN ** (anything except 0) is NAN
//   5.  +-(|x| > 1) **  +INF is +INF
//   6.  +-(|x| > 1) **  -INF is +0
//   7.  +-(|x| < 1) **  +INF is +0
//   8.  +-(|x| < 1) **  -INF is +INF
//   9.  +-1         ** +-INF is NAN
//   10. +0 ** (+anything except 0, NAN)               is +0
//   11. -0 ** (+anything except 0, NAN, odd integer)  is +0
//   12. +0 ** (-anything except 0, NAN)               is +INF
//   13. -0 ** (-anything except 0, NAN, odd integer)  is +INF
//   14. -0 ** (odd integer) = -( +0 ** (odd integer) )
//   15. +INF ** (+anything except 0,NAN) is +INF
//   16. +INF ** (-anything except 0,NAN) is +0
//   17. -INF ** (anything)  = -0 ** (-anything)
//   18. (-anything) ** (integer) is (-1)**(integer)*(+anything**integer)
//   19. (-anything except 0 and inf) ** (non-integer) is NAN
//
// Accuracy:
//   pow(x,y) returns x**y nearly rounded. In particular
//                      pow(integer,integer)
//   always returns the correct integer provided it is representable.
//
// The variable names and overall structure here intentionally mirror
// `FdLibm.Pow.compute` so the port is auditable line-by-line.

package strictmath

import "math"

// noFMA forces an explicit IEEE-754 rounding boundary. Go's compiler
// may fuse `a*b + c` into a single FMA instruction on platforms where
// it is available; fdlibm and Java's `StrictMath` assume strict
// separately-rounded operations. Any multiplication whose result
// feeds into an add/sub must be wrapped in `noFMA(...)` so the
// rounded product (not the extended-precision intermediate) is what
// the next op sees.
//
// `//go:noinline` is the belt-and-suspenders. A bare `float64(x)`
// identity cast is usually enough on current toolchains, but a
// non-inlined call is a true SSA boundary that future compilers
// cannot see through.
//
//go:noinline
func noFMA(x float64) float64 { return x }

// hi returns the high-order 32 bits of x as a signed int32.
func hi(x float64) int32 {
	return int32(math.Float64bits(x) >> 32)
}

// lo returns the low-order 32 bits of x as a uint32.
func lo(x float64) uint32 {
	return uint32(math.Float64bits(x))
}

// withHi returns a float64 whose high 32 bits are h and whose low 32
// bits are the low 32 bits of x.
func withHi(x float64, h int32) float64 {
	b := math.Float64bits(x)
	return math.Float64frombits((b & 0x00000000FFFFFFFF) | (uint64(uint32(h)) << 32))
}

// withLo returns a float64 whose low 32 bits are l and whose high 32
// bits are the high 32 bits of x.
func withLo(x float64, l uint32) float64 {
	b := math.Float64bits(x)
	return math.Float64frombits((b & 0xFFFFFFFF00000000) | uint64(l))
}

// Pow returns x**y, bit-identical to Java's `StrictMath.pow(x, y)`.
func Pow(x, y float64) float64 {
	// y == zero: x**0 = 1
	if y == 0.0 {
		return 1.0
	}

	// +/-NaN return x + y to propagate NaN significands
	if math.IsNaN(x) || math.IsNaN(y) {
		return x + y
	}

	yAbs := math.Abs(y)
	xAbs := math.Abs(x)

	// Special values of y
	switch {
	case y == 2.0:
		return x * x
	case y == 0.5:
		if x >= -math.MaxFloat64 { // Handle x == -infinity later
			return math.Sqrt(x + 0.0) // Add 0.0 to properly handle x == -0.0
		}
	case yAbs == 1.0: // y is +/-1
		if y == 1.0 {
			return x
		}
		return 1.0 / x
	case math.IsInf(y, 0): // y is +/-infinity
		if xAbs == 1.0 {
			return y - y // inf**+/-1 is NaN
		} else if xAbs > 1.0 { // (|x| > 1)**+/-inf = inf, 0
			if y >= 0 {
				return y
			}
			return 0.0
		} else { // (|x| < 1)**-/+inf = inf, 0
			if y < 0 {
				return -y
			}
			return 0.0
		}
	}

	hx := hi(x)
	ix := hx & 0x7fffffff

	// When x < 0, determine if y is an odd integer:
	//   yIsInt = 0  ... y is not an integer
	//   yIsInt = 1  ... y is an odd int
	//   yIsInt = 2  ... y is an even int
	var yIsInt int32 = 0
	if hx < 0 {
		if yAbs >= 0x1.0p53 { // |y| >= 2^53 = 9.007199254740992E15
			yIsInt = 2 // y is an even integer since ulp(2^53) = 2.0
		} else if yAbs >= 1.0 { // |y| >= 1.0
			yAbsAsLong := int64(yAbs)
			if float64(yAbsAsLong) == yAbs {
				yIsInt = 2 - int32(yAbsAsLong&0x1)
			}
		}
	}

	// Special value of x
	if xAbs == 0.0 || math.IsInf(xAbs, 0) || xAbs == 1.0 {
		z := xAbs // x is +/-0, +/-inf, +/-1
		if y < 0.0 {
			z = 1.0 / z // z = (1/|x|)
		}
		if hx < 0 {
			if ((ix - 0x3ff00000) | yIsInt) == 0 {
				z = (z - z) / (z - z) // (-1)**non-int is NaN
			} else if yIsInt == 1 {
				z = -1.0 * z // (x < 0)**odd = -(|x|**odd)
			}
		}
		return z
	}

	n := (hx >> 31) + 1

	// (x < 0)**(non-int) is NaN
	if (n | yIsInt) == 0 {
		return (x - x) / (x - x)
	}

	s := 1.0 // s (sign of result -ve**odd) = -1 else = 1
	if (n | (yIsInt - 1)) == 0 {
		s = -1.0 // (-ve)**(odd int)
	}

	var p_h, p_l, t1, t2 float64

	// |y| is huge
	if yAbs > 0x1.00000ffffffffp31 { // if |y| > ~2**31
		const (
			INV_LN2   = 0x1.71547652b82fep0   //  1.44269504088896338700e+00 = 1/ln2
			INV_LN2_H = 0x1.715476p0          //  1.44269502162933349609e+00 = 24 bits of 1/ln2
			INV_LN2_L = 0x1.4ae0bf85ddf44p-26 //  1.92596299112661746887e-08 = 1/ln2 tail
		)

		// Over/underflow if x is not close to one
		if xAbs < 0x1.fffff00000000p-1 { // |x| < ~0.9999995231628418
			if y < 0.0 {
				return s * math.Inf(1)
			}
			return s * 0.0
		}
		if xAbs > 0x1.00000ffffffffp0 { // |x| > ~1.0
			if y > 0.0 {
				return s * math.Inf(1)
			}
			return s * 0.0
		}
		// now |1-x| is tiny <= 2**-20, sufficient to compute
		// log(x) by x - x^2/2 + x^3/3 - x^4/4
		t := xAbs - 1.0 // t has 20 trailing zeros
		w := noFMA(t*t) * (0.5 - noFMA(t*noFMA(0.3333333333333333333333-noFMA(t*0.25))))
		u := noFMA(INV_LN2_H * t) // INV_LN2_H has 21 sig. bits
		v := noFMA(t*INV_LN2_L) - noFMA(w*INV_LN2)
		t1 = u + v
		t1 = withLo(t1, 0)
		t2 = v - (t1 - u)
	} else {
		const (
			CP   = 0x1.ec709dc3a03fdp-1   //  9.61796693925975554329e-01 = 2/(3ln2)
			CP_H = 0x1.ec709ep-1          //  9.61796700954437255859e-01 = (float)cp
			CP_L = -0x1.e2fe0145b01f5p-28 // -7.02846165095275826516e-09 = tail of CP_H
		)
		var z_h, z_l, ss, s2, s_h, s_l, t_h, t_l float64
		n = 0
		// Take care of subnormal numbers
		if ix < 0x00100000 {
			xAbs *= 0x1.0p53 // 2^53 = 9007199254740992.0
			n -= 53
			ix = hi(xAbs)
		}
		n += (ix >> 20) - 0x3ff
		j := ix & 0x000fffff
		// Determine interval
		ix = j | 0x3ff00000 // Normalize ix
		var k int32
		if j <= 0x3988E {
			k = 0 // |x| < sqrt(3/2)
		} else if j < 0xBB67A {
			k = 1 // |x| < sqrt(3)
		} else {
			k = 0
			n += 1
			ix -= 0x00100000
		}
		xAbs = withHi(xAbs, ix)

		// Compute ss = s_h + s_l = (x-1)/(x+1) or (x-1.5)/(x+1.5)
		BP := [2]float64{1.0, 1.5}
		DP_H := [2]float64{0.0, 0x1.2b8034p-1}         // 5.84962487220764160156e-01
		DP_L := [2]float64{0.0, 0x1.cfdeb43cfd006p-27} // 1.35003920212974897128e-08

		// Poly coefs for (3/2)*(log(x)-2s-2/3*s**3
		const (
			L1 = 0x1.3333333333303p-1 //  5.99999999999994648725e-01
			L2 = 0x1.b6db6db6fabffp-2 //  4.28571428578550184252e-01
			L3 = 0x1.55555518f264dp-2 //  3.33333329818377432918e-01
			L4 = 0x1.17460a91d4101p-2 //  2.72728123808534006489e-01
			L5 = 0x1.d864a93c9db65p-3 //  2.30660745775561754067e-01
			L6 = 0x1.a7e284a454eefp-3 //  2.06975017800338417784e-01
		)
		u := xAbs - BP[k] // BP[0]=1.0, BP[1]=1.5
		v := 1.0 / (xAbs + BP[k])
		ss = u * v
		s_h = ss
		s_h = withLo(s_h, 0)
		// t_h = x_abs + BP[k] High
		t_h = 0.0
		t_h = withHi(t_h, ((ix>>1)|0x20000000)+0x00080000+(k<<18))
		t_l = xAbs - (t_h - BP[k])
		s_l = v * (noFMA(u-noFMA(s_h*t_h)) - noFMA(s_h*t_l))
		// Compute log(x_abs)
		s2 = noFMA(ss * ss)
		// L-polynomial via explicit intermediates so every multiply and
		// add gets its own round; each line is one IEEE op, matching
		// the Java strictfp evaluation.
		rL6 := noFMA(s2 * L6)
		rL5 := noFMA(L5 + rL6)
		rL5 = noFMA(s2 * rL5)
		rL4 := noFMA(L4 + rL5)
		rL4 = noFMA(s2 * rL4)
		rL3 := noFMA(L3 + rL4)
		rL3 = noFMA(s2 * rL3)
		rL2 := noFMA(L2 + rL3)
		rL2 = noFMA(s2 * rL2)
		rL1 := noFMA(L1 + rL2)
		r := noFMA(noFMA(s2*s2) * rL1)
		shss := s_h + ss
		correction := noFMA(s_l * shss)
		r = r + correction
		s2 = noFMA(s_h * s_h)
		t_h = 3.0 + s2 + r
		t_h = withLo(t_h, 0)
		t_l = r - ((t_h - 3.0) - s2)
		// u+v = ss*(1+...)
		u = noFMA(s_h * t_h)
		v = noFMA(s_l*t_h) + noFMA(t_l*ss)
		// 2/(3log2)*(ss + ...)
		p_h = u + v
		p_h = withLo(p_h, 0)
		p_l = v - (p_h - u)
		z_h = noFMA(CP_H * p_h) // CP_H + CP_L = 2/(3*log2)
		z_l = noFMA(noFMA(CP_L*p_h)+noFMA(p_l*CP)) + DP_L[k]
		// log2(x_abs) = (ss + ..)*2/(3*log2) = n + DP_H + z_h + z_l
		t := float64(n)
		t1 = (((z_h + z_l) + DP_H[k]) + t)
		t1 = withLo(t1, 0)
		t2 = z_l - (((t1 - t) - DP_H[k]) - z_h)
	}

	// Split up y into (y1 + y2) and compute (y1 + y2) * (t1 + t2)
	y1 := y
	y1 = withLo(y1, 0)
	p_l = noFMA((y-y1)*t1) + noFMA(y*t2)
	p_h = noFMA(y1 * t1)
	z := p_l + p_h
	j := hi(z)
	i := int32(lo(z))
	if j >= 0x40900000 { // z >= 1024
		if ((j - 0x40900000) | i) != 0 { // if z > 1024
			return s * math.Inf(1) // Overflow
		}
		const OVT = 8.0085662595372944372e-0017 // -(1024-log2(ovfl+.5ulp))
		if p_l+OVT > z-p_h {
			return s * math.Inf(1) // Overflow
		}
	} else if (j & 0x7fffffff) >= 0x4090cc00 { // z <= -1075
		// 0xc090cc00 as a signed int32 bit pattern.
		const negThreshold = int32(-1064252416) // 0xc090cc00
		if ((j - negThreshold) | i) != 0 {      // z < -1075
			return s * 0.0 // Underflow
		}
		if p_l <= z-p_h {
			return s * 0.0 // Underflow
		}
	}

	// Compute 2**(p_h+p_l)
	const (
		P1    = 0x1.555555555553ep-3   //  1.66666666666666019037e-01
		P2    = -0x1.6c16c16bebd93p-9  // -2.77777777770155933842e-03
		P3    = 0x1.1566aaf25de2cp-14  //  6.61375632143793436117e-05
		P4    = -0x1.bbd41c5d26bf1p-20 // -1.65339022054652515390e-06
		P5    = 0x1.6376972bea4d0p-25  //  4.13813679705723846039e-08
		LG2   = 0x1.62e42fefa39efp-1   //  6.93147180559945286227e-01
		LG2_H = 0x1.62e43p-1           //  6.93147182464599609375e-01
		LG2_L = -0x1.05c610ca86c39p-29 // -1.90465429995776804525e-09
	)
	i = j & 0x7fffffff
	k := (i >> 20) - 0x3ff
	n = 0
	if i > 0x3fe00000 { // if |z| > 0.5, set n = [z + 0.5]
		n = j + (0x00100000 >> uint(k+1))
		k = ((n & 0x7fffffff) >> 20) - 0x3ff // new k for n
		t := 0.0
		t = withHi(t, n & ^(0x000fffff>>uint(k)))
		n = ((n & 0x000fffff) | 0x00100000) >> uint(20-k)
		if j < 0 {
			n = -n
		}
		p_h -= t
	}
	t := p_l + p_h
	t = withLo(t, 0)
	u := noFMA(t * LG2_H)
	v := noFMA((p_l-(t-p_h))*LG2) + noFMA(t*LG2_L)
	z = u + v
	w := v - (z - u)
	t = noFMA(z * z)
	// Horner polynomial: every inner `+` must round before the next
	// `*`, so wrap each addition in noFMA so the rounded sum (not the
	// extended-precision fused intermediate) is what the surrounding
	// multiply sees.
	// P-polynomial: same shape as the L-polynomial above — explicit
	// intermediates make each rounding observable.
	rP5 := noFMA(t * P5)
	rP4 := noFMA(P4 + rP5)
	rP4 = noFMA(t * rP4)
	rP3 := noFMA(P3 + rP4)
	rP3 = noFMA(t * rP3)
	rP2 := noFMA(P2 + rP3)
	rP2 = noFMA(t * rP2)
	rP1 := noFMA(P1 + rP2)
	rP1 = noFMA(t * rP1)
	t1 = z - rP1
	r := noFMA(noFMA(z*t1)/(t1-2.0)) - (w + noFMA(z*w))
	z = 1.0 - (r - z)
	j = hi(z)
	j += n << 20
	if (j >> 20) <= 0 {
		z = math.Ldexp(z, int(n)) // subnormal output
	} else {
		zHi := hi(z)
		zHi += n << 20
		z = withHi(z, zHi)
	}
	return s * z
}
