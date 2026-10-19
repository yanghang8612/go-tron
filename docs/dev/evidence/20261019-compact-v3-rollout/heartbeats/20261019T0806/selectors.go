package main
import("fmt";"github.com/ethereum/go-ethereum/crypto")
func main(){for _,s:=range []string{"current()","minBetAmount()","maxWinRate()","promotionRate()","getOraclizeAddr()","getTronBetPoolAddr()","getReferralshipAddr()"}{fmt.Printf("%s %x\n",s,crypto.Keccak256([]byte(s))[:4])}}
